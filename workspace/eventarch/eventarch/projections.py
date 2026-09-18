"""Auditable derived-lineage projection pipelines (/v3/projections).

A *projection pipeline* asynchronously scans one declared static view
(a freeze token) over a fixed [start_cursor, end_cursor) source range,
restricted to a source-terminal whitelist, and applies a named field
recipe to produce *derived entries* on target terminals.  Every derived
entry carries lineage: a deterministic lineage number, the recipe code
and its generation time -- and it is ingested through the normal write
path, so ordinary streams, dedup, terminal sequencing and the
late/clock-rollback/seq-conflict markers all apply unchanged.

Idempotency
-----------
``pipeline_id`` is the cross-process idempotency key.  Re-posting the
same id returns the original pipeline (200); posting the same id with a
different view/cursors/recipe/whitelist/prefix is rejected with 409 and
the original pipeline runs untouched.  Derived identity is
``f(recipe_code, lineage_no)`` with ``lineage_no`` = source offset, so
the same recipe running again reproduces the same lineage mapping
(dedup absorbs the re-run).

Pause / resume / revoke carry a monotonic generation (``gen``): a
control action must present the current generation; each applied
transition bumps it, and a stale token can never rewrite a newer stage.
Workers fence every batch on the generation they started with.

Static views and gaps
---------------------
Only segments referenced by the declared freeze are scanned and only
offsets below the freeze horizon; content arriving after the view can
never leak in.  A quarantined (or otherwise unreadable) source segment
blocks the pipeline: the gap, the precise resume cursor and the affected
target terminals are reported and never hidden.  The pipeline resumes
from that exact cursor after the segment is repaired.  Segment version
drift (a repair swapped the bytes) makes the worker discard its stale
scan and re-plan.

Durability and GC
-----------------
The pipeline manifest, continuation cursor, derived index, dependency
segment set and the constant terminal answer live in
``state/projections.json`` (atomic replace).  Each batch is:

  1. derived entries flushed via the normal WAL fsync + apply path;
  2. journal persistence publishes the new cursor / derived index;
  3. reaching the horizon marks the terminal answer.

Forced-abort hooks fire immediately BEFORE manifest persistence, entry
flush, cursor publication and terminal marking, so a crash always
resumes from the last complete batch -- never a half entry, a double
entry or a skipped gap (replayed batches dedup to the same offsets).

Unfinished pipelines pin their dependency segments against capacity
reclamation; only completion or revoke releases them.

Heavy segment I/O happens outside the global lock, concurrently with
ingestion, terminal lookups, view creation, repairs and GC.
"""

from __future__ import annotations

import logging
import os
import threading
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple

from . import segments as segmod
from .models import fmt_ts, utcnow
from .util import atomic_write_json, load_json

log = logging.getLogger("eventarch.projections")

TERMINAL = ("completed", "revoked")


class ProjectionConflict(Exception):
    """Declaration/occupancy/generation conflict (HTTP 409)."""

    def __init__(self, reasons: List[str]):
        super().__init__("projection conflict: " + "; ".join(reasons))
        self.reasons = reasons


class _Drift(Exception):
    """Internal: scanned units changed identity; discard and re-plan."""


def _lineage_event_id(recipe_code: str, lineage_no: int) -> str:
    # Deterministic derived identity: same recipe + same lineage entry
    # number always maps to the same event id (and thus the same archive
    # offset through the normal dedup path).
    return f"lin:{recipe_code}:{lineage_no:020d}"


class ProjectionManager:
    def __init__(self, store):
        self.s = store
        self.state_dir = store.state_dir
        self._path = os.path.join(self.state_dir, "projections.json")
        self._pipelines: Dict[str, dict] = {}
        self._q: "Queue[Optional[str]]" = Queue()
        self._workers: List[threading.Thread] = []
        self._inflight: set = set()
        self._inflight_lock = threading.Lock()
        self._stop = threading.Event()
        self._watcher: Optional[threading.Thread] = None
        # Test/observability hook; invoked (may be under the store lock):
        #   hook(pipeline_view, phase)
        # phase in {"manifest_persisted", "entries_flushed",
        #           "cursor_published", "terminal_marked"} -- each fires
        # immediately BEFORE the named durable action.
        self._phase_hook = None

    # ------------------------------------------------------------------ #
    # recovery                                                            #
    # ------------------------------------------------------------------ #

    def recover(self) -> None:
        """Load the durable journal and reconcile derived indexes.

        Runs (in store.open) after segment indexes and the WAL tail have
        been rebuilt, so every already-flushed derived event is already
        present in the in-memory dedup table: a batch whose flush landed
        but whose cursor publication was missed simply re-derives as
        duplicates on the next run, re-attaching the same offsets.
        """
        if os.path.exists(self._path):
            try:
                data = load_json(self._path)
            except Exception as exc:
                log.error("cannot read projections journal (%s); starting empty",
                          exc)
                data = {"pipelines": {}}
            for pid, p in data.get("pipelines", {}).items():
                self._pipelines[pid] = p
        for p in self._pipelines.values():
            p.setdefault("gaps", [])
            p.setdefault("stages", [])
            p.setdefault("derived_index", [])
            p.setdefault("conflicts", [])
            p.setdefault("gen", 1)
            self._verify_index_locked(p)
            if p["status"] not in TERMINAL and p["status"] != "paused":
                # Any unfinished, non-paused pipeline is requeued; work
                # restarts at its last fully published cursor.  Paused
                # pipelines keep waiting for an explicit resume.
                p["status"] = "queued"
                p["stage"] = "queued"
                p["updated_at"] = fmt_ts(utcnow())
        if self._pipelines:
            self._persist_locked()
        log.info("recovered %d projection pipelines (%d non-terminal)",
                 len(self._pipelines),
                 sum(1 for p in self._pipelines.values()
                     if p["status"] not in TERMINAL))

    def _verify_index_locked(self, p: dict) -> None:
        """Drop derived-index tail entries that reference unknown events.

        Derived batches are atomic (one WAL fsync) and journal publication
        is atomic too, so a crash can only leave the journal *behind* the
        flushed entries (never ahead); this check is the defensive
        backstop that keeps cursor and index mutually consistent.
        """
        kept: List[dict] = []
        for e in p.get("derived_index", []):
            dev = self.s._devices.get(e["target"])
            offset = None
            if dev is not None:
                offset = dev.event_ids.get(e["event_id"])
            if offset is None or offset != e["offset"]:
                log.warning("projection %s: dropping stale index entry %s",
                            p["pipeline_id"], e.get("lineage_no"))
                continue
            kept.append(e)
        if len(kept) != len(p.get("derived_index", [])):
            p["derived_index"] = kept
            p["cursor"] = kept[-1]["source_offset"] + 1 if kept else p["start_cursor"]

    def start(self) -> None:
        n = max(1, getattr(self.s.cfg, "projection_workers", 1))
        for p in self._pipelines.values():
            if p["status"] not in TERMINAL and p["status"] != "paused":
                self._q.put(p["pipeline_id"])
        for i in range(n):
            t = threading.Thread(target=self._worker_loop,
                                 name=f"projection-{i}", daemon=True)
            t.start()
            self._workers.append(t)
        # Blocked pipelines wait on a segment repair; instead of hooking
        # every repair transition, one cheap watcher re-enqueues runnable
        # pipelines at a bounded cadence.
        self._watcher = threading.Thread(target=self._watcher_loop,
                                         name="projection-watch", daemon=True)
        self._watcher.start()

    def close(self) -> None:
        self._stop.set()
        for _ in self._workers:
            self._q.put(None)
        for t in self._workers:
            t.join(timeout=5)
        if self._watcher is not None:
            self._watcher.join(timeout=5)

    # ------------------------------------------------------------------ #
    # creation / declaration                                              #
    # ------------------------------------------------------------------ #

    def create(self, pipeline_id: str, view_token: str, sources: List[str],
               start_cursor: int, end_cursor: int, recipe: Dict[str, str],
               recipe_code: str, target_prefix: str) -> Tuple[dict, bool]:
        """Register (or idempotently re-state) a pipeline.

        Returns (view, created).  Same id + same declaration -> the
        original object (200); same id + different declaration -> 409.
        """
        self._validate_decl(pipeline_id, view_token, sources, start_cursor,
                            end_cursor, recipe, recipe_code, target_prefix)
        sources = sorted(set(sources))
        with self.s._lock:
            existing = self._pipelines.get(pipeline_id)
            if existing is not None:
                same = self._same_declaration(
                    existing, view_token, sources, start_cursor, end_cursor,
                    recipe, recipe_code, target_prefix)
                if not same:
                    raise ProjectionConflict(
                        [f"pipeline {pipeline_id} already exists with a different "
                         f"declaration; the original pipeline is unchanged"])
                return self._view_locked(existing), False

            frz = next((f for f in self.s._freezes
                        if f["id"] == view_token), None)
            if frz is None:
                from .store import NotFound
                raise NotFound(f"view token {view_token} not found")
            if end_cursor > frz["end_offset"]:
                raise ProjectionConflict([
                    f"end_cursor {end_cursor} is beyond the static view horizon "
                    f"{frz['end_offset']}; later arrivals must never enter the "
                    f"pipeline"])

            deps, reasons = self._plan_dependencies_locked(
                frz, start_cursor, end_cursor, sources, target_prefix,
                recipe_code, pipeline_id)
            if reasons:
                # Occupancy clashes also annotate the *holder* pipeline;
                # make that bilateral record durable before rejecting.
                self._persist_locked()
                raise ProjectionConflict(reasons)

            now = fmt_ts(utcnow())
            p = {
                "pipeline_id": pipeline_id,
                "status": "queued",
                "stage": "queued",
                "gen": 1,
                "view_token": view_token,
                "view_end_offset": frz["end_offset"],
                "view_segments": list(frz["segments"]),
                "sources": sources,
                "start_cursor": start_cursor,
                "end_cursor": end_cursor,
                "cursor": start_cursor,
                "recipe": recipe,
                "recipe_code": recipe_code,
                "target_prefix": target_prefix,
                "depends_on": deps,
                "derived_index": [],
                "gaps": [],
                "stages": [{"stage": "created", "at": now}],
                "conflicts": [],
                "final": None,
                "error": None,
                "created_at": now,
                "updated_at": now,
            }
            self._pipelines[pipeline_id] = p
            # Forced-abort point #1: crash before the manifest ever lands.
            self._fire_hook_locked(p, "manifest_persisted")
            self._persist_locked()
            view = self._view_locked(p)
        self._q.put(pipeline_id)
        log.info("projection %s created: view=%s range=[%d,%d) sources=%d "
                 "recipe=%s deps=%d", pipeline_id, view_token, start_cursor,
                 end_cursor, len(sources), recipe_code, len(deps))
        return view, True

    @staticmethod
    def _validate_decl(pipeline_id, view_token, sources, start_cursor,
                       end_cursor, recipe, recipe_code, target_prefix) -> None:
        if not isinstance(pipeline_id, str) or not pipeline_id \
                or len(pipeline_id) > 256 or "/" in pipeline_id:
            raise ValueError("pipeline_id must be a non-empty string "
                             "(<=256 chars, no '/')")
        if not isinstance(view_token, str) or not view_token:
            raise ValueError("view_token must reference a declared view")
        if not isinstance(sources, list) or not sources:
            raise ValueError("sources must be a non-empty whitelist array")
        if any(not isinstance(x, str) or not x or len(x) > 256
               for x in sources):
            raise ValueError("every source terminal must be a non-empty "
                             "string (<=256 chars)")
        if isinstance(start_cursor, bool) or not isinstance(start_cursor, int) \
                or start_cursor < 0:
            raise ValueError("start_cursor must be a non-negative integer")
        if isinstance(end_cursor, bool) or not isinstance(end_cursor, int) \
                or end_cursor < 0:
            raise ValueError("end_cursor must be a non-negative integer")
        if end_cursor < start_cursor:
            raise ValueError("end_cursor must be >= start_cursor")
        if not isinstance(recipe, dict) or not recipe:
            raise ValueError("recipe must be a non-empty {field: path} object")
        if any(not isinstance(k, str) or not k
                or not isinstance(v, str) or not v
                for k, v in recipe.items()):
            raise ValueError("recipe keys and source paths must be "
                             "non-empty strings")
        if not isinstance(recipe_code, str) or not recipe_code \
                or len(recipe_code) > 128 or "/" in recipe_code:
            raise ValueError("recipe_code must be a non-empty string "
                             "(<=128 chars, no '/')")
        if not isinstance(target_prefix, str) or not target_prefix \
                or len(target_prefix) > 64:
            raise ValueError("target_prefix must be a non-empty string "
                             "(<=64 chars)")

    @staticmethod
    def _same_declaration(p, view_token, sources, start_cursor, end_cursor,
                          recipe, recipe_code, target_prefix) -> bool:
        return (p["view_token"] == view_token
                and p["sources"] == sources
                and p["start_cursor"] == start_cursor
                and p["end_cursor"] == end_cursor
                and p["recipe"] == recipe
                and p["recipe_code"] == recipe_code
                and p["target_prefix"] == target_prefix)

    def _plan_dependencies_locked(self, frz, start, end, sources,
                                  target_prefix, recipe_code, pipeline_id):
        """Resolve dependency segments and detect occupancy/eviction clashes."""
        deps: List[str] = []
        reasons: List[str] = []
        for sid in frz["segments"]:
            meta = self.s._seg_by_id.get(sid)
            if meta is None:
                if self.s.gc.tombstone(sid) is not None:
                    # A snapshot reference is normally GC-pinned; refuse
                    # rather than silently lose the range.
                    reasons.append(
                        f"{sid}: source segment has been evicted (tombstone)")
                else:
                    reasons.append(
                        f"{sid}: source segment is mid-eviction; retry shortly")
                continue
            if meta["last_offset"] < start or meta["first_offset"] >= end:
                continue
            if self.s.gc.is_pending(sid):
                reasons.append(f"{sid}: capacity reclamation in flight")
                continue
            deps.append(sid)

        # A target terminal (prefix + whitelisted source) is the
        # materialized derivation of exactly one recipe.  It stays occupied
        # by every pipeline that has (or is) producing it -- including
        # completed ones whose derived entries remain in the streams;
        # only a revoked pipeline releases its terminals.  The SAME recipe
        # on the same terminal is allowed: deterministic identity makes
        # that overlap an idempotent no-op (identical lineage mapping,
        # deduped entries).  A different recipe gets a bilateral 409.
        now = fmt_ts(utcnow())
        whitelist = set(sources)
        for other in self._pipelines.values():
            if other["pipeline_id"] == pipeline_id \
                    or other["status"] == "revoked":
                continue
            if other["target_prefix"] != target_prefix:
                continue
            clash = sorted(whitelist & set(other["sources"]))
            diff_recipe = [d for d in clash
                           if other["recipe_code"] != recipe_code]
            for d in diff_recipe:
                target = target_prefix + d
                reasons.append(
                    f"target terminal {target!r} (source {d!r}) is already "
                    f"occupied by pipeline {other['pipeline_id']} with recipe "
                    f"{other['recipe_code']!r}")
                other["conflicts"].append({
                    "with": pipeline_id, "source": d, "target": target,
                    "recipe": recipe_code, "at": now})
            same_recipe = [d for d in clash
                           if other["recipe_code"] == recipe_code]
            if same_recipe:
                # Not a conflict; dependency sharing is fine.  Record the
                # sibling so operators can see the shared derivation.
                other.setdefault("siblings", []).append({
                    "pipeline_id": pipeline_id, "sources": same_recipe,
                    "at": now})
        return sorted(deps), reasons

    # ------------------------------------------------------------------ #
    # pause / resume / revoke (fenced by monotonic gen)                   #
    # ------------------------------------------------------------------ #

    def control(self, pipeline_id: str, action: str, gen: int) -> dict:
        if action not in ("pause", "resume", "revoke"):
            raise ValueError("action must be pause, resume or revoke")
        if isinstance(gen, bool) or not isinstance(gen, int):
            raise ValueError("gen must be an integer generation token")
        with self.s._lock:
            p = self._pipelines.get(pipeline_id)
            if p is None:
                from .store import NotFound
                raise NotFound(f"projection {pipeline_id} not found")
            if gen != p["gen"]:
                raise ProjectionConflict([
                    f"stale control token gen={gen}; current generation is "
                    f"{p['gen']} (stage={p['stage']}); the older token cannot "
                    f"rewrite the newer stage"])
            now = fmt_ts(utcnow())
            if action == "pause":
                if p["status"] in TERMINAL:
                    raise ProjectionConflict(
                        [f"pipeline is {p['status']}; cannot pause"])
                if p["status"] != "paused":
                    p["status"] = "paused"
                    p["stage"] = "paused"
                    p["stages"].append({"stage": "paused", "at": now})
                    p["gen"] += 1
                    self._touch_locked(p, persist=True)
                return self._view_locked(p)
            if action == "resume":
                if p["status"] in TERMINAL:
                    raise ProjectionConflict(
                        [f"pipeline is {p['status']}; cannot resume"])
                if p["status"] == "paused":
                    p["status"] = "queued"
                    p["stage"] = "queued"
                    p["stages"].append({"stage": "resumed", "at": now})
                    p["gen"] += 1
                    self._touch_locked(p, persist=True)
                    view = self._view_locked(p)
                    enqueue = True
                else:
                    view = self._view_locked(p)
                    enqueue = False
            else:  # revoke
                if p["status"] == "revoked":
                    return self._view_locked(p)
                if p["status"] == "completed":
                    raise ProjectionConflict(
                        ["pipeline already completed; cannot revoke"])
                p["status"] = "revoked"
                p["stage"] = "revoked"
                p["stages"].append({"stage": "revoked", "at": now})
                p["gen"] += 1
                p["final"] = self._view_locked(p)
                self._touch_locked(p, persist=True)
                log.info("projection %s revoked at cursor %d (%d derived)",
                         pipeline_id, p["cursor"], len(p["derived_index"]))
                return p["final"]
        if enqueue:
            self._q.put(pipeline_id)
        return view

    # ------------------------------------------------------------------ #
    # queries                                                             #
    # ------------------------------------------------------------------ #

    def get(self, pipeline_id: str) -> dict:
        with self.s._lock:
            p = self._pipelines.get(pipeline_id)
            if p is None:
                from .store import NotFound
                raise NotFound(f"projection {pipeline_id} not found")
            # A revoked answer is frozen forever; a completed pipeline's
            # audit fields (e.g. bilateral conflicts annotated later by
            # rejected declarations) still reflect the live record.
            if p["status"] == "revoked" and p.get("final") is not None:
                return p["final"]
            return self._view_locked(p)

    def list_pipelines(self, limit: int = 100) -> dict:
        with self.s._lock:
            items = sorted(self._pipelines.values(),
                           key=lambda p: p["created_at"], reverse=True)
            return {"pipelines": [{
                "pipeline_id": p["pipeline_id"],
                "status": p["status"],
                "stage": p["stage"],
                "gen": p["gen"],
                "view_token": p["view_token"],
                "recipe_code": p["recipe_code"],
                "target_prefix": p["target_prefix"],
                "cursor": p["cursor"],
                "start_cursor": p["start_cursor"],
                "end_cursor": p["end_cursor"],
                "derived": len(p["derived_index"]),
                "gaps": len(p["gaps"]),
                "depends_on": list(p["depends_on"]),
                "created_at": p["created_at"],
                "updated_at": p["updated_at"],
            } for p in items[:max(1, limit)]]}

    def dependency_ids_locked(self) -> set:
        """Segments pinned by unfinished pipelines (GC exclusion set)."""
        out = set()
        for p in self._pipelines.values():
            if p["status"] in ("queued", "running", "blocked", "paused"):
                out.update(p.get("depends_on", ()))
        return out

    def _stats_locked(self) -> dict:
        ps = list(self._pipelines.values())
        return {
            "total": len(ps),
            "queued": sum(1 for p in ps if p["status"] == "queued"),
            "running": sum(1 for p in ps if p["status"] == "running"),
            "blocked": sum(1 for p in ps if p["status"] == "blocked"),
            "paused": sum(1 for p in ps if p["status"] == "paused"),
            "completed": sum(1 for p in ps if p["status"] == "completed"),
            "revoked": sum(1 for p in ps if p["status"] == "revoked"),
            "derived_entries": sum(len(p["derived_index"]) for p in ps),
            "pinned_segments": len(self.dependency_ids_locked()),
        }

    def stats(self) -> dict:
        with self.s._lock:
            return self._stats_locked()

    # ------------------------------------------------------------------ #
    # worker pool                                                         #
    # ------------------------------------------------------------------ #

    def _worker_loop(self) -> None:
        while True:
            pid = self._q.get()
            try:
                if pid is None:
                    return
                with self._inflight_lock:
                    if pid in self._inflight:
                        continue
                    self._inflight.add(pid)
                try:
                    self._run_pipeline(pid)
                finally:
                    with self._inflight_lock:
                        self._inflight.discard(pid)
            except Exception:
                log.exception("projection worker crashed running %s", pid)
                with self.s._lock:
                    p = self._pipelines.get(pid)
                    if p is not None and p["status"] not in TERMINAL \
                            and p["status"] != "paused":
                        p["status"] = "queued"
                        p["stage"] = "queued"
                        p["error"] = "worker exception; will retry"
                        self._touch_locked(p, persist=True)
            finally:
                self._q.task_done()

    def _watcher_loop(self) -> None:
        """Re-enqueue runnable pipelines (e.g. blocked ones post-repair)."""
        interval = max(0.02, getattr(self.s.cfg,
                                     "projection_recheck_sec", 0.2))
        while not self._stop.wait(interval):
            try:
                with self.s._lock:
                    runnable = [p["pipeline_id"] for p in self._pipelines.values()
                                if p["status"] in ("queued", "blocked")]
                with self._inflight_lock:
                    runnable = [pid for pid in runnable
                                if pid not in self._inflight]
                for pid in runnable:
                    self._q.put(pid)
            except Exception:
                log.exception("projection watcher error")

    # ------------------------------------------------------------------ #
    # per-pipeline state machine                                          #
    # ------------------------------------------------------------------ #

    def _run_pipeline(self, pid: str) -> None:
        with self.s._lock:
            p = self._pipelines.get(pid)
            if p is None or p["status"] in TERMINAL or p["status"] == "paused":
                return
            if p["status"] != "blocked":
                p["status"] = "running"
                p["stage"] = "starting"
                self._touch_locked(p, persist=True)

        batch_size = max(1, getattr(self.s.cfg, "projection_batch_size", 200))
        while not self._stop.is_set():
            with self.s._lock:
                p = self._pipelines.get(pid)
                if p is None or p["status"] in TERMINAL:
                    return
                if p["status"] == "paused":
                    return
                gen = p["gen"]
                cursor = p["cursor"]
                if cursor >= p["end_cursor"]:
                    self._complete_locked(p)
                    return
                # Plan the scan against an immutable snapshot of the view.
                view_units = self._plan_units_locked(p)
                snapshot = [(sid, meta.get("version", 1))
                            for sid, meta in view_units]

            # Heavy I/O outside the lock.
            try:
                derived, scanned_to = self._scan_batch(
                    p, view_units, cursor, batch_size)
            except _Blocked as b:
                with self.s._lock:
                    p = self._pipelines.get(pid)
                    if p is None or p["status"] in TERMINAL \
                            or p["status"] == "paused" or p["gen"] != gen:
                        return
                    self._record_gap_locked(p, b.gap)
                    p["status"] = "blocked"
                    p["stage"] = f"blocked:{b.gap['segment']}"
                    self._touch_locked(p, persist=True)
                log.info("projection %s blocked at cursor %d on %s",
                         pid, b.gap.get("resume_offset"), b.gap["segment"])
                return  # the watcher re-enqueues once the segment is healthy
            except _Drift:
                log.info("projection %s: segment algebra drifted; re-planning",
                         pid)
                self._stop.wait(0.02)
                continue

            action = self._commit_batch(pid, gen, cursor, p, snapshot,
                                        derived, scanned_to)
            if action == "done":
                return
            if action == "drift":
                self._stop.wait(0.02)
                continue
            # "advanced" -> loop for the next batch

    def _commit_batch(self, pid, gen, cursor, p_plan, snapshot,
                      derived, scanned_to) -> str:
        """Publish one scanned batch under the store lock.

        Returns one of: "done" (terminal/paused/blocked -> stop the loop),
        "drift" (segment algebra changed -> discard and re-plan),
        "advanced" (cursor published -> scan the next batch).
        """
        with self.s._lock:
            p = self._pipelines.get(pid)
            if p is None or p["status"] in TERMINAL:
                return "done"
            if p["status"] == "paused" or p["gen"] != gen:
                # A pause/resume (or revoke) raced the scan: the newer
                # generation owns the next stage; drop this batch.
                return "done"
            # Re-verify the algebra against the live manifest.
            for sid, version in snapshot:
                meta = self.s._seg_by_id.get(sid)
                if meta is None:
                    tomb = self.s.gc.tombstone(sid)
                    if tomb is not None:
                        self._record_gap_locked(
                            p, self._gap_for(p, tomb, cursor, evicted=True))
                        p["status"] = "blocked"
                        p["stage"] = f"blocked:{sid}"
                        self._touch_locked(p, persist=True)
                        return "done"
                    # Swap window (repair/gc) raced the scan: re-plan.
                    return "drift"
                if meta.get("version", 1) != version:
                    # A repair swapped the bytes mid-pipeline: the staged
                    # scan is stale.  Discard it and re-check from scratch.
                    return "drift"
                if meta["status"] == "quarantined":
                    # Got quarantined between plan and commit; the
                    # off-lock scan path normally catches this first.
                    self._record_gap_locked(p, self._gap_for(
                        p, meta, cursor))
                    p["status"] = "blocked"
                    p["stage"] = f"blocked:{meta['id']}"
                    self._touch_locked(p, persist=True)
                    return "done"

            if derived:
                generated_at = fmt_ts(utcnow())
                events = [self._synthesize(p, d, generated_at)
                          for d in derived]
                # Forced-abort point #2: crash before the batch's
                # derived entries are flushed (WAL fsync).
                self._fire_hook_locked(p, "entries_flushed")
                results = self.s._ingest_derived_locked(
                    pid, p["recipe_code"], events, generated_at)
            else:
                results = []

            # Forced-abort point #3: entries are durable but the new
            # cursor / derived index is not yet published.
            self._fire_hook_locked(p, "cursor_published")
            for d, r in zip(derived, results):
                # A "duplicate" result is the crash-window re-run: the
                # deterministic identity resolves to the very same archive
                # offset -- no double entry, and the index still records it.
                p["derived_index"].append({
                    "lineage_no": d["source_offset"],
                    "source_offset": d["source_offset"],
                    "offset": r["offset"],
                    "event_id": r["event_id"],
                    "target": r["device_id"],
                    "recipe": p["recipe_code"],
                    "generated_at": r["generated_at"],
                })
            p["cursor"] = scanned_to + 1
            p["status"] = "running"
            p["stage"] = "scanning"
            self._touch_locked(p, persist=True)
            if p["cursor"] >= p["end_cursor"]:
                self._complete_locked(p)
                return "done"
            return "advanced"

    def _plan_units_locked(self, p: dict) -> List[Tuple[str, dict]]:
        units = []
        for sid in p["view_segments"]:
            meta = self.s._seg_by_id.get(sid)
            if meta is None:
                # Keep the placeholder so scanning reports the gap.
                units.append((sid, {"id": sid, "missing": True,
                                    "first_offset": p["cursor"],
                                    "last_offset": p["cursor"]}))
                continue
            if meta["last_offset"] < p["cursor"] \
                    or meta["first_offset"] >= p["end_cursor"]:
                continue
            units.append((sid, meta))
        units.sort(key=lambda u: u[1]["first_offset"])
        return units

    def _scan_batch(self, p: dict, units, cursor: int, limit: int):
        """Scan up to ``limit`` whitelisted source records off-lock.

        Returns (derived_inputs, scanned_to).  A source gap raises
        _Blocked; nothing from the partial batch is committed in that
        case (gaps are never skipped over).
        """
        out: List[dict] = []
        scanned = cursor - 1
        end = p["end_cursor"]
        whitelist = set(p["sources"])
        for sid, meta0 in units:
            if len(out) >= limit:
                break
            if meta0.get("missing"):
                tomb = self.s.gc.tombstone(sid)
                if tomb is not None:
                    raise _Blocked(self._gap_for(p, tomb, cursor, evicted=True))
                # Directory swap / pending eviction raced the plan: the
                # watcher retries; treat as transient block (no gap noise
                # unless it persists).
                raise _Drift()
            meta = dict(meta0)
            if meta["last_offset"] <= scanned:
                continue
            if meta["status"] == "quarantined":
                raise _Blocked(self._gap_for(p, meta, cursor))
            try:
                recs, _ = segmod.scan_records(self.s.seg_root, meta,
                                              max(cursor, meta["first_offset"]))
            except segmod.SegmentCorrupt as c:
                self.s.quarantine(meta["id"], c.reason,
                                  expected_sha=meta.get("sha256"))
                live = self.s._seg_by_id.get(meta["id"])
                raise _Blocked(self._gap_for(p, live or meta, cursor))
            except FileNotFoundError:
                # Repair/GC swap raced the read: re-plan.
                raise _Drift()
            for rec in recs:
                off = rec["offset"]
                if off <= scanned or off >= end:
                    continue
                scanned = off
                ev = rec["event"]
                if ev["device_id"] in whitelist:
                    out.append({
                        "source_offset": off,
                        "source_device": ev["device_id"],
                        "seq": ev["seq"],
                        "device_ts": ev["device_ts"],
                        "event": ev,
                        "flags": rec.get("flags", {}),
                    })
                    if len(out) >= limit:
                        break
        if scanned < cursor:
            scanned = cursor - 1
        return out, scanned

    def _gap_for(self, p: dict, meta: dict, cursor: int,
                 evicted: bool = False) -> dict:
        """Build a gap descriptor with the precise resume cursor.

        The pipeline must not skip the range: after the segment is
        repaired it resumes at the first unprocessed offset, so the
        resume cursor is the segment's first offset at or after the
        pipeline cursor (not last+1).
        """
        first = max(meta["first_offset"], cursor)
        terminals = self._terminals_in(p, meta)
        return {
            "segment": meta["id"],
            "reason": "evicted" if evicted else meta.get(
                "quarantine_reason", "quarantined"),
            "first_offset": meta["first_offset"],
            "last_offset": meta["last_offset"],
            "resume_offset": first,
            "terminals": terminals,
            "at": fmt_ts(utcnow()),
        }

    def _terminals_in(self, p: dict, meta: dict) -> List[str]:
        """Whitelisted source terminals present inside the gapped unit."""
        try:
            idx = segmod.load_index(self.s.seg_root, meta["id"])
            present = set(idx.get("devices", {}))
        except Exception:
            # Index unreadable: conservatively report every whitelisted
            # terminal as potentially affected.
            present = set(p["sources"])
        return sorted(p["target_prefix"] + s
                      for s in p["sources"] if s in present) \
            or sorted(p["target_prefix"] + s for s in p["sources"])

    def _record_gap_locked(self, p: dict, gap: dict) -> None:
        for g in p["gaps"]:
            if g["segment"] == gap["segment"]:
                g.update(gap)
                return
        p["gaps"].append(gap)

    def _synthesize(self, p: dict, d: dict, generated_at: str) -> dict:
        """Apply the field recipe to one source record."""
        payload: Dict[str, Any] = {}
        for out_name, path in p["recipe"].items():
            payload[out_name] = self._extract(d["event"], path)
        return {
            "device_id": p["target_prefix"] + d["source_device"],
            "event_id": _lineage_event_id(p["recipe_code"], d["source_offset"]),
            "seq": d["seq"],
            "device_ts": d["device_ts"],
            "payload": payload,
            "source_offset": d["source_offset"],
            "generated_at": generated_at,
        }

    @staticmethod
    def _extract(event: dict, path: str):
        cur: Any = event
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    def _complete_locked(self, p: dict) -> None:
        now = fmt_ts(utcnow())
        p["status"] = "completed"
        p["stage"] = "completed"
        p["error"] = None
        # The whole declared range was consumed: every blocking gap was
        # healed and re-scanned, so none remains outstanding.
        p["gaps"] = []
        if not any(s["stage"] == "completed" for s in p["stages"]):
            p["stages"].append({"stage": "completed", "at": now})
        # Forced-abort point #4: crash before the terminal answer is
        # marked; restart re-runs (no new entries past the horizon) and
        # reaches the same constant answer.
        self._fire_hook_locked(p, "terminal_marked")
        p["final"] = self._view_locked(p)
        self._touch_locked(p, persist=True)
        log.info("projection %s completed: %d derived entries, cursor=%d",
                 p["pipeline_id"], len(p["derived_index"]), p["cursor"])

    # ------------------------------------------------------------------ #
    # views / journal                                                     #
    # ------------------------------------------------------------------ #

    def _view_locked(self, p: dict) -> dict:
        """Constant response shape: every key always present."""
        return {
            "pipeline_id": p["pipeline_id"],
            "status": p["status"],
            "stage": p["stage"],
            "gen": p["gen"],
            "stages": list(p.get("stages", [])),
            "declaration": {
                "view_token": p["view_token"],
                "sources": list(p["sources"]),
                "start_cursor": p["start_cursor"],
                "end_cursor": p["end_cursor"],
                "recipe": dict(p["recipe"]),
                "recipe_code": p["recipe_code"],
                "target_prefix": p["target_prefix"],
            },
            "view": {
                "view_token": p["view_token"],
                "end_offset": p["view_end_offset"],
                "segments": list(p["view_segments"]),
                "segment_count": len(p["view_segments"]),
            },
            "derived": [dict(e) for e in p.get("derived_index", [])],
            "gaps": [dict(g) for g in p.get("gaps", [])],
            "cursor": p["cursor"],
            "depends_on": list(p.get("depends_on", [])),
            "conflicts": [dict(c) for c in p.get("conflicts", [])],
            "error": p.get("error"),
            "created_at": p["created_at"],
            "updated_at": p["updated_at"],
        }

    def _touch_locked(self, p: dict, persist: bool = True) -> None:
        p["updated_at"] = fmt_ts(utcnow())
        if persist:
            self._persist_locked()

    def _persist_locked(self) -> None:
        atomic_write_json(self._path, {"pipelines": self._pipelines})

    def _fire_hook_locked(self, p: dict, phase: str) -> None:
        hook = self._phase_hook
        if hook is None:
            return
        try:
            hook(self._view_locked(p), phase)
        except Exception:
            log.exception("projection phase hook raised")


class _Blocked(Exception):
    """Internal: a source gap blocks further scanning."""

    def __init__(self, gap: dict):
        self.gap = gap
