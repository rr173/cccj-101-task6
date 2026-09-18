"""Auditable derived-lineage projection pipelines (POST /v3/projections).

A *pipeline* asynchronously scans a **static declared view** of raw entries
and emits *derived entries* into ordinary target streams (terminals named
``<target_prefix><source_device_id>``).  Every derived entry carries its
bloodline: a deterministic lineage entry id (血缘条目号), the recipe code
(配方代号) and the generation timestamp (生成时刻); late-arrival and
clock-rollback flags are inherited from the source entry, and terminal
sequence numbers are assigned per target stream.

Idempotency
-----------
The pipeline id is a cross-process idempotency key: double clicks, two
workers opening the same pipeline, or a retried network request all map to
the single journaled pipeline -- no extra entries are ever created.
Re-submitting the same id with a different view, cursors, recipe, source
whitelist or prefix fails with 409 and the original pipeline is untouched.

Static views
------------
A pipeline may only read its declared view (a /v3/views token or an
existing v1 freeze token).  The view is sealed at creation (open buffer
flushed, horizon pinned); raw content arriving afterwards lands in higher
offsets and can never leak into the pipeline, even on a re-run.

Gaps
----
When a source segment is quarantined ("封存") inside the scan range, the
pipeline blocks and reports every gap: segment, affected terminals and the
exact resume cursor (last_offset + 1).  Nothing is hidden.  After a repair
restores the segment the worker is nudged and continues at that cursor.

Control
-------
pause / resume / abort each carry a strictly increasing control epoch;
stale or replayed tokens are rejected (409) and can never rewrite a later
stage.  Active pipelines pin every source segment against capacity
reclamation; abort (or completion) releases the pin.

Crash safety
------------
The pipeline manifest (listing, cursors, derived index, dependency set,
terminal answer) lives in state/projections.json.  Per batch the worker
journals the batch intent, flushes derived entries (WAL fsync), then
publishes the new cursor -- with forced-abort injection points after each
seam (manifest persisted / batch flushed / cursor published / before the
terminal mark).  On restart only fully journalled batches are reconciled:
re-flushing is idempotent (deterministic derived event ids dedup inside the
target terminal), so there are never half entries, double entries, or
skipped gaps.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
import uuid
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple

from . import segments as segmod
from .models import fmt_ts, new_flags, utcnow
from .util import atomic_write_json, load_json

log = logging.getLogger("eventarch.projections")

TERMINAL_STATUSES = ("succeeded", "aborted")
ACTIVE_STATUSES = ("queued", "running", "paused", "blocked")

MISSING = object()


class ProjectionConflict(Exception):
    """Declaration/control/source conflict (HTTP 409)."""

    def __init__(self, msg: str, reasons: Optional[List[str]] = None):
        super().__init__(msg)
        self.reasons = reasons or [msg]


class ProjectionGone(Exception):
    """The requested range crosses an evicted source run (HTTP 410)."""

    def __init__(self, cursor: int, first_offset: int, last_offset: int):
        super().__init__(
            f"source range crosses an evicted run; resume at cursor {cursor}")
        self.cursor = cursor
        self.first_offset = first_offset
        self.last_offset = last_offset


class _StaleBatch(Exception):
    """Segment generation drifted between scan and commit: discard, replan."""


class _StopScanning(Exception):
    pass


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _canonical(obj: Any) -> str:
    import json
    return json.dumps(obj, separators=(",", ":"), sort_keys=True,
                      ensure_ascii=False)


def _dig(path: str, root: Any):
    cur = root
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return MISSING
    return cur


# ---------------------------------------------------------------------- #
# request validation                                                     #
# ---------------------------------------------------------------------- #

def _require_str(value, field: str, max_len: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > max_len:
        raise ValueError(f"{field} must be a non-empty string (<={max_len} chars)")
    return value


def _require_offset(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer offset")
    return value


def _normalize_sources(sources: Any) -> List[str]:
    if not isinstance(sources, list) or not sources:
        raise ValueError("sources must be a non-empty array of device ids")
    out: List[str] = []
    for s in sources:
        if not isinstance(s, str) or not s or len(s) > 256 or "/" in s:
            raise ValueError("every source id must be a non-empty string")
        if s not in out:
            out.append(s)
    return sorted(out)


def _normalize_recipe(recipe: Any) -> Tuple[Dict[str, dict], str]:
    """A field recipe maps target field -> {"source": dotted.path} and/or
    {"const": value} and/or {"default": value}."""
    if not isinstance(recipe, dict) or not recipe:
        raise ValueError("recipe must be a non-empty object of field specs")
    norm: Dict[str, dict] = {}
    for field, spec in recipe.items():
        if not isinstance(field, str) or not field:
            raise ValueError("recipe keys must be non-empty field names")
        if not isinstance(spec, dict):
            raise ValueError(f"recipe spec for {field!r} must be an object")
        entry: Dict[str, Any] = {}
        if "source" in spec:
            if not isinstance(spec["source"], str) or not spec["source"]:
                raise ValueError(f"{field}.source must be a non-empty path")
            entry["source"] = spec["source"]
        if "const" in spec:
            entry["const"] = spec["const"]
        if "default" in spec:
            entry["default"] = spec["default"]
        if "source" not in entry and "const" not in entry:
            raise ValueError(
                f"recipe spec for {field!r} needs 'source' or 'const'")
        norm[field] = entry
    return norm, _sha1(_canonical(norm))


def _apply_recipe(recipe: Dict[str, dict], rec: dict) -> dict:
    ev = rec["event"]
    root = {
        "event": ev,
        "payload": ev.get("payload"),
        "offset": rec["offset"],
        "device_id": ev["device_id"],
        "event_id": ev["event_id"],
        "seq": ev["seq"],
        "device_ts": ev["device_ts"],
        "ingest_ts": rec["ingest_ts"],
        "flags": rec.get("flags", {}),
    }
    out: Dict[str, Any] = {}
    for field, spec in recipe.items():
        if "const" in spec:
            out[field] = spec["const"]
            continue
        val = _dig(spec["source"], root)
        if val is MISSING:
            if "default" in spec:
                out[field] = spec["default"]
            # missing without default: field omitted, entry still emitted
            continue
        out[field] = val
    return out


class ProjectionManager:
    def __init__(self, store):
        self.s = store
        self.state_dir = store.state_dir
        self._path = os.path.join(self.state_dir, "projections.json")
        self._views: Dict[str, dict] = {}
        self._pipelines: Dict[str, dict] = {}
        self._q: "Queue[Optional[str]]" = Queue()
        self._workers: List[threading.Thread] = []
        self._scheduled: set = set()
        self._stop = threading.Event()
        # Test/observability hook, invoked OUTSIDE the store lock:
        #   hook(pipeline_id, phase)
        # phase in {"manifest_persisted", "batch_flushed",
        #           "cursor_published", "terminal_pending"}
        self._phase_hook = None

    # ------------------------------------------------------------------ #
    # views (static declared horizons)                                    #
    # ------------------------------------------------------------------ #

    def create_view(self, note: str = "") -> dict:
        """Seal the open buffer and pin a static, named horizon.

        Raw entries arriving afterwards get higher offsets and can never
        enter this view.  The view is deliberately NOT registered as a v1
        freeze: an active pipeline pins its own dependency set, so segments
        are released for reclamation when the pipeline finishes/aborts
        rather than being pinned forever by a snapshot reference.
        """
        note = note if isinstance(note, str) else ""
        with self.s._lock:
            pending = self.s.gc.pending_ids()
            self.s._seal_open()
            view = {
                "token": f"vw-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}"
                         f"-{uuid.uuid4().hex[:8]}",
                "note": note[:256],
                "created_at": fmt_ts(utcnow()),
                "end_offset": self.s._next_offset,
                "segments": [m["id"] for m in self.s.manifest["segments"]
                             if m["id"] not in pending],
            }
            self._views[view["token"]] = view
            self._persist_locked()
            log.info("projection view %s sealed at end_offset=%d (%d segments)",
                     view["token"], view["end_offset"], len(view["segments"]))
            return self._view_summary(view)

    def list_views(self) -> List[dict]:
        with self.s._lock:
            return [self._view_summary(v)
                    for v in sorted(self._views.values(),
                                    key=lambda v: v["created_at"])]

    def _resolve_view_locked(self, token: str) -> Optional[dict]:
        v = self._views.get(token)
        if v is not None:
            return v
        frz = next((f for f in self.s._freezes if f["id"] == token), None)
        if frz is not None:
            # Accept a v1 freeze token as a declared view (read-only compat);
            # such a snapshot also keeps its permanent GC pin independently.
            return {
                "token": frz["id"], "note": frz.get("note", ""),
                "created_at": frz["created_at"], "end_offset": frz["end_offset"],
                "segments": list(frz.get("segments", [])), "freeze": True,
            }
        return None

    def _view_summary(self, v: dict) -> dict:
        return {
            "token": v["token"],
            "end_offset": v["end_offset"],
            "segments": list(v["segments"]),
            "segment_count": len(v["segments"]),
            "created_at": v["created_at"],
            "note": v.get("note", ""),
        }

    # ------------------------------------------------------------------ #
    # pipeline declaration                                                #
    # ------------------------------------------------------------------ #

    def create_pipeline(self, pipeline_id, view_token, sources,
                        from_offset, to_offset, recipe, recipe_code,
                        target_prefix, batch_size=None,
                        start_paused: bool = False) -> Tuple[dict, bool]:
        """Returns (constant response view, created).  Raises on conflict."""
        pid = _require_str(pipeline_id, "pipeline_id")
        token = _require_str(view_token, "view_token")
        source_list = _normalize_sources(sources)
        start = _require_offset(from_offset, "from_offset")
        end = _require_offset(to_offset, "to_offset")
        recipe_norm, recipe_fp = _normalize_recipe(recipe)
        code = _require_str(recipe_code, "recipe_code")
        prefix = _require_str(target_prefix, "target_prefix", max_len=128)
        if batch_size is None:
            batch_size = getattr(self.s.cfg, "projection_batch_size", 200)
        batch_size = _require_offset(batch_size, "batch_size")
        batch_size = max(1, min(batch_size, self.s.cfg.max_batch))
        if end <= start:
            raise ValueError("to_offset must be greater than from_offset")

        with self.s._lock:
            view = self._resolve_view_locked(token)
            if view is None:
                from .store import NotFound
                raise NotFound(f"view token {token} not found")
            if end > view["end_offset"] or start > view["end_offset"]:
                raise ValueError(
                    f"cursors [{start}..{end}) exceed the view horizon "
                    f"{view['end_offset']}")

            existing = self._pipelines.get(pid)
            declaration = {
                "view_token": view["token"],
                "sources": source_list,
                "from_offset": start,
                "to_offset": end,
                "recipe_code": code,
                "recipe_fingerprint": recipe_fp,
                "target_prefix": prefix,
                "batch_size": batch_size,
            }
            if existing is not None:
                diff = self._declaration_diff(existing, declaration)
                if diff:
                    raise ProjectionConflict(
                        f"pipeline {pid} already exists with a different "
                        f"declaration", diff)
                return self.response_locked(existing), False

            # The range may not cross an evicted run: v3 views only carry
            # live segments, so a tombstone overlapping the declared range
            # means requested offsets are unavailable (410 + exact cursor).
            tomb = next((t for t in self.s.gc.tombstones
                         if t["last_offset"] >= start
                         and t["first_offset"] < end), None)
            if tomb is not None:
                raise ProjectionGone(
                    self.s.gc.cursor_after(max(start, tomb["first_offset"])),
                    tomb["first_offset"], tomb["last_offset"])
            # A reclamation accepted but not yet published would change the
            # view under our feet; refuse and let the caller retry.
            for seg_id in self.s.gc.pending_ids():
                meta = self.s._seg_by_id.get(seg_id)
                if meta is not None and meta["last_offset"] >= start \
                        and meta["first_offset"] < end:
                    raise ProjectionConflict(
                        f"source range overlaps segment {seg_id} whose "
                        f"reclamation is in flight; retry shortly")

            # Two live pipelines may not fan out the same source device over
            # overlapping ranges: both callers must see a named conflict.
            for other in self._pipelines.values():
                if other["status"] in TERMINAL_STATUSES:
                    continue
                overlap_sources = sorted(
                    set(source_list) & set(other["source_whitelist"]))
                if (overlap_sources
                        and start < other["to_offset"]
                        and end > other["from_offset"]):
                    reasons = [
                        f"sources {overlap_sources} over ["
                        f"{max(start, other['from_offset'])}.."
                        f"{min(end, other['to_offset'])}) are already occupied "
                        f"by pipeline {other['id']}"]
                    self._record_conflict_locked(other, pid, reasons[0])
                    raise ProjectionConflict(
                        f"source occupied by pipeline {other['id']}", reasons)

            depends = [m["id"] for m in self.s.manifest["segments"]
                       if m["id"] in set(view["segments"])
                       and m["last_offset"] >= start
                       and m["first_offset"] < end]
            depends.sort()

            now = fmt_ts(utcnow())
            p = {
                "id": pid,
                "status": "paused" if start_paused else "queued",
                "stage": "accepted:paused" if start_paused else "accepted",
                "view_token": view["token"],
                "view_segments": list(view["segments"]),
                "view_horizon": view["end_offset"],
                "source_whitelist": source_list,
                "from_offset": start,
                "to_offset": end,
                "recipe": recipe_norm,
                "recipe_code": code,
                "recipe_fingerprint": recipe_fp,
                "target_prefix": prefix,
                "batch_size": batch_size,
                "cursor": start,
                "seq_counters": {},
                "derived_count": 0,
                "entries": [],
                "batches": [],
                "gaps": [],
                "depends_on": depends,
                "stages": [{"seq": 0, "stage": "accepted", "at": now}],
                "control_epoch": 0,
                "conflicts": [],
                "final_response": None,
                "error": None,
                "created_at": now,
                "updated_at": now,
            }
            self._pipelines[pid] = p
            self._persist_locked()
            self._fire_hook(pid, "manifest_persisted")
            self._schedule_locked(p)
            log.info("pipeline %s accepted: view=%s range=[%d..%d) sources=%d",
                     pid, view["token"], start, end, len(source_list))
            return self.response_locked(p), True

    @staticmethod
    def _declaration_diff(p: dict, decl: dict) -> List[str]:
        mapping = {
            "view_token": "view",
            "sources": "source whitelist",
            "from_offset": "head cursor",
            "to_offset": "tail cursor",
            "recipe_fingerprint": "field recipe",
            "recipe_code": "recipe code",
            "target_prefix": "target prefix",
            "batch_size": "batch size",
        }
        current = {
            "view_token": p["view_token"],
            "sources": p["source_whitelist"],
            "from_offset": p["from_offset"],
            "to_offset": p["to_offset"],
            "recipe_fingerprint": p["recipe_fingerprint"],
            "recipe_code": p["recipe_code"],
            "target_prefix": p["target_prefix"],
            "batch_size": p["batch_size"],
        }
        return [f"{label} differs" for key, label in mapping.items()
                if current[key] != decl[key]]

    def _record_conflict_locked(self, p: dict, other_id: str, note: str) -> None:
        p["conflicts"].append({
            "other": other_id, "at": fmt_ts(utcnow()), "note": note})
        del p["conflicts"][:-10]
        p["updated_at"] = fmt_ts(utcnow())
        self._persist_locked()

    # ------------------------------------------------------------------ #
    # control                                                             #
    # ------------------------------------------------------------------ #

    def control(self, pid: str, action: str, epoch: int) -> dict:
        if action not in ("pause", "resume", "abort"):
            raise ValueError(f"unknown control action {action!r}")
        if isinstance(epoch, bool) or not isinstance(epoch, int):
            raise ValueError("epoch must be an integer control generation")
        with self.s._lock:
            p = self._pipelines.get(pid)
            if p is None:
                from .store import NotFound
                raise NotFound(f"pipeline {pid} not found")
            # Monotone generations only: a stale or replayed token must
            # never rewrite a later stage.
            if epoch <= p["control_epoch"]:
                raise ProjectionConflict(
                    f"stale control token: epoch {epoch} <= "
                    f"{p['control_epoch']}")
            now = fmt_ts(utcnow())
            changed = False
            if action == "pause":
                if p["status"] in TERMINAL_STATUSES:
                    raise ProjectionConflict(
                        f"pipeline {pid} is {p['status']}; cannot pause")
                changed = p["status"] != "paused"
                p["status"] = "paused"
                p["stage"] = "paused"
            elif action == "resume":
                if p["status"] in TERMINAL_STATUSES:
                    raise ProjectionConflict(
                        f"pipeline {pid} is {p['status']}; cannot resume")
                if p["status"] != "paused":
                    raise ProjectionConflict(
                        f"pipeline {pid} is not paused (status={p['status']})")
                p["status"] = "queued"
                p["stage"] = "resumed"
                changed = True
            else:  # abort
                if p["status"] in TERMINAL_STATUSES:
                    raise ProjectionConflict(
                        f"pipeline {pid} is already {p['status']}")
                p["status"] = "aborted"
                p["stage"] = "aborted"
                self._add_stage_locked(p, "aborted")
                p["final_response"] = self.response_locked(p)
                changed = True
            p["control_epoch"] = epoch
            p["updated_at"] = now
            if changed:
                self._add_stage_locked(p, f"{action}:epoch={epoch}")
            self._persist_locked()
            if action == "resume":
                self._schedule_locked(p)
            return self.response_locked(p)

    # ------------------------------------------------------------------ #
    # queries                                                             #
    # ------------------------------------------------------------------ #

    def get_pipeline(self, pid: str) -> dict:
        with self.s._lock:
            p = self._pipelines.get(pid)
            if p is None:
                from .store import NotFound
                raise NotFound(f"pipeline {pid} not found")
            return self.response_locked(p)

    def list_pipelines(self) -> List[dict]:
        with self.s._lock:
            return [self.response_locked(self._pipelines[pid], entries_limit=0)
                    for pid in sorted(self._pipelines,
                                      key=lambda k: self._pipelines[k]["created_at"])]

    def response_locked(self, p: dict, entries_limit: Optional[int] = None) -> dict:
        """The constant response shape returned for every pipeline answer."""
        entries = p["entries"]
        if entries_limit is not None:
            entries = entries[:entries_limit]
        source_counts = self._source_counts_locked(p)
        return {
            "pipeline_id": p["id"],
            "status": p["status"],
            "stage": p["stage"],
            "control_epoch": p["control_epoch"],
            "stages": list(p["stages"]),
            "view": {
                "token": p["view_token"],
                "end_offset": p["view_horizon"],
                "range": [p["from_offset"], p["to_offset"]],
                "segments": list(p["view_segments"]),
                "segment_count": len(p["view_segments"]),
                "sources": source_counts,
            },
            "derived": [self._entry_view(e) for e in entries],
            "derived_count": p["derived_count"],
            "cursor": p["cursor"],
            "next_cursor": p["cursor"],
            "gaps": [dict(g) for g in p["gaps"]],
            "depends_on": list(p["depends_on"]),
            "conflicts": [dict(c) for c in p["conflicts"]],
            "error": p["error"],
            "created_at": p["created_at"],
            "updated_at": p["updated_at"],
        }

    @staticmethod
    def _entry_view(e: dict) -> dict:
        return {k: e[k] for k in (
            "lineage_id", "derived_event_id", "source_offset", "terminal",
            "seq", "offset", "recipe_code", "generated_at", "batch_no")}

    def _source_counts_locked(self, p: dict) -> Dict[str, int]:
        """Raw entries per whitelisted source within [from, to) -- the
        input-horizon summary, counted from the in-memory index."""
        counts = {dev: 0 for dev in p["source_whitelist"]}
        for dev in p["source_whitelist"]:
            state = self.s._devices.get(dev)
            if state is None:
                continue
            for e in state.entries:
                if p["from_offset"] <= e.offset < p["to_offset"]:
                    counts[dev] += 1
        return counts

    # ------------------------------------------------------------------ #
    # GC integration                                                      #
    # ------------------------------------------------------------------ #

    def dependency_segments_locked(self) -> set:
        """Segments pinned by every non-terminal pipeline."""
        out: set = set()
        for p in self._pipelines.values():
            if p["status"] not in TERMINAL_STATUSES:
                out.update(p["depends_on"])
        return out

    def notify_repaired(self, seg_id: str) -> None:
        """Called by the store after a successful rebuild: requeue any
        pipeline blocked on that segment so it resumes at its gap cursor."""
        with self.s._lock:
            for p in self._pipelines.values():
                if p["status"] != "blocked":
                    continue
                hit = [g for g in p["gaps"] if g["segment"] == seg_id]
                if not hit:
                    continue
                meta = self.s._seg_by_id.get(seg_id)
                if meta is None or meta["status"] != "sealed":
                    continue
                for g in hit:
                    g["resolved_at"] = fmt_ts(utcnow())
                p["gaps"] = [g for g in p["gaps"] if "resolved_at" not in g]
                p["status"] = "queued"
                p["stage"] = f"rechecking:{seg_id}"
                p["error"] = None
                p["updated_at"] = fmt_ts(utcnow())
                self._add_stage_locked(p, f"gap_resolved:{seg_id}")
                self._persist_locked()
                self._schedule_locked(p)

    # ------------------------------------------------------------------ #
    # worker pool                                                         #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Reconcile journalled batches after a restart, then schedule."""
        with self.s._lock:
            for pid, p in list(self._pipelines.items()):
                if p["status"] in TERMINAL_STATUSES:
                    continue
                # Finish a batch whose entries were flushed but whose cursor
                # was never published.  Re-flushing is idempotent.
                unfinished = [b for b in p["batches"]
                              if not b.get("published")]
                for b in unfinished:
                    self._publish_recovered_batch_locked(p, b)
                if p["status"] == "paused":
                    continue
                if p["status"] in TERMINAL_STATUSES:
                    continue
                p["status"] = "queued"
                p["stage"] = "queued"
                p["updated_at"] = fmt_ts(utcnow())
                self._persist_locked()
                self._schedule_locked(p)
        n = max(1, getattr(self.s.cfg, "projection_workers", 1))
        for i in range(n):
            t = threading.Thread(target=self._worker_loop,
                                 name=f"projection-{i}", daemon=True)
            t.start()
            self._workers.append(t)

    def close(self) -> None:
        self._stop.set()
        for _ in self._workers:
            self._q.put(None)
        for t in self._workers:
            t.join(timeout=5)

    def _park(self, pid: str) -> None:
        """Return an in-flight pipeline to the durable queue at shutdown."""
        with self.s._lock:
            p = self._pipelines.get(pid)
            if p is None or p["status"] in TERMINAL_STATUSES:
                return
            if p["status"] == "running":
                p["status"] = "queued"
                p["stage"] = "queued"
                p["updated_at"] = fmt_ts(utcnow())
                self._persist_locked()

    def _schedule_locked(self, p: dict) -> None:
        if p["status"] in TERMINAL_STATUSES or p["status"] == "paused":
            return
        if p["id"] in self._scheduled:
            return
        self._scheduled.add(p["id"])
        self._q.put(p["id"])

    def _worker_loop(self) -> None:
        while True:
            pid = self._q.get()
            try:
                if pid is None:
                    return
                try:
                    self._run_pipeline(pid)
                except Exception:
                    log.exception("projection worker crashed running %s", pid)
                    with self.s._lock:
                        p = self._pipelines.get(pid)
                        if p is not None and p["status"] == "running":
                            p["status"] = "queued"
                            p["stage"] = "queued"
                            p["error"] = "worker exception; retry scheduled"
                            p["updated_at"] = fmt_ts(utcnow())
                            self._persist_locked()
                            self._scheduled.discard(pid)
                            self._schedule_locked(p)
            finally:
                self._scheduled.discard(pid)
                self._q.task_done()

    def _run_pipeline(self, pid: str) -> None:
        with self.s._lock:
            p = self._pipelines.get(pid)
            if p is None or p["status"] in TERMINAL_STATUSES \
                    or p["status"] == "paused":
                return
            p["status"] = "running"
            p["stage"] = "starting"
            p["updated_at"] = fmt_ts(utcnow())
            self._add_stage_locked(p, "running")
            self._persist_locked()

        while not self._stop.is_set():
            with self.s._lock:
                p = self._pipelines.get(pid)
                if p is None or p["status"] in TERMINAL_STATUSES:
                    return
                if p["status"] == "paused":
                    return
                plan = self._plan_batch_locked(p)

            if plan is None:
                if self._stop.is_set():
                    self._park(pid)
                    return
                # cursor >= tail: inject the forced-abort point *before* the
                # terminal mark, then seal.
                self._fire_hook(pid, "terminal_pending")
                with self.s._lock:
                    p = self._pipelines.get(pid)
                    if p is None or p["status"] in TERMINAL_STATUSES:
                        return
                    if p["status"] == "paused":
                        return
                    if self._stop.is_set():
                        self._park(pid)
                        return
                    if p["cursor"] >= p["to_offset"]:
                        p["status"] = "succeeded"
                        p["stage"] = "succeeded"
                        p["error"] = None
                        self._add_stage_locked(p, "succeeded")
                        p["updated_at"] = fmt_ts(utcnow())
                        p["final_response"] = self.response_locked(p)
                        self._persist_locked()
                        log.info("pipeline %s succeeded: %d derived entries",
                                 pid, p["derived_count"])
                        return
                continue

            if plan["kind"] == "blocked":
                with self.s._lock:
                    p = self._pipelines.get(pid)
                    if p is None or p["status"] in TERMINAL_STATUSES:
                        return
                    if p["status"] == "paused":
                        return
                    # Only publish gaps once per blocking episode.
                    known = {g["segment"] for g in p["gaps"]}
                    new_gaps = [g for g in plan["gaps"]
                                if g["segment"] not in known]
                    if new_gaps or p["status"] != "blocked":
                        p["gaps"].extend(new_gaps)
                        p["status"] = "blocked"
                        if new_gaps:
                            p["stage"] = "blocked:" + ",".join(
                                g["segment"] for g in new_gaps)
                        self._add_stage_locked(p, "blocked")
                        p["updated_at"] = fmt_ts(utcnow())
                        self._persist_locked()
                return  # nudged by notify_repaired / resume

            # Heavy segment I/O outside the global lock.
            try:
                scanned = self._scan_batch(plan)
            except segmod.SegmentCorrupt as exc:
                self.s.quarantine(exc.seg_id, exc.reason,
                                  expected_sha=plan["versions"].get(exc.seg_id))
                continue  # next iteration lists the gap and blocks
            except FileNotFoundError:
                continue  # repair directory swap: replan
            if scanned is None:
                continue  # raced the view boundary; replan

            records, cursor_after, source_offsets, scanned_versions = scanned
            with self.s._lock:
                p = self._pipelines.get(pid)
                if p is None or p["status"] in TERMINAL_STATUSES \
                        or p["status"] == "paused":
                    return
                if p["cursor"] != plan["cursor"]:
                    continue  # another transition happened: replan
                # Segment generation drift: quarantine/repair changed a unit
                # between the scan and the commit -> discard and recheck.
                drifted = self._drifted_units_locked(plan, scanned_versions)
                if drifted:
                    if self._block_on_drift_locked(p, plan, drifted):
                        return
                    # Repaired/quarantined in place: discard stale bytes and
                    # recheck from the same cursor.
                    log.info("pipeline %s discards stale batch: %s",
                             pid, ",".join(drifted))
                    p["error"] = f"segment generation drifted: {drifted}"
                    p["updated_at"] = fmt_ts(utcnow())
                    self._persist_locked()
                    continue
                self._commit_batch_locked(
                    p, records, cursor_after, source_offsets)
                # If this batch drained the healthy prefix up to a
                # quarantined unit, publish the gap and block.
                if plan.get("block_at") is not None \
                        and cursor_after >= plan["block_at"]:
                    fresh = self._gaps_for_locked(
                        p, [dict(self.s._seg_by_id[g["segment"]])
                            for g in plan["pending_gaps"]
                            if g["segment"] in self.s._seg_by_id])
                    known = {g["segment"] for g in p["gaps"]}
                    fresh = [g for g in fresh if g["segment"] not in known]
                    p["gaps"].extend(fresh or plan["pending_gaps"])
                    p["status"] = "blocked"
                    p["stage"] = "blocked:" + ",".join(
                        g["segment"] for g in plan["pending_gaps"])
                    self._add_stage_locked(p, "blocked")
                    p["updated_at"] = fmt_ts(utcnow())
                    self._persist_locked()
                    return

        # _stop set mid-scan: park the pipeline so the next process or
        # close() sequence resumes it from the unchanged cursor.
        self._park(pid)

    # ------------------------------------------------------------------ #
    # batch planning / scanning                                           #
    # ------------------------------------------------------------------ #

    def _plan_batch_locked(self, p: dict) -> Optional[dict]:
        """Snapshot the units for the next batch and spot quarantined gaps.

        A healthy prefix before the first quarantined unit may still be
        scanned; once the cursor reaches the quarantined unit the pipeline
        blocks and reports it (never silently skips its offsets).
        """
        cursor = p["cursor"]
        if cursor >= p["to_offset"]:
            return None
        view_ids = set(p["view_segments"])
        units = [dict(m) for m in self.s.manifest["segments"]
                 if m["id"] in view_ids
                 and m["last_offset"] >= cursor
                 and m["first_offset"] < p["to_offset"]]
        units.sort(key=lambda m: m["first_offset"])

        # The unit that currently owns the cursor (offset points into it).
        cover = next((m for m in units
                      if m["first_offset"] <= cursor <= m["last_offset"]),
                     None)
        if cover is not None and cover["status"] == "quarantined":
            return {"kind": "blocked", "cursor": cursor,
                    "gaps": self._gaps_for_locked(p, units)}
        # No unit covers the cursor (it sits on a boundary).  If the very
        # next unit at that boundary is quarantined, block immediately
        # rather than looping on an empty healthy prefix.
        if cover is None and units and units[0]["first_offset"] >= cursor:
            nxt = next((m for m in units if m["first_offset"] >= cursor),
                       None)
            if nxt is not None and nxt["status"] == "quarantined":
                return {"kind": "blocked", "cursor": cursor,
                        "gaps": self._gaps_for_locked(p, units)}

        # Units strictly ahead of the cursor: if the very next one (at
        # last_examined + 1) is quarantined, scanning must stop at its edge
        # and block after this batch drains the healthy prefix.
        healthy = [m for m in units if m["status"] != "quarantined"]
        blocked_unit = None
        if cover is None and units and units[0]["status"] == "quarantined":
            blocked_unit = units[0]
        else:
            boundary = cursor
            for m in healthy:
                if m["first_offset"] <= cursor <= m["last_offset"]:
                    boundary = m["last_offset"] + 1
            nxt = next((m for m in units if m["first_offset"] == boundary), None)
            if nxt is not None and nxt["status"] == "quarantined":
                blocked_unit = nxt
        return {
            "kind": "scan",
            "cursor": cursor,
            "to": p["to_offset"],
            "whitelist": set(p["source_whitelist"]),
            "batch_size": p["batch_size"],
            "recipe": p["recipe"],
            "units": healthy,
            "versions": {m["id"]: m.get("version", 1) for m in healthy},
            "block_at": blocked_unit["first_offset"] if blocked_unit else None,
            "pending_gaps": ([self._gap_for_locked(p, blocked_unit)]
                             if blocked_unit is not None else []),
            "pipeline_id": p["id"],
            "view_token": p["view_token"],
            "recipe_code": p["recipe_code"],
            "recipe_fp": p["recipe_fingerprint"],
            "prefix": p["target_prefix"],
        }

    def _gap_for_locked(self, p: dict, m: dict) -> dict:
        terminals = sorted({
            p["target_prefix"] + dev
            for dev in p["source_whitelist"]
            if self._segment_has_device_locked(m["id"], dev)})
        if not terminals:
            # Index unreadable/unknowable: conservatively name every
            # whitelisted terminal as potentially affected.
            terminals = [p["target_prefix"] + dev
                         for dev in p["source_whitelist"]]
        return {
            "segment": m["id"],
            "reason": m.get("quarantine_reason", "quarantined"),
            "first_offset": m["first_offset"],
            "last_offset": m["last_offset"],
            "resume_offset": m["last_offset"] + 1,
            "terminals": terminals,
        }

    def _gaps_for_locked(self, p: dict, units: List[dict]) -> List[dict]:
        return [self._gap_for_locked(p, m) for m in units
                if m["status"] == "quarantined"]

    def _segment_has_device_locked(self, seg_id: str, dev_id: str) -> bool:
        state = self.s._devices.get(dev_id)
        if state is None:
            return False
        return any(e.seg_id == seg_id for e in state.entries)

    def _scan_batch(self, plan: dict):
        """Read the planned healthy units (I/O, no lock) up to the batch
        size / view tail.  Returns (candidate records, cursor_after,
        source offsets, per-unit versions) or None on a boundary race.

        A unit that became quarantined while reading surfaces as
        SegmentCorrupt (store.quarantine marks it, next plan blocks).
        """
        if plan["kind"] == "blocked":
            return None
        candidates: List[dict] = []
        source_offsets: List[int] = []
        scanned_through = plan["cursor"] - 1
        versions: Dict[str, int] = {}
        # Never read past the view tail nor into the quarantined unit the
        # plan proved is next; its offsets must not be silently consumed.
        stop = plan["to"]
        if plan.get("block_at") is not None:
            stop = min(stop, plan["block_at"])
        full = False
        for meta in plan["units"]:
            if full or meta["first_offset"] >= stop:
                break
            versions[meta["id"]] = meta.get("version", 1)
            recs, _ = segmod.scan_records(
                self.s.seg_root, meta, max(plan["cursor"], meta["first_offset"]))
            for rec in recs:
                off = rec["offset"]
                if off < plan["cursor"]:
                    continue
                if off >= stop:
                    full = True
                    break
                scanned_through = off
                if rec["event"]["device_id"] in plan["whitelist"]:
                    payload = _apply_recipe(plan["recipe"], rec)
                    candidates.append({
                        "source": rec,
                        "payload": payload,
                        "source_offset": off,
                        "device": rec["event"]["device_id"],
                    })
                    source_offsets.append(off)
                    if len(candidates) >= plan["batch_size"]:
                        full = True
                        break
        # A fully-consumed empty prefix still moves the cursor forward; a
        # scan that examined nothing (units vanished) replans.
        if scanned_through < plan["cursor"]:
            return None
        cursor_after = min(stop, scanned_through + 1)
        return candidates, cursor_after, source_offsets, versions

    def _block_on_drift_locked(self, p: dict, plan: dict,
                               drifted: List[str]) -> bool:
        """A drifted unit that is now quarantined/evicted is a real gap.

        Publish it (exact resume cursor + terminals) and block; the repair
        nudge / a future reclamation recovery will resume.  Units merely
        repaired to other content return False so the caller replans.
        """
        blocking = False
        for token in drifted:
            seg_id = token.split("(", 1)[0]
            meta = self.s._seg_by_id.get(seg_id)
            gap = None
            if meta is not None and meta["status"] == "quarantined":
                gap = self._gap_for_locked(p, meta)
            elif meta is None and self.s.gc.tombstone(seg_id) is not None:
                tomb = self.s.gc.tombstone(seg_id)
                gap = {
                    "segment": seg_id,
                    "reason": "evicted",
                    "first_offset": tomb["first_offset"],
                    "last_offset": tomb["last_offset"],
                    "resume_offset": self.s.gc.cursor_after(
                        max(p["cursor"], tomb["first_offset"])),
                    "terminals": [p["target_prefix"] + dev
                                  for dev in p["source_whitelist"]],
                }
            if gap is None:
                continue
            if not any(g["segment"] == gap["segment"] for g in p["gaps"]):
                p["gaps"].append(gap)
            p["status"] = "blocked"
            p["stage"] = f"blocked:{gap['segment']}"
            p["error"] = f"source unit {seg_id} became unavailable mid-scan"
            self._add_stage_locked(p, "blocked")
            p["updated_at"] = fmt_ts(utcnow())
            self._persist_locked()
            blocking = True
        return blocking

    def _drifted_units_locked(self, plan: dict,
                              scanned_versions: Dict[str, int]) -> List[str]:
        """Re-check every scanned unit's generation before committing.

        A version bump means the unit was repaired/quarantined between the
        scan and the commit; an eviction removes it.  Either way the batch
        is stale and must be discarded and replanned from the same cursor.
        """
        out: List[str] = []
        for seg_id, version in scanned_versions.items():
            meta = self.s._seg_by_id.get(seg_id)
            if meta is None:
                if self.s.gc.tombstone(seg_id) is not None:
                    out.append(seg_id + "(evicted)")
                else:
                    out.append(seg_id + "(vanished)")
                continue
            if meta.get("version", 1) != version:
                out.append(seg_id)
            elif meta["status"] != "sealed":
                out.append(seg_id + f"({meta['status']})")
        return out

    def _commit_batch_locked(self, p: dict, candidates: List[dict],
                             cursor_after: int, source_offsets: List[int]) -> None:
        """Journal intent -> flush derived entries -> publish cursor.

        Caller HOLDS the store lock and already verified status/versions.
        """
        batch_no = len(p["batches"]) + 1
        batch = {
            "no": batch_no,
            "from": p["cursor"],
            "to": cursor_after,
            "source_offsets": list(source_offsets),
            "published": False,
            "records": [],
            "results": [],
        }
        records: List[dict] = []
        now = fmt_ts(utcnow())
        for cand in candidates:
            rec = cand["source"]
            off = cand["source_offset"]
            terminal = p["target_prefix"] + cand["device"]
            seq = p["seq_counters"].get(terminal, 0) + 1
            p["seq_counters"][terminal] = seq
            lineage_id = self._lineage_id(p, off)
            event_id = self._derived_event_id(p, off)
            # Deterministic generation time: the source entry's arrival time
            # keeps the derived identity stable across re-runs/recovery.
            generated_at = rec["ingest_ts"]
            flags = new_flags()
            src_flags = rec.get("flags", {})
            flags["late"] = bool(src_flags.get("late"))
            flags["clock_rollback"] = bool(src_flags.get("clock_rollback"))
            derived = {
                "ingest_ts": generated_at,
                "event": {
                    "device_id": terminal,
                    "event_id": event_id,
                    "seq": seq,
                    "device_ts": rec["event"]["device_ts"],
                    "payload": cand["payload"],
                },
                "flags": flags,
                "provenance": {
                    "kind": "derived",
                    "pipeline_id": p["id"],
                    "lineage_id": lineage_id,
                    "recipe_code": p["recipe_code"],
                    "view_token": p["view_token"],
                    "source_offset": off,
                    "source_event_id": rec["event"]["event_id"],
                    "source_device_id": cand["device"],
                    "generated_at": generated_at,
                    "batch_no": batch_no,
                },
            }
            records.append(derived)
            batch["records"].append(derived)

        # Phase A: durable batch intent (with assigned terminal seqs) BEFORE
        # any derived byte is flushed.
        p["batches"].append(batch)
        p["stage"] = f"batch:{batch_no}:planned"
        self._add_stage_locked(p, f"batch:{batch_no}:planned")
        p["updated_at"] = now
        self._persist_locked()

        self._flush_and_publish_locked(p, batch)

    def _flush_and_publish_locked(self, p: dict, batch: dict) -> None:
        # Phase B: WAL flush + fsync of the derived entries.
        results = self.s._append_projection_records(batch["records"])
        batch["results"] = [{
            "event_id": r["event_id"], "offset": r["offset"],
            "status": r["status"],
        } for r in results]
        p["stage"] = f"batch:{batch['no']}:flushed"
        self._add_stage_locked(p, f"batch:{batch['no']}:flushed")
        p["updated_at"] = fmt_ts(utcnow())
        self._persist_locked()
        self._fire_hook(p["id"], "batch_flushed")

        # Phase C: publish the cursor and derived index exactly once.  This
        # is the normal (same-process) path, so every record is freshly
        # stored; recovery uses _publish_recovered_batch_locked, where
        # already-durable records must not be indexed twice.
        if not batch["published"]:
            for rec, r in zip(batch["records"], results):
                prov = rec["provenance"]
                p["entries"].append({
                    "lineage_id": prov["lineage_id"],
                    "derived_event_id": rec["event"]["event_id"],
                    "source_offset": prov["source_offset"],
                    "terminal": rec["event"]["device_id"],
                    "seq": rec["event"]["seq"],
                    "offset": r["offset"],
                    "recipe_code": p["recipe_code"],
                    "generated_at": prov["generated_at"],
                    "batch_no": batch["no"],
                })
            p["entries"].sort(key=lambda e: (e["offset"], e["terminal"]))
            p["derived_count"] += sum(
                1 for r in results if r["status"] == "stored")
            p["cursor"] = batch["to"]
            batch["published"] = True
            batch["records"] = []  # payloads no longer needed once published
        p["stage"] = f"batch:{batch['no']}:published"
        self._add_stage_locked(p, f"batch:{batch['no']}:published")
        p["updated_at"] = fmt_ts(utcnow())
        self._persist_locked()
        self._fire_hook(p["id"], "cursor_published")

    def _publish_recovered_batch_locked(self, p: dict, batch: dict) -> None:
        """Finish an interrupted batch after a restart (idempotent).

        The journaled records are re-flushed: deterministic derived event
        ids dedup inside the target terminal, so the flush can never create
        double entries.  Records already present (the crash landed after
        the fsync) are looked up by their original offset and only the
        index/cursor are reconciled -- the derived count counts physical
        entries exactly once.
        """
        if not batch.get("records"):
            # Intent lost its payloads without a publish marker: the batch
            # cannot be reconstructed verbatim; drop it and let the worker
            # rescan from the unchanged cursor (no entries were published).
            p["batches"].remove(batch)
            self._persist_locked()
            return
        results = self.s._append_projection_records(batch["records"])
        batch["results"] = [{
            "event_id": r["event_id"], "offset": r["offset"],
            "status": r["status"],
        } for r in results]
        if not batch["published"]:
            # Every journaled record corresponds to exactly one physical
            # target entry; the "duplicate" statuses are the pre-crash
            # flush returning its existing offsets, not second entries.
            for rec, r in zip(batch["records"], results):
                prov = rec["provenance"]
                p["entries"].append({
                    "lineage_id": prov["lineage_id"],
                    "derived_event_id": rec["event"]["event_id"],
                    "source_offset": prov["source_offset"],
                    "terminal": rec["event"]["device_id"],
                    "seq": rec["event"]["seq"],
                    "offset": r["offset"],
                    "recipe_code": p["recipe_code"],
                    "generated_at": prov["generated_at"],
                    "batch_no": batch["no"],
                })
            p["entries"].sort(key=lambda e: (e["offset"], e["terminal"]))
            p["derived_count"] += len(batch["records"])
            p["cursor"] = batch["to"]
            batch["published"] = True
            batch["records"] = []
        p["stage"] = f"batch:{batch['no']}:recovered"
        self._add_stage_locked(p, f"batch:{batch['no']}:recovered")
        p["updated_at"] = fmt_ts(utcnow())
        self._persist_locked()
        log.info("pipeline %s recovered batch %d at cursor %d",
                 p["id"], batch["no"], p["cursor"])

    # ------------------------------------------------------------------ #
    # deterministic identities                                            #
    # ------------------------------------------------------------------ #

    def _lineage_id(self, p: dict, source_offset: int) -> str:
        """Same recipe code + recipe body + view + source offset always
        yields the same lineage entry number -- independent of which
        pipeline executes it (the recipe's identity for that source)."""
        h = _sha1("|".join([
            "lin", p["recipe_code"], p["recipe_fingerprint"],
            p["view_token"], str(source_offset)]))
        return f"lin-{h[:20]}"

    def _derived_event_id(self, p: dict, source_offset: int) -> str:
        """Stable per (pipeline/target prefix, source offset); dedup key in
        the target terminal, so a re-flush can never double-write.

        Note the target prefix is part of the key: two pipelines running
        the same recipe into the same prefix is a declaration collision
        caught upstream, while distinct prefixes own distinct entries."""
        h = _sha1("|".join([
            "drv", p["id"], p["target_prefix"], p["recipe_code"],
            p["recipe_fingerprint"], p["view_token"], str(source_offset)]))
        return f"drv-{h[:24]}"

    # ------------------------------------------------------------------ #
    # stages / hooks / journals                                           #
    # ------------------------------------------------------------------ #

    def _add_stage_locked(self, p: dict, stage: str) -> None:
        p["stages"].append({
            "seq": len(p["stages"]),
            "stage": stage,
            "at": fmt_ts(utcnow()),
        })
        if len(p["stages"]) > 500:
            del p["stages"][:-500]

    def _fire_hook(self, pid: str, phase: str) -> None:
        hook = self._phase_hook
        if hook is None:
            return
        try:
            hook(pid, phase)
        except Exception:
            log.exception("projection phase hook raised")

    def recover(self) -> None:
        """Load the durable journal (called early in store.open())."""
        if not os.path.exists(self._path):
            return
        try:
            data = load_json(self._path)
        except Exception as exc:
            log.error("cannot read projections journal (%s); starting empty",
                      exc)
            return
        for v in data.get("views", []):
            self._views[v["token"]] = v
        for p in data.get("pipelines", []):
            self._pipelines[p["id"]] = p

    def _persist_locked(self) -> None:
        atomic_write_json(self._path, {
            "views": sorted(self._views.values(),
                            key=lambda v: v["created_at"]),
            "pipelines": [self._pipelines[k]
                          for k in sorted(self._pipelines)],
        })

    def _stats_locked(self) -> dict:
        return {
            "views": len(self._views),
            "total": len(self._pipelines),
            "queued": sum(1 for p in self._pipelines.values()
                          if p["status"] in ("queued", "running")),
            "paused": sum(1 for p in self._pipelines.values()
                          if p["status"] == "paused"),
            "blocked": sum(1 for p in self._pipelines.values()
                           if p["status"] == "blocked"),
            "succeeded": sum(1 for p in self._pipelines.values()
                             if p["status"] == "succeeded"),
            "aborted": sum(1 for p in self._pipelines.values()
                           if p["status"] == "aborted"),
            "derived_total": sum(p["derived_count"]
                                 for p in self._pipelines.values()),
            "pinned_segments": len(self.dependency_segments_locked()),
        }
