"""Acceptance tests for auditable derived-lineage pipelines (/v3/projections).

Covers the nine acceptance scenarios:
  1. a multi-terminal view projected through a field recipe yields queryable
     target streams whose lineage numbers trace back to source offsets;
  2. concurrent opens of the same pipeline id leave exactly one pipeline
     and exactly one set of derived entries (cross-process idempotency);
  3. re-posting the same id with changed declaration fields -> 409 and the
     original manifest keeps running untouched;
  4. content arriving after the static view never enters the derived stream;
  5. a quarantined source segment surfaces a gap, a precise resume cursor and
     the affected terminals; after repair the pipeline resumes at that exact
     cursor (the gap is never hidden or skipped);
  6. forced aborts at all four persistence seams (manifest persistence,
     single-batch entry flush, cursor publication, terminal marking)
     reconcile on restart: derived count, cursor and derived index agree;
  7. dependency segments cannot be space-reclaimed while a pipeline is
     unfinished; completion/revoke alone moves them into GC candidacy;
  8. pause/resume/revoke fenced by a monotonic generation run concurrently
     with ingest, terminal lookups, view creation, repair and GC without
     cross-writing stages;
  9. the same recipe re-run keeps the same lineage-entry -> identity mapping,
     and the static horizon never extends with later arrivals.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch import gc as gcmod
from eventarch import projections as projmod
from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore


def make_cfg(tmp, **kw):
    base = dict(
        data_dir=tmp, segment_max_records=5, segment_max_age_sec=3600,
        late_threshold_sec=900, wal_retain_segments=8,
        janitor_interval_sec=0.05,
        repair_workers=2, repair_retry_backoff_sec=0.01,
        gc_workers=1,
        projection_workers=2, projection_batch_size=4,
        projection_recheck_sec=0.03,
    )
    base.update(kw)
    return Config(**base)


def ev(device, seq, event_id=None, ts=None):
    return {
        "device_id": device,
        "event_id": event_id or f"{device}-{seq}",
        "seq": seq,
        "device_ts": fmt_ts(ts or utcnow()),
        "payload": {"v": seq, "x": f"{device}:{seq}"},
    }


def open_store(tmp, **kw):
    s = ArchiveStore(make_cfg(tmp, **kw))
    s.open()
    return s


RECIPE = {"val": "payload.v", "x": "payload.x", "src": "device_id"}


def wait_status(s, pid, targets, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = s.projections.get(pid)
        if v["status"] in targets:
            return v
        time.sleep(0.01)
    raise AssertionError(f"{pid} never reached {targets}: "
                         f"{s.projections.get(pid)}")


def start_pipe(s, pid, frz, sources=("dA", "dB"), start=0, end=None,
               recipe=RECIPE, code="r1", prefix="out/"):
    end = frz["end_offset"] if end is None else end
    v, created = s.projections.create(
        pid, frz["id"], list(sources), start, end, recipe, code, prefix)
    return v, created


class ProjectionTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def prep(self, n_per=6, devices=("dA", "dB"), store=None):
        s = store or open_store(self.tmp)
        s.ingest([ev(d, i) for d in devices for i in range(n_per)])
        frz = s.freeze()
        return s, frz


class Scenario1LineageTest(ProjectionTestBase):
    def test_01_multi_terminal_recipe_queryable_lineage(self):
        s, frz = self.prep()
        v, created = start_pipe(s, "p1", frz)
        self.assertTrue(created)
        v = wait_status(s, "p1", ("completed",))
        self.assertEqual(v["status"], "completed")
        self.assertEqual(v["cursor"], frz["end_offset"])
        self.assertFalse(v["gaps"])

        # constant response shape
        for key in ("pipeline_id", "status", "stage", "gen", "stages",
                    "declaration", "view", "derived", "gaps", "cursor",
                    "depends_on", "conflicts", "error", "created_at",
                    "updated_at"):
            self.assertIn(key, v)
        self.assertEqual(v["view"]["end_offset"], frz["end_offset"])
        self.assertEqual(v["view"]["segments"], frz["segments"])

        # every source offset in range appears exactly once, in order
        los = [d["lineage_no"] for d in v["derived"]]
        self.assertEqual(los, list(range(frz["end_offset"])))
        offsets = [d["offset"] for d in v["derived"]]
        self.assertEqual(len(set(offsets)), len(offsets))
        targets = {d["target"] for d in v["derived"]}
        self.assertEqual(targets, {"out/dA", "out/dB"})

        # ordinary stream fetch shows the new entries, business-ordered,
        # carrying recipe output + lineage
        for d, base in (("dA", 0), ("dB", 6)):
            got = s.device_events(f"out/{d}", limit=1000)
            self.assertEqual([e["event"]["seq"] for e in got["events"]],
                             list(range(6)))
            rec0 = got["events"][0]
            self.assertEqual(rec0["event"]["payload"],
                             {"val": 0, "x": f"{d}:0", "src": d})
            lin = rec0["derived"]
            self.assertEqual(lin["pipeline_id"], "p1")
            self.assertEqual(lin["recipe"], "r1")
            self.assertEqual(lin["lineage_no"], base)
            self.assertEqual(lin["source_offset"], base)
            self.assertTrue(lin["generated_at"])

        # lineage maps straight back to the original record
        first = v["derived"][0]
        src = next(e for e in s.replay(frz["id"], limit=1000)["events"]
                   if e["offset"] == first["lineage_no"])
        self.assertEqual(src["event"]["device_id"], "dA")
        s.close()


class Scenario2IdempotencyTest(ProjectionTestBase):
    def test_02a_same_id_concurrent_open_single_pipeline(self):
        s, frz = self.prep()
        outcomes = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def open_pipe():
            barrier.wait()
            try:
                _, created = start_pipe(s, "same-pipe", frz)
                with lock:
                    outcomes.append(created)
            except Exception as exc:  # noqa: BLE001
                with lock:
                    outcomes.append(exc)

        threads = [threading.Thread(target=open_pipe) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count(True), 1, outcomes)
        self.assertEqual(outcomes.count(False), 7, outcomes)

        wait_status(s, "same-pipe", ("completed",))
        self.assertEqual(len(s.projections.list_pipelines()["pipelines"]), 1)
        # exactly one derived entry per source offset despite 8 opens
        v = s.projections.get("same-pipe")
        self.assertEqual(len(v["derived"]), frz["end_offset"])
        s.close()

    def test_02b_same_id_across_process_restart(self):
        s, frz = self.prep()
        v, _ = start_pipe(s, "durable-pipe", frz)
        self.assertEqual(v["status"], "queued")
        wait_status(s, "durable-pipe", ("completed",))
        s.close()

        s2 = open_store(self.tmp)
        frz2 = next(f for f in s2.list_freezes()
                    if f["id"] == frz["id"])
        # identical declaration after restart -> same object, 200, same id
        v2, created = start_pipe(s2, "durable-pipe", frz2)
        self.assertFalse(created)
        self.assertEqual(v2["cursor"], frz["end_offset"])
        self.assertEqual(v2["status"], "completed")
        # re-run produced no NEW entries (deterministic identity deduped)
        self.assertEqual(len(v2["derived"]), frz["end_offset"])
        s2.close()


class Scenario3ConflictTest(ProjectionTestBase):
    def test_03_changed_declaration_409_original_unchanged(self):
        s, frz = self.prep()
        v, _ = start_pipe(s, "p3", frz)
        original_cursor = v["cursor"]
        original_sources = list(v["declaration"]["sources"])

        def expect_conflict(**overrides):
            kwargs = dict(view_token=frz["id"], sources=["dA", "dB"],
                          start_cursor=0, end_cursor=frz["end_offset"],
                          recipe=RECIPE,
                          recipe_code="r1", target_prefix="out/")
            kwargs.update(overrides)
            with self.assertRaises(projmod.ProjectionConflict):
                s.projections.create("p3", **kwargs)

        expect_conflict(sources=["dA"])                        # whitelist
        expect_conflict(view_token=self._other_view(s, frz))  # view
        expect_conflict(start_cursor=1)                        # cursor
        expect_conflict(end_cursor=frz["end_offset"] - 1)      # cursor
        expect_conflict(recipe={"val": "payload.x"})           # recipe
        expect_conflict(recipe_code="r2")                      # recipe code
        expect_conflict(target_prefix="other/")                # prefix

        # the original pipeline is untouched and still completes
        v2 = wait_status(s, "p3", ("completed",))
        self.assertEqual(v2["declaration"]["sources"], original_sources)
        self.assertEqual(len(v2["derived"]), frz["end_offset"])
        self.assertGreaterEqual(v2["cursor"], original_cursor)
        # only one pipeline exists
        self.assertEqual(len(s.projections.list_pipelines()["pipelines"]), 1)
        s.close()

    @staticmethod
    def _other_view(s, frz):
        other = s.freeze()
        return other["id"]

    def test_03b_same_targets_different_recipe_bilateral_409(self):
        s, frz = self.prep()
        start_pipe(s, "owner", frz, code="recipe-A", prefix="t/")
        wait_status(s, "owner", ("completed",))
        with self.assertRaises(projmod.ProjectionConflict) as ctx:
            start_pipe(s, "intruder", frz, code="recipe-B", prefix="t/")
        self.assertTrue(any("occupied" in r for r in ctx.exception.reasons))
        # holder is annotated (bilateral)
        holder = s.projections.get("owner")
        self.assertTrue(any(c["with"] == "intruder" for c in holder["conflicts"]))
        # same recipe on the same terminals -> allowed (idempotent lineage)
        _, created = start_pipe(s, "twin", frz, code="recipe-A", prefix="t/")
        self.assertTrue(created)
        wait_status(s, "twin", ("completed",))
        s.close()


class Scenario4StaticHorizonTest(ProjectionTestBase):
    def test_04_later_arrivals_never_enter_pipeline(self):
        s, frz = self.prep(n_per=4, devices=("dA",))  # end_offset == 4
        # ingest AFTER freezing; lands in later segments / open tail
        s.ingest([ev("dA", 100 + i, event_id=f"late-{i}") for i in range(5)])
        v, _ = start_pipe(s, "p4", frz, sources=("dA",),
                          end=frz["end_offset"])
        v = wait_status(s, "p4", ("completed",))
        self.assertEqual(v["cursor"], frz["end_offset"])
        self.assertEqual([d["lineage_no"] for d in v["derived"]], [0, 1, 2, 3])
        # even more arrivals while/after the pipeline runs
        s.ingest([ev("dA", 200 + i, event_id=f"later-{i}") for i in range(3)])
        time.sleep(0.15)  # let any (incorrect) work surface
        v = s.projections.get("p4")
        self.assertEqual(v["status"], "completed")
        self.assertEqual(len(v["derived"]), 4)
        self.assertEqual(max(d["lineage_no"] for d in v["derived"]), 3)
        # target stream contains only horizon-derived entries
        got = s.device_events("out/dA", limit=1000)
        self.assertEqual(len(got["events"]), 4)
        self.assertTrue(all(e["derived"]["source_offset"] < frz["end_offset"]
                            for e in got["events"]))
        s.close()


class Scenario5GapTest(ProjectionTestBase):
    def test_05_quarantine_gap_then_resume_at_cursor(self):
        s = open_store(self.tmp)
        s.ingest([ev("dA", i) for i in range(12)])
        frz = s.freeze()
        victim = "seg-00000000000000000005"
        s.quarantine(victim, "simulated rot")
        v, _ = start_pipe(s, "p5", frz, sources=("dA",))
        # all three view segments remain declared dependencies
        self.assertEqual(set(v["depends_on"]), set(frz["segments"]))

        v = wait_status(s, "p5", ("blocked",))
        self.assertEqual(v["stage"], f"blocked:{victim}")
        self.assertEqual(len(v["gaps"]), 1)
        gap = v["gaps"][0]
        self.assertEqual(gap["segment"], victim)
        self.assertEqual(gap["resume_offset"], 5)   # precise resume cursor
        self.assertEqual(gap["first_offset"], 5)
        self.assertEqual(gap["last_offset"], 9)
        self.assertEqual(gap["terminals"], ["out/dA"])
        # nothing past the gap was derived
        self.assertTrue(all(d["lineage_no"] < 5 for d in v["derived"]))

        # GC cannot reclaim any pinned dependency while blocked
        self.assertIn(victim, s.projections.dependency_ids_locked())
        plan = s.gc.create_plan(10_000)
        self.assertEqual(plan["items"], [])

        # repair -> the pipeline resumes exactly at offset 5
        job, _ = s.start_repair(victim)
        s.wait_repair(job["id"], timeout=10)
        v = wait_status(s, "p5", ("completed",), timeout=10)
        self.assertEqual(v["cursor"], frz["end_offset"])
        self.assertEqual([d["lineage_no"] for d in v["derived"]],
                         list(range(12)))
        self.assertEqual(v["gaps"], [])
        got = s.device_events("out/dA", limit=1000)
        self.assertEqual([e["event"]["seq"] for e in got["events"]],
                         list(range(12)))
        s.close()

    def test_05b_gap_during_scan_also_blocks(self):
        # Healthy at creation; corruption detected by the scan thread.
        # Small batches: the worker is parked after the first batch
        # (offsets 0..3, all in seg-0) so seg-5 has not been read yet.
        s = open_store(self.tmp, projection_batch_size=4)
        s.ingest([ev("dA", i) for i in range(12)])
        frz = s.freeze()
        entered = threading.Event()
        release = threading.Event()

        def hook(view, phase):
            # Park right before the FIRST batch flush: no source segment
            # has been derived from yet, so seg-5 (offsets 5..9) is still
            # unread when the corruption lands.
            if phase == "entries_flushed" and view["cursor"] == 0 \
                    and not entered.is_set():
                entered.set()
                release.wait(5)

        s.projections._phase_hook = hook
        start_pipe(s, "p5b", frz, sources=("dA",))
        self.assertTrue(entered.wait(5))
        # corrupt seg-5 while the pipeline is parked past its first batch
        path = os.path.join(self.tmp, "segments",
                            "seg-00000000000000000005", "events.log")
        with open(path, "r+b") as fh:
            fh.seek(10)
            b = fh.read(1)
            fh.seek(10)
            fh.write(bytes([b[0] ^ 0xFF]))
        release.set()
        v = wait_status(s, "p5b", ("blocked", "completed"), timeout=10)
        self.assertEqual(v["status"], "blocked")
        self.assertTrue(v["gaps"])
        self.assertEqual(v["gaps"][0]["resume_offset"], 5)
        s.close()


class Scenario6CrashTest(unittest.TestCase):
    PHASES = ("manifest_persisted", "entries_flushed",
              "cursor_published", "terminal_marked")

    def setUp(self):
        self.driver = os.path.join(os.path.dirname(__file__),
                                   "projections_crash_driver.py")

    def test_06_force_abort_at_each_seam_reconciles(self):
        for phase in self.PHASES:
            with self.subTest(phase=phase):
                self._crash_case(phase)

    def _crash_case(self, phase):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        ready = os.path.join(tmp, ".ready")
        proc = subprocess.Popen(
            [sys.executable, self.driver, tmp, phase, ready],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if os.path.exists(ready):
                break
            if proc.poll() is not None:
                raise AssertionError("driver exited early:\n"
                                     + proc.stderr.read().decode()[-2000:])
            time.sleep(0.02)
        else:
            proc.kill()
            raise AssertionError("crash point never reached")
        proc.wait(timeout=10)
        self.assertEqual(proc.returncode, 17,
                         proc.stderr.read().decode()[-1000:])
        proc.stdout.close()
        proc.stderr.close()

        if phase == "manifest_persisted":
            # Nothing may have landed: the journal write is the first
            # durable action of the pipeline.
            self.assertFalse(os.path.exists(
                os.path.join(tmp, "state", "projections.json")))

        # Restart against the same data dir and resume the SAME pipeline.
        cfg = Config(data_dir=tmp, segment_max_records=5,
                     segment_max_age_sec=3600, janitor_interval_sec=3600,
                     projection_batch_size=4, projection_recheck_sec=0.02,
                     projection_workers=1)
        s = ArchiveStore(cfg)
        s.open()
        try:
            if phase == "manifest_persisted":
                # First durable action never landed: re-create it fresh.
                with open(os.path.join(tmp, ".prj_prepared")) as fh:
                    view_token = fh.read().strip()
                s.projections.create("crashpipe", view_token, ["dA", "dB"],
                                     0, 12, RECIPE, "crash-recipe", "out/")
            v = wait_status(s, "crashpipe", ("completed",), timeout=15)
            # derived count / cursor / index all agree
            self.assertEqual(v["cursor"], 12)
            self.assertEqual(len(v["derived"]), 12)
            los = [d["lineage_no"] for d in v["derived"]]
            self.assertEqual(los, list(range(12)))
            offs = [d["offset"] for d in v["derived"]]
            self.assertEqual(len(set(offs)), 12)
            # each target carries exactly six entries, in seq order
            for d in ("dA", "dB"):
                got = s.device_events(f"out/{d}", limit=100)
                self.assertEqual([e["event"]["seq"] for e in got["events"]],
                                 list(range(6)))
            # index entries must resolve to live offsets of the same event id
            for e in v["derived"]:
                dev = s._devices[e["target"]]
                self.assertEqual(dev.event_ids[e["event_id"]], e["offset"])
            # one more clean restart: still exactly one pipeline/set
            s.close()
            s = ArchiveStore(cfg)
            s.open()
            v2 = wait_status(s, "crashpipe", ("completed",), timeout=15)
            self.assertEqual(len(v2["derived"]), 12)
            self.assertEqual(len(s.projections.list_pipelines()["pipelines"]), 1)
        finally:
            s.close()


class Scenario7GCPinTest(ProjectionTestBase):
    def test_07_dependencies_pinned_until_complete_or_revoke(self):
        s, frz = self.prep(n_per=12, devices=("dA",))  # segs 0,5 (sealed)
        # park the pipeline right before its first batch flush
        entered = threading.Event()
        release = threading.Event()

        def hook(view, phase):
            if phase == "entries_flushed" and not entered.is_set():
                entered.set()
                release.wait(5)

        s.projections._phase_hook = hook
        start_pipe(s, "p7", frz, sources=("dA",), code="gc", prefix="g/")
        self.assertTrue(entered.wait(5))
        pins = s.projections.dependency_ids_locked()
        for sid in frz["segments"]:
            self.assertIn(sid, pins)
        # and the projection pin set reaches GC eligibility directly
        self.assertTrue(set(frz["segments"]) <= pins)
        release.set()
        wait_status(s, "p7", ("completed",))
        # completion releases the projection pin (the freeze snapshot may
        # still pin the same segments, but the pipeline no longer does)
        self.assertEqual(s.projections.dependency_ids_locked(), set())

        # a paused/unfinished pipeline re-pins until revoke
        s2, frz2 = self.prep(store=s) if False else (s, frz)
        v, _ = start_pipe(s, "p7b", frz, sources=("dA",), code="gc2",
                          prefix="h/")
        g = v["gen"]
        wait_status(s, "p7b", ("completed", "running", "queued"))
        s.projections.control("p7b", "pause", gen=g)
        self.assertTrue(s.projections.dependency_ids_locked())
        self.assertFalse(
            {i["id"] for i in s.gc.create_plan(10_000)["items"]}
            & s.projections.dependency_ids_locked())
        pv = s.projections.get("p7b")
        s.projections.control("p7b", "revoke", gen=pv["gen"])
        self.assertEqual(s.projections.dependency_ids_locked(), set())
        s.close()


class Scenario8ControlConcurrencyTest(ProjectionTestBase):
    def test_08_gen_fenced_controls_and_parallel_traffic(self):
        s, frz = self.prep(n_per=40, devices=("dA", "dB"))
        park = threading.Event()
        release = threading.Event()

        def hook(view, phase):
            if phase == "cursor_published":
                park.set()
                release.wait(5)

        s.projections._phase_hook = hook
        v, _ = start_pipe(s, "p8", frz)
        self.assertTrue(park.wait(5))
        g0 = v["gen"]

        # stale token can never rewrite a newer stage
        with self.assertRaises(projmod.ProjectionConflict):
            s.projections.control("p8", "pause", gen=g0 + 50)
        pv = s.projections.control("p8", "pause", gen=g0)
        self.assertEqual(pv["gen"], g0 + 1)
        self.assertEqual(pv["status"], "paused")
        # the worker holding the old generation must not publish another
        # stage when released
        cursor_paused = s.projections.get("p8")["cursor"]
        release.set()
        time.sleep(0.15)
        self.assertEqual(s.projections.get("p8")["cursor"], cursor_paused)
        self.assertEqual(s.projections.get("p8")["status"], "paused")
        # old-gen resume rejected
        with self.assertRaises(projmod.ProjectionConflict):
            s.projections.control("p8", "resume", gen=g0)
        pv = s.projections.control("p8", "resume", gen=g0 + 1)
        self.assertEqual(pv["gen"], g0 + 2)
        wait_status(s, "p8", ("completed",), timeout=15)
        final = s.projections.get("p8")
        self.assertEqual(len(final["derived"]), frz["end_offset"])

        # revoke fenced as well
        v, _ = start_pipe(s, "p8b", frz, code="rev", prefix="rv/")
        g = v["gen"]
        with self.assertRaises(projmod.ProjectionConflict):
            s.projections.control("p8b", "revoke", gen=g - 1)
        rv = s.projections.control("p8b", "revoke", gen=g)
        self.assertEqual(rv["status"], "revoked")
        # revoked answer is frozen
        self.assertIs(rv, s.projections.get("p8b"))
        s.close()

    def test_08b_controls_while_other_traffic_runs(self):
        s = open_store(self.tmp, projection_batch_size=2,
                       projection_recheck_sec=0.01)
        s.ingest([ev("dA", i) for i in range(20)])
        frz = s.freeze()
        stop = threading.Event()
        errors = []

        def traffic():
            i = 1000
            while not stop.is_set():
                try:
                    s.ingest([ev("dA", i, event_id=f"bg-{i}")])
                    s.device_events("dA", limit=5)
                    s.freeze()
                    s.replay(frz["id"], limit=10)
                    s.gc.create_plan(10_000)
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
                i += 1

        th = threading.Thread(target=traffic)
        th.start()
        try:
            v, _ = start_pipe(s, "p8c", frz, sources=("dA",),
                              code="par", prefix="par/")
            gen = v["gen"]
            for _ in range(6):
                cur = s.projections.get("p8c")
                if cur["status"] in ("completed", "revoked"):
                    break
                if cur["status"] != "paused":
                    cur = s.projections.control("p8c", "pause", gen=cur["gen"])
                    gen = cur["gen"]
                    time.sleep(0.005)
                    cur = s.projections.control("p8c", "resume", gen=cur["gen"])
                    gen = cur["gen"]
                time.sleep(0.005)
            cur = s.projections.get("p8c")
            if cur["status"] == "paused":
                s.projections.control("p8c", "resume", gen=cur["gen"])
            wait_status(s, "p8c", ("completed",), timeout=20)
        finally:
            stop.set()
            th.join()
        self.assertEqual(errors, [])
        final = s.projections.get("p8c")
        # each source offset still appears exactly once despite churn
        self.assertEqual(sorted(d["lineage_no"] for d in final["derived"]),
                         list(range(frz["end_offset"])))
        s.close()


class Scenario9DeterminismTest(ProjectionTestBase):
    def test_09_same_recipe_rerun_same_mapping_horizon_sealed(self):
        s = open_store(self.tmp)
        s.ingest([ev("dA", i) for i in range(8)])
        frz = s.freeze()
        # post-view arrivals
        s.ingest([ev("dA", 50 + i, event_id=f"post-{i}") for i in range(4)])

        v, _ = start_pipe(s, "run-1", frz, sources=("dA",), code="det",
                          prefix="det/")
        v = wait_status(s, "run-1", ("completed",))
        mapping = [(d["lineage_no"], d["event_id"], d["offset"], d["target"])
                   for d in v["derived"]]
        self.assertEqual([m[0] for m in mapping], list(range(8)))

        # same recipe + same terminals under a second pipeline id ->
        # identical lineage mapping, resolved to identical offsets
        v2, created = start_pipe(s, "run-2", frz, sources=("dA",), code="det",
                                 prefix="det/")
        self.assertTrue(created)
        v2 = wait_status(s, "run-2", ("completed",))
        mapping2 = [(d["lineage_no"], d["event_id"], d["offset"], d["target"])
                    for d in v2["derived"]]
        self.assertEqual(mapping, mapping2)
        # no extra entries landed in the target stream (dedup)
        got = s.device_events("det/dA", limit=1000)
        self.assertEqual(len(got["events"]), 8)
        # the static horizon never grew
        self.assertEqual(v2["view"]["end_offset"], frz["end_offset"])
        self.assertTrue(all(d["lineage_no"] < frz["end_offset"]
                            for d in v2["derived"]))

        # restart persistence of both pipelines keeps the mapping
        s.close()
        s2 = open_store(self.tmp)
        for pid in ("run-1", "run-2"):
            w = s2.projections.get(pid)
            self.assertEqual(w["status"], "completed")
            self.assertEqual([(d["lineage_no"], d["event_id"], d["offset"])
                              for d in w["derived"]],
                             [(m[0], m[1], m[2]) for m in mapping])
        s2.close()


if __name__ == "__main__":
    unittest.main()
