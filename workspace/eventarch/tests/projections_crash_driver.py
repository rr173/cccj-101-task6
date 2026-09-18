"""Subprocess driver for projection crash-injection tests.

Usage:
  python3 tests/projections_crash_driver.py <data_dir> <crash_phase> <ready_file>

Ingests 12 events from dA/dB (segments [0..4],[5..9], open [10,11]),
freezes a view, then starts pipeline "crashpipe" with a batch size of 4.
The worker exits hard (os._exit) the first time it reaches crash_phase:

  manifest_persisted  immediately before the pipeline journal is first
                      written (no pipeline must survive)
  entries_flushed     immediately before one batch's derived entries are
                      flushed via the WAL (entries must not exist)
  cursor_published    after a batch's entries are durable but before the
                      cursor/derived index is published
  terminal_marked     once the whole range is scanned, immediately before
                      the terminal answer is marked

<ready_file> is written (fsync) right before the exit so the parent can
tell the crash point was genuinely reached.

The driver is re-runnable on the SAME data_dir: it skips ingestion/freezing
when its marker file exists, re-declaring the same idempotent pipeline
(identical declaration), which resumes from the last complete batch.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore

PHASES = {"manifest_persisted", "entries_flushed",
          "cursor_published", "terminal_marked"}
PIPE = "crashpipe"
RECIPE = {"val": "payload.v", "src": "device_id"}


def ev(d, i):
    return {"device_id": d, "event_id": f"{d}-{i}", "seq": i,
            "device_ts": fmt_ts(utcnow()), "payload": {"v": i, "x": f"{d}{i}"}}


def main():
    data_dir, phase, ready_file = sys.argv[1], sys.argv[2], sys.argv[3]
    assert phase in PHASES
    cfg = Config(data_dir=data_dir, segment_max_records=5,
                 segment_max_age_sec=3600, janitor_interval_sec=3600,
                 projection_batch_size=4, projection_recheck_sec=0.02,
                 projection_workers=1)
    s = ArchiveStore(cfg)
    s.open()

    marker = os.path.join(data_dir, ".prj_prepared")
    if not os.path.exists(marker):
        s.ingest([ev("dA", i) for i in range(6)]
                 + [ev("dB", i) for i in range(6)])
        frz = s.freeze()
        with open(marker, "w") as fh:
            fh.write(frz["id"])
            fh.flush()
            os.fsync(fh.fileno())
    with open(marker) as fh:
        view_token = fh.read().strip()

    crashed = {"done": False, "count": 0}

    def hook(view, ph):
        # The terminal hook fires once; the per-batch hooks fire on the
        # first eligible batch only so the crash point is unambiguous.
        if ph != phase or crashed["done"]:
            return
        if ph in ("entries_flushed", "cursor_published") and view["cursor"] != 0:
            return
        crashed["done"] = True
        with open(ready_file, "w") as fh:
            fh.write(ph)
            fh.flush()
            os.fsync(fh.fileno())
        os._exit(17)  # hard crash: no close(), no atexit

    s.projections._phase_hook = hook
    s.projections.create(PIPE, view_token, ["dA", "dB"], 0, 12,
                         RECIPE, "crash-recipe", "out/")

    deadline = time.time() + 10
    while time.time() < deadline:
        v = s.projections.get(PIPE)
        if v["status"] in ("completed", "revoked", "failed"):
            break
        time.sleep(0.01)
    # Allow the hook a moment if the terminal phase is the target.
    time.sleep(0.2)
    s.close()


if __name__ == "__main__":
    main()
