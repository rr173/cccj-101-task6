"""Subprocess driver for projection crash-injection acceptance tests.

Usage:
  python3 tests/projections_crash_driver.py <data_dir> <crash_phase> <ready_file>

Prepares 12 raw events (segments of 5) and a static v3 view, then starts
pipeline "pj-1" scanning all sources in batches of 4.  A single worker is
forced to ``os._exit(17)`` at the requested seam:

  manifest_persisted  right after the pipeline listing landed on disk,
                      before any batch was planned/flushed
  batch_flushed       after the first batch's derived entries were WAL
                      fsync'd, before its cursor was published
  cursor_published    after the first batch's cursor + derived index were
                      published, before the next batch flushes
  terminal_pending    after all batches finished, right before the terminal
                      (succeeded) mark

The hook writes <ready_file> immediately before the hard exit.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore

PHASES = {"manifest_persisted", "batch_flushed", "cursor_published",
          "terminal_pending"}


def ev(i):
    dev = "d1" if i % 2 == 0 else "d2"
    return {"device_id": dev, "event_id": f"{dev}-{i}", "seq": i,
            "device_ts": fmt_ts(utcnow()), "payload": {"v": i}}


def main():
    data_dir, phase, ready_file = sys.argv[1], sys.argv[2], sys.argv[3]
    assert phase in PHASES
    cfg = Config(data_dir=data_dir, segment_max_records=5,
                 segment_max_age_sec=3600, janitor_interval_sec=3600,
                 projection_batch_size=4, projection_workers=1)
    s = ArchiveStore(cfg)
    s.open()
    s.ingest([ev(i) for i in range(12)])
    view = s.projections.create_view()

    state = {"fired": False}

    def hook(pid, ph):
        if pid != "pj-1" or ph != phase or state["fired"]:
            return
        state["fired"] = True
        with open(ready_file, "w") as fh:
            fh.write(ph)
            fh.flush()
            os.fsync(fh.fileno())
        os._exit(17)  # hard crash: no close(), no atexit

    # Install BEFORE creating the pipeline: the worker can reach
    # batch_flushed within the same instant the creation returns.
    s.projections._phase_hook = hook
    s.projections.create_pipeline(
        "pj-1", view["token"], ["d1", "d2"], 0, view["end_offset"],
        {"v": {"source": "payload.v"}}, "R1", "out-", batch_size=4)
    # Later seams fire asynchronously on the worker thread; keep the
    # process alive until the target seam is reached (the hook itself hard
    # exits before this returns).
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and not state["fired"]:
        time.sleep(0.005)
    s.close()


if __name__ == "__main__":
    main()
