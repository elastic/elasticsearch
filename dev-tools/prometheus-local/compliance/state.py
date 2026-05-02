#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

"""File-based mutual exclusion for promcheck runs. The lock file lives
under the per-user state directory and contains the active run's `run_id`,
pid, and start time. Two concurrent promcheck invocations would write to
the same Prometheus TSDB and ES data streams at the same time, so we
refuse to start when an alive holder is detected."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path


_STATE_FILENAME = "run.json"


class StateLockError(RuntimeError):
    """Another promcheck run holds the lock."""


def _path(state_dir: Path) -> Path:
    return state_dir / _STATE_FILENAME


def _pid_alive(pid: int) -> bool:
    # Signal 0 is the standard "is this pid alive?" probe: it does no work
    # but raises ProcessLookupError if the pid doesn't exist.
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The pid exists, just owned by someone else.
        return True
    return True


def acquire(state_dir: Path, run_id: str) -> Path:
    """Take the lock or raise `StateLockError`. A leftover lock from a
    crashed previous run (file present, pid dead) is silently overwritten
    so the next clean run isn't blocked by it."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = _path(state_dir)

    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            existing = None

        if existing and _pid_alive(int(existing.get("pid", 0))):
            raise StateLockError(
                f"another promcheck is running: pid={existing.get('pid')}, "
                f"run_id={existing.get('run_id')!r} (lock at {path})"
            )

    payload = {"run_id": run_id, "pid": os.getpid(), "started_at": time.time()}
    path.write_text(json.dumps(payload))
    return path


def release(state_dir: Path) -> None:
    """Remove the lock if it's still ours. Best-effort - if the file is
    already gone (manual cleanup, tmpfs reboot) we don't fight it."""
    _path(state_dir).unlink(missing_ok=True)
