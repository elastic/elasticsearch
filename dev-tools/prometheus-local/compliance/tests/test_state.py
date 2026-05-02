#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

import json
import os

import pytest

import state


def test_acquire_creates_lock_file_with_run_id_and_pid(tmp_path):
    state.acquire(tmp_path, "rabc")
    payload = json.loads((tmp_path / "run.json").read_text())
    assert payload["run_id"] == "rabc"
    assert payload["pid"] == os.getpid()


def test_release_removes_lock_file(tmp_path):
    state.acquire(tmp_path, "rabc")
    state.release(tmp_path)
    assert not (tmp_path / "run.json").exists()


def test_release_is_no_op_when_lock_already_gone(tmp_path):
    # Crashed-and-cleaned-up scenario: release shouldn't raise just
    # because the file vanished underneath us.
    state.release(tmp_path)


def test_acquire_steals_stale_lock_from_dead_pid(tmp_path):
    # Pid 1 (init) is alive; pick something that shouldn't exist. Use a
    # very large pid that's almost certainly not in use.
    (tmp_path / "run.json").write_text(
        json.dumps({"run_id": "old", "pid": 2_000_000_000, "started_at": 0})
    )
    state.acquire(tmp_path, "new")
    payload = json.loads((tmp_path / "run.json").read_text())
    assert payload["run_id"] == "new"


def test_acquire_refuses_when_holder_is_alive(tmp_path):
    # Our own pid is definitely alive, so this must look like a live holder.
    (tmp_path / "run.json").write_text(
        json.dumps({"run_id": "old", "pid": os.getpid(), "started_at": 0})
    )
    with pytest.raises(state.StateLockError, match="another promcheck"):
        state.acquire(tmp_path, "new")


def test_acquire_overwrites_corrupt_lock_file(tmp_path):
    # Truncated/garbage state file from a violently killed prior run.
    (tmp_path / "run.json").write_text("{not valid json")
    state.acquire(tmp_path, "new")
    assert json.loads((tmp_path / "run.json").read_text())["run_id"] == "new"


def test_acquire_creates_state_dir_if_missing(tmp_path):
    nested = tmp_path / "promcheck"
    assert not nested.exists()
    state.acquire(nested, "rabc")
    assert (nested / "run.json").exists()
