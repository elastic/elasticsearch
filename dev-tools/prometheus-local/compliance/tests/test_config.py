#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

import re
from pathlib import Path

from config import Config, default_state_dir, generate_run_id


def test_run_id_is_lowercase_alnum_and_distinct():
    a = generate_run_id()
    b = generate_run_id()
    # ES `validateNamespace` and Prometheus label values both accept this
    # alphabet; if we ever emit something fancier, both sides reject the
    # write at runtime.
    assert re.fullmatch(r"r[0-9a-f]+", a)
    assert a != b


def test_state_dir_under_xdg_runtime_dir_when_set(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    assert default_state_dir() == tmp_path / "promcheck"


def test_state_dir_falls_back_to_per_uid_tmp_when_xdg_unset(monkeypatch):
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    result = default_state_dir()
    assert result.name.startswith("promcheck-")
    assert isinstance(result, Path)


def test_default_config_es_data_stream_format():
    cfg = Config.default()
    # The ES Prometheus plugin lays out indices as
    # `metrics-{dataset}.prometheus-{namespace}`. If this format changes
    # upstream, every query/cleanup URL in proc.py breaks.
    assert cfg.es_data_stream == f"metrics-promcheck.prometheus-{cfg.run_id}"


def test_default_config_assigns_unique_run_id_per_instance():
    a = Config.default()
    b = Config.default()
    assert a.run_id != b.run_id
