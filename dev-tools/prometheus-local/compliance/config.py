#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from __future__ import annotations

import os
import secrets
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from corpus import CorpusLoader
    from proc import Proc
    from util import HttpClient


# Used as the ES "dataset" segment in `metrics/{dataset}/{namespace}` write
# URLs and as the prefix for the `__promcheck_run__` label injected into
# Prometheus series. Constant across runs - the per-run uniqueness comes
# from `Config.run_id`.
PROMCHECK_DATASET = "promcheck"


def generate_run_id() -> str:
    """A 16-char lowercase alnum identifier that is safe to use as both an
    ES data-stream namespace and a Prometheus label value. Time prefix keeps
    it sortable; random suffix makes collisions effectively impossible."""
    return f"r{int(time.time()):x}{secrets.token_hex(3)}"


def default_state_dir() -> Path:
    """Per-user runtime directory. Prefers `$XDG_RUNTIME_DIR/promcheck`
    (tmpfs on Linux); on macOS / when XDG isn't set, falls back to a
    per-uid subdirectory under the system temp dir."""
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        return Path(xdg) / "promcheck"
    return Path(tempfile.gettempdir()) / f"promcheck-{os.getuid()}"


@dataclass
class Config:
    control_url: str
    test_url: str
    test_user: str | None
    test_password: str | None
    script_dir: Path
    compose_dir: Path
    es_root: Path
    http_timeout: int
    http_retries: int

    # Per-run identity. `run_id` is the namespace segment for ES (data
    # stream becomes `metrics-{dataset}.prometheus-{run_id}`) and the value
    # of the `__promcheck_run__` label injected into Prometheus series.
    dataset: str = PROMCHECK_DATASET
    run_id: str = field(default_factory=generate_run_id)
    state_dir: Path = field(default_factory=default_state_dir)

    # Wired up in `__post_init__`. Lazy imports avoid a config <-> proc cycle
    # at module load time.
    http: "HttpClient" = field(init=False)
    corpus_loader: "CorpusLoader" = field(init=False)
    control_proc: "Proc" = field(init=False)
    test_proc: "Proc" = field(init=False)

    def __post_init__(self) -> None:
        from corpus import GrafanaDashboardsQueryCorpusLoader
        from proc import ESProc, PrometheusProc
        from util import HttpClient

        self.http = HttpClient(timeout=self.http_timeout, retries=self.http_retries)
        self.corpus_loader = GrafanaDashboardsQueryCorpusLoader()
        self.control_proc = PrometheusProc(self, self.http)
        self.test_proc = ESProc(self, self.http)

    @property
    def es_data_stream(self) -> str:
        """The ES data stream name that receives this run's seeded data and
        that we query against. Format dictated by the ES Prometheus plugin:
        `metrics-{dataset}.prometheus-{namespace}`."""
        return f"metrics-{self.dataset}.prometheus-{self.run_id}"

    @classmethod
    def default(cls) -> Config:
        # `compliance/` lives at <repo>/dev-tools/prometheus-local/compliance.
        # docker-compose.yml lives one level up; es repo root is three up.
        script_dir = Path(__file__).resolve().parent
        compose_dir = script_dir.parent
        es_root = script_dir.parent.parent.parent
        return cls(
            control_url="http://prometheus.localhost",
            test_url="http://localhost:9200",
            test_user=None,
            test_password=None,
            script_dir=script_dir,
            compose_dir=compose_dir,
            es_root=es_root,
            http_timeout=30,
            http_retries=3,
        )
