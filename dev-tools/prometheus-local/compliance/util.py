"""Domain types, constants, and output helpers shared by the rest of the
package."""

from __future__ import annotations

import os
import re
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import click


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JOB_LABEL = "promcheck"
INSTANCE_LABEL = "test:9999"
COMMON_LABELS = {"job": JOB_LABEL, "instance": INSTANCE_LABEL}

SAMPLE_INTERVAL_SECONDS = 15

DEFAULT_HTTP_TIMEOUT = 30
DEFAULT_HTTP_RETRIES = 3
DEFAULT_FAILURE_CAP = 5
DEFAULT_SEED = 42
DEFAULT_SAMPLES = 120

EXIT_OK = 0
EXIT_DIFFS = 1
EXIT_ERROR = 2

COUNTER_SUFFIXES = ("_total", "_count", "_sum", "_seconds")

UNKNOWN_VAR_PLACEHOLDER = "__compare_promql_unknown__"

UNSUPPORTED_FUNCTIONS = frozenset({"histogram_quantile"})

GRAFANA_VAR_SUBS: dict[str, str] = {
    "$__rate_interval": "5m",
    "$__range": "1h",
    "$__interval_ms": "30000",
    "$__interval": "30s",
    "$__auto_interval_interval": "30s",
    "$rate_interval": "5m",
    "$interval": "30s",
    "$node": INSTANCE_LABEL,
    "$instance": INSTANCE_LABEL,
    "$Node": INSTANCE_LABEL,
    "$Instance": INSTANCE_LABEL,
    "$job": JOB_LABEL,
    "$Job": JOB_LABEL,
    "$cluster": "default",
    "$Cluster": "default",
    "$namespace": "default",
    "$Namespace": "default",
}

PROMQL_KEYWORDS = frozenset(
    {
        "sum",
        "min",
        "max",
        "avg",
        "group",
        "stddev",
        "stdvar",
        "count",
        "count_values",
        "bottomk",
        "topk",
        "quantile",
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
        "and",
        "or",
        "unless",
        "bool",
        "offset",
        "atan2",
        "abs",
        "absent",
        "absent_over_time",
        "ceil",
        "changes",
        "clamp",
        "clamp_max",
        "clamp_min",
        "delta",
        "deriv",
        "exp",
        "floor",
        "histogram_avg",
        "histogram_count",
        "histogram_fraction",
        "histogram_quantile",
        "histogram_stddev",
        "histogram_stdvar",
        "histogram_sum",
        "holt_winters",
        "idelta",
        "increase",
        "irate",
        "label_join",
        "label_replace",
        "ln",
        "log10",
        "log2",
        "predict_linear",
        "rate",
        "resets",
        "round",
        "scalar",
        "sgn",
        "sort",
        "sort_desc",
        "sqrt",
        "timestamp",
        "vector",
        "time",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "day_of_year",
        "days_in_month",
        "hour",
        "minute",
        "acos",
        "acosh",
        "asin",
        "asinh",
        "atan",
        "atanh",
        "cos",
        "cosh",
        "deg",
        "pi",
        "rad",
        "sin",
        "sinh",
        "tan",
        "tanh",
        "avg_over_time",
        "min_over_time",
        "max_over_time",
        "sum_over_time",
        "count_over_time",
        "stddev_over_time",
        "stdvar_over_time",
        "quantile_over_time",
        "last_over_time",
        "present_over_time",
        "Inf",
        "NaN",
        "true",
        "false",
    }
)

METRIC_NAME_RE = re.compile(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\b")
UNSUBSTITUTED_VAR_RE = re.compile(r"\$\w+")

STATUS_FG: dict[str, str | None] = {
    "OK": "green",
    "FAIL": "red",
    "ERROR": "red",
    "SKIP": None,
}
STATUS_BOLD = frozenset({"ERROR"})
STATUS_DIM = frozenset({"SKIP"})

Auth = tuple[str, str] | None


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def echo(message: str = "") -> None:
    click.echo(message)


def ok(message: str) -> None:
    click.echo(f"{click.style('[OK]', fg='green')} {message}")


def warn(message: str) -> None:
    click.echo(f"{click.style('warning:', fg='yellow')} {message}")


def err(message: str) -> None:
    click.echo(f"{click.style('error:', fg='red', bold=True)} {message}")


def section(title: str, suffix: str = "") -> None:
    suffix_text = f" {click.style(suffix, dim=True)}" if suffix else ""
    click.echo()
    click.echo(click.style(f"=== {title} ===", bold=True) + suffix_text)


@contextmanager
def spinner(message: str) -> Iterator[None]:
    if not sys.stdout.isatty():
        echo(f"  ... {message}")
        yield
        return

    stop = threading.Event()
    chars = "|/-\\"
    started = time.monotonic()

    def animate() -> None:
        i = 0
        while not stop.is_set():
            elapsed = time.monotonic() - started
            spin = click.style(chars[i % len(chars)], fg="cyan", bold=True)
            sys.stdout.write(
                f"\r  {spin} {message} {click.style(f'{elapsed:4.1f}s', dim=True)}"
            )
            sys.stdout.flush()
            i += 1
            stop.wait(0.08)

    thread = threading.Thread(target=animate, daemon=True)
    thread.start()

    try:
        yield
    finally:
        stop.set()
        thread.join()
        sys.stdout.write("\r" + " " * (len(message) + 14) + "\r")
        sys.stdout.flush()


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AppConfig:
    prometheus_url: str
    es_url: str
    script_dir: Path
    compose_dir: Path
    es_root: Path
    es_pid_file: Path
    http_timeout: int = DEFAULT_HTTP_TIMEOUT
    http_retries: int = DEFAULT_HTTP_RETRIES

    @classmethod
    def from_environment(cls) -> AppConfig:
        # `compliance/` lives at <repo>/dev-tools/prometheus-local/compliance.
        # docker-compose.yml lives one level up; es repo root is three up.
        script_dir = Path(__file__).resolve().parent
        compose_dir = script_dir.parent
        es_root = script_dir.parent.parent.parent
        return cls(
            prometheus_url=os.environ.get(
                "COMPARE_PROMQL_PROMETHEUS_URL",
                "http://prometheus.localhost",
            ),
            es_url=os.environ.get("COMPARE_PROMQL_ES_URL", "http://localhost:9200"),
            script_dir=script_dir,
            compose_dir=compose_dir,
            es_root=es_root,
            es_pid_file=script_dir / ".es.pid",
        )


@dataclass(frozen=True)
class QueryCase:
    name: str
    expr: str
    range_seconds: int
    step_seconds: int
    notes: str = ""


@dataclass(frozen=True)
class CorpusStats:
    loaded: int = 0
    blank: int = 0
    unsupported: int = 0
    unsubstitutable: int = 0
    duplicate: int = 0

    @property
    def skipped(self) -> int:
        return self.unsupported + self.unsubstitutable + self.duplicate

    def increment(self, field_name: str) -> CorpusStats:
        return CorpusStats(
            loaded=self.loaded + (field_name == "loaded"),
            blank=self.blank + (field_name == "blank"),
            unsupported=self.unsupported + (field_name == "unsupported"),
            unsubstitutable=self.unsubstitutable + (field_name == "unsubstitutable"),
            duplicate=self.duplicate + (field_name == "duplicate"),
        )


@dataclass(frozen=True)
class PointDiff:
    timestamp: float
    prom: float
    es: float
    abs_diff: float


@dataclass(frozen=True)
class SeriesIssue:
    description: str


@dataclass
class QueryComparison:
    case: QueryCase
    start_sec: float
    end_sec: float
    ok: bool = False
    series_count: int = 0
    point_count: int = 0
    failure_count: int = 0
    max_abs_diff: float = 0.0
    series_issues: list[SeriesIssue] = field(default_factory=list)
    failures: list[PointDiff] = field(default_factory=list)
    prom_latency_ms: float = 0.0
    es_latency_ms: float = 0.0
    error: str | None = None
    skipped: bool = False

    @property
    def status(self) -> str:
        if self.skipped:
            return "SKIP"
        if self.error is not None:
            return "ERROR"
        return "OK" if self.ok else "FAIL"


class HttpError(RuntimeError):
    pass
