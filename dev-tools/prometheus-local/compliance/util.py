#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from __future__ import annotations

import asyncio
import math
import re
import sys
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import click
import httpx

if TYPE_CHECKING:
    from comparator import CompareResult
    from corpus import CorpusStats


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JOB_LABEL = "promcheck"
INSTANCE_LABEL = "test:9999"
COMMON_LABELS = {"job": JOB_LABEL, "instance": INSTANCE_LABEL}

SEED_WINDOW_SECONDS = 30 * 60  # total time span of seeded data (30 minutes)
MIN_SAMPLES_PER_SERIES = 4  # window/granularity must yield at least this many

DEFAULT_FAILURE_CAP = 5
DEFAULT_SEED = 42

EXIT_OK = 0
EXIT_DIFFS = 1
EXIT_ERROR = 2

COUNTER_SUFFIXES = ("_total", "_count", "_sum", "_seconds")

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


@asynccontextmanager
async def spinner(message: str) -> AsyncIterator[None]:
    if not sys.stdout.isatty():
        echo(f"  ... {message}")
        yield
        return

    chars = "|/-\\"
    started = time.monotonic()
    stop = asyncio.Event()

    async def animate() -> None:
        i = 0
        while not stop.is_set():
            elapsed = time.monotonic() - started
            spin = click.style(chars[i % len(chars)], fg="cyan", bold=True)
            sys.stdout.write(
                f"\r  {spin} {message} {click.style(f'{elapsed:4.1f}s', dim=True)}"
            )
            sys.stdout.flush()
            i += 1
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.08)
            except TimeoutError:
                pass

    task = asyncio.create_task(animate())

    try:
        yield
    finally:
        stop.set()
        await task
        sys.stdout.write("\r" + " " * (len(message) + 14) + "\r")
        sys.stdout.flush()


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PointDiff:
    timestamp: float
    control: float
    test: float
    abs_diff: float


@dataclass(frozen=True)
class SeriesIssue:
    description: str


class HttpError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Variable substitution
# ---------------------------------------------------------------------------


def substitute(query: str, kv: dict[str, str]) -> tuple[bool, str]:
    """Substitute `$var` and `${var}` references in `query` using `kv`.

    Returns `(ok, result)`. `ok` is False whenever at least one unknown
    variable appears - either inside a string literal (where we leave a
    `__unknown__` placeholder so the rest of the query still parses) or
    outside one (where we leave the raw `$var` text in place since there's
    no valid PromQL placeholder for it). The loader uses `ok=False` to
    drop the query entirely - no PromQL engine accepts a bare `$`.
    """
    out: list[str] = []
    i = 0
    quote: str | None = None
    all_known = True

    while i < len(query):
        ch = query[i]

        if ch in ("'", '"'):
            if quote is None:
                quote = ch
            elif quote == ch:
                quote = None
            out.append(ch)
            i += 1
            continue

        if ch != "$":
            out.append(ch)
            i += 1
            continue

        if i + 1 < len(query) and query[i + 1] == "{":
            end = i + 2
            while end < len(query) and (query[end].isalnum() or query[end] == "_"):
                end += 1

            if end == i + 2:
                out.append(ch)
                i += 1
                continue

            has_closing_brace = end < len(query) and query[end] == "}"
            raw = "$" + query[i + 2 : end]
            original = query[i : end + 1] if has_closing_brace else query[i:end]
            i = end + 1 if has_closing_brace else end
        else:
            end = i + 1
            while end < len(query) and (query[end].isalnum() or query[end] == "_"):
                end += 1

            if end == i + 1:
                out.append(ch)
                i += 1
                continue

            raw = query[i:end]
            original = raw
            i = end

        value = kv.get(raw)
        if value is not None:
            out.append(value)
        elif quote is not None:
            all_known = False
            out.append("__unknown__")
        else:
            all_known = False
            out.append(original)

    return all_known, "".join(out)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def format_ts(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def shorten(text: str, limit: int = 100) -> str:
    text = text.replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 1] + "..."


def status_label(status: str) -> str:
    return click.style(
        f"{status:<5}",
        fg=STATUS_FG.get(status),
        bold=status in STATUS_BOLD,
        dim=status in STATUS_DIM,
    )


def print_one_line(comparison: CompareResult) -> None:
    status = status_label(comparison.status)
    name = click.style(f"{comparison.case.name:<22}", bold=True)
    expr = click.style(shorten(comparison.case.expr, 90), dim=True)

    suffix = ""

    if comparison.skipped and comparison.error:
        suffix = " " + click.style(shorten(comparison.error, 60), dim=True)
    elif comparison.error:
        suffix = " " + click.style(shorten(comparison.error, 60), fg="red")
    elif comparison.failure_count:
        diff = (
            "inf"
            if math.isinf(comparison.max_abs_diff)
            else f"{comparison.max_abs_diff:.3g}"
        )
        suffix = " " + click.style(
            f"max_abs={diff} ({comparison.failure_count} pts)",
            fg="red",
        )
    elif comparison.series_issues:
        suffix = " " + click.style(
            f"{len(comparison.series_issues)} series issue(s)",
            fg="yellow",
        )

    click.echo(f"  {status} {name} {expr}{suffix}")


def print_query_detail(comparison: CompareResult) -> None:
    case = comparison.case

    click.echo()
    click.echo(f"  {click.style(case.name, bold=True)}")
    click.echo(f"    {click.style('expr   :', dim=True)} {case.expr}")
    click.echo(
        f"    {click.style('window :', dim=True)} "
        f"[{format_ts(comparison.start_sec)} .. "
        f"{format_ts(comparison.end_sec)}]  "
        f"step={case.step_seconds}s"
    )

    if case.notes:
        click.echo(f"    {click.style('notes  :', dim=True)} {case.notes}")

    if comparison.error:
        click.echo(
            f"    {click.style('ERROR', fg='red', bold=True)}: {comparison.error}"
        )
        return

    click.echo(
        f"    {click.style('stats  :', dim=True)} "
        f"series={comparison.series_count} "
        f"points={comparison.point_count} "
        f"control={comparison.control_latency_ms:.0f}ms "
        f"test={comparison.test_latency_ms:.0f}ms"
    )

    for issue in comparison.series_issues[:5]:
        click.echo(f"    {click.style('issue  :', fg='yellow')} {issue.description}")

    if len(comparison.series_issues) > 5:
        click.echo(
            f"    {click.style('issue  :', fg='yellow')} "
            f"... ({len(comparison.series_issues) - 5} more)"
        )

    if comparison.failure_count:
        diff = (
            "inf"
            if math.isinf(comparison.max_abs_diff)
            else f"{comparison.max_abs_diff:.4g}"
        )
        click.echo(
            f"    {click.style('diffs  :', fg='red')} "
            f"max_abs={diff} "
            f"({comparison.failure_count} of {comparison.point_count} pts)"
        )

        for failure in comparison.failures:
            click.echo(
                f"      {format_ts(failure.timestamp)}  "
                f"control={failure.control:.8g}  "
                f"test={failure.test:.8g}  "
                f"abs={failure.abs_diff:.3e}"
            )


def print_summary(results: Sequence[CompareResult]) -> None:
    counts: dict[str, int] = {}

    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1

    section("Summary")

    notes = {
        "OK": "matched exactly",
        "FAIL": "differ",
        "ERROR": "query errored on one engine",
        "SKIP": "error matched --filter-err",
    }

    for status in ("OK", "FAIL", "ERROR", "SKIP"):
        count = counts.get(status, 0)
        if not count:
            continue

        click.echo(
            f"  {status_label(status)} {count:>4}  "
            f"{click.style(notes[status], dim=True)}"
        )

    click.echo(f"  {click.style('TOTAL', bold=True):<5} {len(results):>4}")

    bad = [result for result in results if not result.ok and not result.skipped]
    if not bad:
        return

    click.echo()
    click.echo(click.style("Differences:", bold=True))

    for result in bad:
        if result.error:
            detail = click.style(shorten(result.error, 70), fg="red")
        else:
            diff = (
                "inf"
                if math.isinf(result.max_abs_diff)
                else f"{result.max_abs_diff:.3g}"
            )
            detail = click.style(f"max_abs={diff}", fg="red")

        click.echo(
            f"  {status_label(result.status)} "
            f"{click.style(f'{result.case.name:<22}', bold=True)} {detail}"
        )


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class HttpClient:
    def __init__(
        self,
        *,
        timeout: int,
        retries: int,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.client = client or httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def request(
        self,
        method: str,
        url: str,
        *,
        retries: int | None = None,
        backoff: float = 0.5,
        **kwargs: Any,
    ) -> httpx.Response:
        attempts = retries if retries is not None else self.retries
        last_error: BaseException | None = None

        for attempt in range(attempts):
            try:
                response = await self.client.request(method, url, **kwargs)
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                last_error = exc
            else:
                if response.status_code < 500:
                    return response
                last_error = HttpError(
                    f"HTTP {response.status_code} from {url}: {response.text[:200]}"
                )

            await asyncio.sleep(backoff * (2**attempt))

        raise HttpError(
            f"HTTP {method} {url} failed after {attempts} attempts: {last_error}"
        )

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("POST", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("DELETE", url, **kwargs)


class RegexType(click.ParamType):
    """A regex pattern compiled at parse time. Click skips `convert` when the
    option is missing and the default is None, so the orchestrator receives
    `re.Pattern | None` without us having to handle None here."""

    name = "regex"

    def convert(
        self,
        value: str | re.Pattern[str],
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> re.Pattern[str]:
        if isinstance(value, re.Pattern):
            return value
        try:
            return re.compile(value)
        except re.error as exc:
            self.fail(f"invalid regex {value!r}: {exc}", param, ctx)


REGEX = RegexType()


_DURATION_RE = re.compile(r"^(\d+)([smh])$")
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600}


class GranularityType(click.ParamType):
    name = "duration"

    def convert(
        self,
        value: str | int,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> int:
        if isinstance(value, int):
            return value

        match = _DURATION_RE.match(value)
        if not match:
            self.fail(
                f"invalid duration {value!r}; expected NNs / NNm / NNh", param, ctx
            )
        seconds = int(match.group(1)) * _DURATION_UNITS[match.group(2)]
        if seconds <= 0:
            self.fail(f"duration must be > 0, got {value!r}", param, ctx)

        samples = SEED_WINDOW_SECONDS // seconds
        if samples < MIN_SAMPLES_PER_SERIES:
            self.fail(
                f"--spacing {value} yields {samples} samples per series in a "
                f"{SEED_WINDOW_SECONDS}s window; need >= {MIN_SAMPLES_PER_SERIES}",
                param,
                ctx,
            )
        return seconds


CHRONO = GranularityType()
