"""Runtime engine: HTTP client, service orchestration, Prometheus
remote-write, seeding, query execution, result comparison, and reporting.
Everything that talks to Prometheus or Elasticsearch lives here."""

from __future__ import annotations

import json
import math
import os
import struct
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

import click
import requests
import snappy

from corpus import SyntheticDataFactory
from util import (
    AppConfig,
    Auth,
    DEFAULT_FAILURE_CAP,
    HttpError,
    PointDiff,
    QueryCase,
    QueryComparison,
    SAMPLE_INTERVAL_SECONDS,
    SeriesIssue,
    STATUS_BOLD,
    STATUS_DIM,
    STATUS_FG,
    echo,
    ok,
    section,
    spinner,
    warn,
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
        session: requests.Session | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.session = session or requests.Session()

    def request(
        self,
        method: str,
        url: str,
        *,
        retries: int | None = None,
        backoff: float = 0.5,
        **kwargs: Any,
    ) -> requests.Response:
        attempts = retries if retries is not None else self.retries
        kwargs.setdefault("timeout", self.timeout)

        last_error: BaseException | None = None

        for attempt in range(attempts):
            try:
                response = self.session.request(method, url, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
            else:
                if response.status_code < 500:
                    return response
                last_error = HttpError(
                    f"HTTP {response.status_code} from {url}: {response.text[:200]}"
                )

            time.sleep(backoff * (2**attempt))

        raise HttpError(
            f"HTTP {method} {url} failed after {attempts} attempts: {last_error}"
        )

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("POST", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("DELETE", url, **kwargs)


# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------


def wait_until(
    condition: Callable[[], bool],
    *,
    timeout: int,
    interval: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(interval)

    raise TimeoutError(f"timed out after {timeout}s")


class ServiceManager:
    def __init__(self, config: AppConfig, http: HttpClient) -> None:
        self.config = config
        self.http = http

    def elasticsearch_healthy(self, auth: Auth = None) -> bool:
        try:
            response = self.http.get(
                f"{self.config.es_url}/_cluster/health",
                auth=auth,
                timeout=5,
                retries=1,
            )
            return response.status_code == 200
        except HttpError, requests.RequestException:
            return False

    def prometheus_healthy(self) -> bool:
        try:
            response = self.http.get(
                f"{self.config.prometheus_url}/-/healthy",
                timeout=5,
                retries=1,
            )
            return response.status_code == 200
        except HttpError, requests.RequestException:
            return False

    def ensure_elasticsearch(self) -> Auth:
        known_auths: tuple[Auth, ...] = (
            None,
            ("elastic", "password"),
            ("elastic-admin", "elastic-password"),
        )

        for auth in known_auths:
            if self.elasticsearch_healthy(auth):
                ok("Elasticsearch already running")
                return auth

        if self.config.es_pid_file.exists():
            auth = self._wait_for_existing_elasticsearch_pid()
            if auth is not False:
                return auth

        return self._start_elasticsearch()

    def _wait_for_existing_elasticsearch_pid(self) -> Auth | bool:
        try:
            pid = int(self.config.es_pid_file.read_text().strip())
            os.kill(pid, 0)
        except ProcessLookupError, ValueError:
            self.config.es_pid_file.unlink(missing_ok=True)
            return False

        with spinner(f"Waiting for Elasticsearch (pid {pid})"):
            wait_until(lambda: self.elasticsearch_healthy(None), timeout=300)

        ok(f"Elasticsearch ready (pid {pid})")
        return None

    def _start_elasticsearch(self) -> Auth:
        echo(
            "Starting Elasticsearch via gradlew run "
            "(security disabled, logs suppressed)"
        )

        proc = subprocess.Popen(
            [
                str(self.config.es_root / "gradlew"),
                "run",
                "--configuration-cache",
                "-Dtests.es.http.host=0.0.0.0",
                "-Dtests.es.xpack.ml.enabled=false",
                "-Dtests.es.xpack.security.enabled=false",
                "-Drun.license_type=trial",
                "-Dtests.heap.size=4G",
                "-Dtests.jvm.argline=-da -dsa",
            ],
            cwd=str(self.config.es_root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

        self.config.es_pid_file.write_text(str(proc.pid))

        with spinner(
            f"Booting Elasticsearch (pid {proc.pid}, stop with: kill {proc.pid})"
        ):
            wait_until(lambda: self.elasticsearch_healthy(None), timeout=300)

        ok(f"Elasticsearch ready (pid {proc.pid})")
        return None

    def ensure_docker_services(self) -> None:
        if self.prometheus_healthy():
            ok("Prometheus already running")
            return

        with spinner("Starting Docker services"):
            result = subprocess.run(
                ["docker", "compose", "up", "-d"],
                cwd=str(self.config.compose_dir),
                capture_output=True,
                text=True,
            )

        if result.returncode != 0 and not self.prometheus_healthy():
            raise RuntimeError(f"docker compose up failed:\n{result.stderr}")

        with spinner("Waiting for Prometheus to become healthy"):
            wait_until(self.prometheus_healthy, timeout=60)

        ok("Prometheus ready")

    def restart_prometheus_with_clean_tsdb(self) -> None:
        project = self.config.compose_dir.name
        volume = f"{project}_prometheus_data"

        with spinner("Clearing Prometheus TSDB"):
            subprocess.run(
                ["docker", "compose", "stop", "prometheus"],
                cwd=str(self.config.compose_dir),
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{volume}:/data",
                    "alpine",
                    "sh",
                    "-c",
                    "rm -rf /data/*",
                ],
                check=True,
                capture_output=True,
            )
            subprocess.run(
                ["docker", "compose", "start", "prometheus"],
                cwd=str(self.config.compose_dir),
                check=True,
                capture_output=True,
            )
            wait_until(self.prometheus_healthy, timeout=30)


# ---------------------------------------------------------------------------
# Prometheus remote-write protobuf encoding v1
# ---------------------------------------------------------------------------


class RWEncoder:
    @staticmethod
    def varint(value: int) -> bytes:
        if value < 0:
            value &= 0xFFFFFFFFFFFFFFFF

        buf = bytearray()
        while value > 0x7F:
            buf.append((value & 0x7F) | 0x80)
            value >>= 7
        buf.append(value)
        return bytes(buf)

    @classmethod
    def field_bytes(cls, num: int, data: bytes) -> bytes:
        return cls.varint((num << 3) | 2) + cls.varint(len(data)) + data

    @classmethod
    def field_varint(cls, num: int, value: int) -> bytes:
        return cls.varint((num << 3) | 0) + cls.varint(value)

    @classmethod
    def field_double(cls, num: int, value: float) -> bytes:
        return cls.varint((num << 3) | 1) + struct.pack("<d", value)

    @classmethod
    def encode_timeseries(
        cls,
        labels: Mapping[str, str],
        samples: Sequence[tuple[float, int]],
    ) -> bytes:
        body = bytearray()

        # Prometheus requires labels sorted by name on the wire.
        for name in sorted(labels):
            label = cls.field_bytes(1, name.encode()) + cls.field_bytes(
                2,
                labels[name].encode(),
            )
            body.extend(cls.field_bytes(1, label))

        for value, ts_ms in samples:
            sample = cls.field_double(1, value) + cls.field_varint(2, ts_ms)
            body.extend(cls.field_bytes(2, sample))

        return bytes(body)

    @classmethod
    def build_write_request(
        cls,
        timeseries: Sequence[tuple[Mapping[str, str], Sequence[tuple[float, int]]]],
    ) -> bytes:
        return b"".join(
            cls.field_bytes(1, cls.encode_timeseries(labels, samples))
            for labels, samples in timeseries
        )


class RWClient:
    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def write(
        self,
        url: str,
        timeseries: Sequence[tuple[Mapping[str, str], Sequence[tuple[float, int]]]],
        *,
        auth: Auth = None,
    ) -> None:
        compressed = snappy.compress(RWEncoder.build_write_request(timeseries))

        response = self.http.post(
            url,
            data=compressed,
            headers={
                "Content-Type": "application/x-protobuf",
                "Content-Encoding": "snappy",
                "X-Prometheus-Remote-Write-Version": "0.1.0",
            },
            auth=auth,
        )

        if response.status_code not in (200, 204):
            raise HttpError(
                f"remote write to {url} failed [{response.status_code}]: "
                f"{response.text[:300]}"
            )


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


class Seeder:
    def __init__(
        self,
        config: AppConfig,
        http: HttpClient,
        services: ServiceManager,
        remote_write: RWClient,
    ) -> None:
        self.config = config
        self.http = http
        self.services = services
        self.remote_write = remote_write

    def seed(
        self,
        *,
        now_ms: int,
        es_auth: Auth,
        metric_names: Iterable[str],
        samples: int,
        rng_seed: int,
    ) -> int:
        interval_ms = SAMPLE_INTERVAL_SECONDS * 1000
        end_ms = (now_ms // interval_ms) * interval_ms

        timeseries = SyntheticDataFactory.generate_timeseries(
            now_ms=now_ms,
            metric_names=metric_names,
            samples=samples,
            seed=rng_seed,
        )

        if not timeseries:
            raise RuntimeError("no metrics extracted from corpus to seed")

        series_count = len(timeseries)

        self.services.restart_prometheus_with_clean_tsdb()

        with spinner(
            f"Writing {series_count} series x {samples} samples to Prometheus"
        ):
            self.remote_write.write(
                f"{self.config.prometheus_url}/api/v1/write",
                timeseries,
            )
        ok(f"Prometheus seeded ({series_count} series)")

        with spinner("Clearing Elasticsearch data stream"):
            response = self.http.delete(
                f"{self.config.es_url}/_data_stream/metrics-generic.prometheus-default",
                auth=es_auth,
                timeout=30,
            )

        if response.status_code not in (200, 404):
            warn(
                f"could not clear Elasticsearch data stream: HTTP {response.status_code}"
            )

        with spinner(
            f"Writing {series_count} series x {samples} samples to Elasticsearch"
        ):
            self.remote_write.write(
                f"{self.config.es_url}/_prometheus/api/v1/write",
                timeseries,
                auth=es_auth,
            )

        ok(f"Elasticsearch seeded ({series_count} series)")
        return end_ms


# ---------------------------------------------------------------------------
# Query execution and comparison
# ---------------------------------------------------------------------------


class QueryClient:
    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def query_range(
        self,
        base_url: str,
        *,
        query: str,
        start: float,
        end: float,
        step: int,
        auth: Auth = None,
    ) -> tuple[dict[str, Any], float]:
        started = time.monotonic()

        response = self.http.get(
            f"{base_url}/api/v1/query_range",
            params={
                "query": query,
                "start": start,
                "end": end,
                "step": step,
            },
            auth=auth,
        )

        latency_ms = (time.monotonic() - started) * 1000.0

        if response.status_code != 200:
            raise HttpError(
                f"{base_url} returned HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )

        payload = response.json()

        if payload.get("status") != "success":
            raise HttpError(f"{base_url} returned non-success: {payload}")

        return payload, latency_ms


class ResultComparator:
    @staticmethod
    def normalize(result: Mapping[str, Any]) -> dict[str, list[tuple[float, float]]]:
        """Index series by canonical label key. __name__ is stripped because
        Prometheus drops it from function results while Elasticsearch may
        retain it."""
        output: dict[str, list[tuple[float, float]]] = {}

        for series in result.get("data", {}).get("result", []):
            labels = {
                key: value
                for key, value in series.get("metric", {}).items()
                if key != "__name__"
            }
            label_key = json.dumps(labels, sort_keys=True)
            output[label_key] = [
                (float(timestamp), float(value))
                for timestamp, value in series.get("values", [])
            ]

        return output

    @classmethod
    def compare_into(
        cls,
        prom: Mapping[str, Any],
        es: Mapping[str, Any],
        comparison: QueryComparison,
        *,
        failure_cap: int = DEFAULT_FAILURE_CAP,
    ) -> None:
        prom_norm = cls.normalize(prom)
        es_norm = cls.normalize(es)

        prom_keys = set(prom_norm)
        es_keys = set(es_norm)

        only_prom = sorted(prom_keys - es_keys)
        only_es = sorted(es_keys - prom_keys)

        if only_prom:
            comparison.series_issues.append(
                SeriesIssue(f"only in prom ({len(only_prom)}): {only_prom[:3]}")
            )

        if only_es:
            comparison.series_issues.append(
                SeriesIssue(f"only in es   ({len(only_es)}): {only_es[:3]}")
            )

        common_keys = sorted(prom_keys & es_keys)
        comparison.series_count = len(common_keys)

        for key in common_keys:
            prom_points = dict(prom_norm[key])
            es_points = dict(es_norm[key])

            prom_timestamps = set(prom_points)
            es_timestamps = set(es_points)

            only_prom_ts = sorted(prom_timestamps - es_timestamps)
            only_es_ts = sorted(es_timestamps - prom_timestamps)

            if only_prom_ts:
                comparison.series_issues.append(
                    SeriesIssue(
                        f"{key}: {len(only_prom_ts)} ts only in prom "
                        f"(e.g. {only_prom_ts[:3]})"
                    )
                )

            if only_es_ts:
                comparison.series_issues.append(
                    SeriesIssue(
                        f"{key}: {len(only_es_ts)} ts only in es "
                        f"(e.g. {only_es_ts[:3]})"
                    )
                )

            for timestamp in sorted(prom_timestamps & es_timestamps):
                comparison.point_count += 1

                prom_value = prom_points[timestamp]
                es_value = es_points[timestamp]

                if math.isnan(prom_value) and math.isnan(es_value):
                    continue

                values_differ = (
                    math.isnan(prom_value) != math.isnan(es_value)
                    or prom_value != es_value
                )

                if not values_differ:
                    continue

                comparison.failure_count += 1

                if not (math.isnan(prom_value) or math.isnan(es_value)):
                    comparison.max_abs_diff = max(
                        comparison.max_abs_diff,
                        abs(prom_value - es_value),
                    )

                if len(comparison.failures) < failure_cap:
                    abs_diff = (
                        abs(prom_value - es_value)
                        if not (math.isnan(prom_value) or math.isnan(es_value))
                        else math.nan
                    )
                    comparison.failures.append(
                        PointDiff(timestamp, prom_value, es_value, abs_diff)
                    )

        comparison.ok = not comparison.series_issues and comparison.failure_count == 0


class QueryRunner:
    def __init__(
        self,
        *,
        config: AppConfig,
        query_client: QueryClient,
        comparator: type[ResultComparator] = ResultComparator,
    ) -> None:
        self.config = config
        self.query_client = query_client
        self.comparator = comparator

    def run(
        self,
        case: QueryCase,
        *,
        end_sec: float,
        es_auth: Auth,
    ) -> QueryComparison:
        start_sec = end_sec - case.range_seconds
        comparison = QueryComparison(case=case, start_sec=start_sec, end_sec=end_sec)

        try:
            prom_result, comparison.prom_latency_ms = self.query_client.query_range(
                self.config.prometheus_url,
                query=case.expr,
                start=start_sec,
                end=end_sec,
                step=case.step_seconds,
            )
            es_result, comparison.es_latency_ms = self.query_client.query_range(
                f"{self.config.es_url}/_prometheus",
                query=case.expr,
                start=start_sec,
                end=end_sec,
                step=case.step_seconds,
                auth=es_auth,
            )
        except (HttpError, requests.RequestException, ValueError) as exc:
            comparison.error = str(exc)
            return comparison

        self.comparator.compare_into(prom_result, es_result, comparison)
        return comparison


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


class Reporter:
    @staticmethod
    def format_ts(timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    @staticmethod
    def shorten(text: str, limit: int = 100) -> str:
        text = text.replace("\n", " ")
        return text if len(text) <= limit else text[: limit - 1] + "..."

    @staticmethod
    def status_label(status: str) -> str:
        return click.style(
            f"{status:<5}",
            fg=STATUS_FG.get(status),
            bold=status in STATUS_BOLD,
            dim=status in STATUS_DIM,
        )

    @classmethod
    def print_corpus_stats(cls, stats: Any) -> None:
        breakdown = click.style(
            f"(unsupported={stats.unsupported} "
            f"unsubstitutable={stats.unsubstitutable} "
            f"duplicate={stats.duplicate})",
            dim=True,
        )
        click.echo(
            f"  {click.style('corpus:', bold=True)} loaded={stats.loaded} "
            f"skipped={stats.skipped} {breakdown}"
        )

    @classmethod
    def print_one_line(cls, comparison: QueryComparison) -> None:
        status = cls.status_label(comparison.status)
        name = click.style(f"{comparison.case.name:<22}", bold=True)
        expr = click.style(cls.shorten(comparison.case.expr, 90), dim=True)

        suffix = ""

        if comparison.skipped and comparison.error:
            suffix = " " + click.style(cls.shorten(comparison.error, 60), dim=True)
        elif comparison.error:
            suffix = " " + click.style(cls.shorten(comparison.error, 60), fg="red")
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

    @classmethod
    def print_query_detail(cls, comparison: QueryComparison) -> None:
        case = comparison.case

        click.echo()
        click.echo(f"  {click.style(case.name, bold=True)}")
        click.echo(f"    {click.style('expr   :', dim=True)} {case.expr}")
        click.echo(
            f"    {click.style('window :', dim=True)} "
            f"[{cls.format_ts(comparison.start_sec)} .. "
            f"{cls.format_ts(comparison.end_sec)}]  "
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
            f"prom={comparison.prom_latency_ms:.0f}ms "
            f"es={comparison.es_latency_ms:.0f}ms"
        )

        for issue in comparison.series_issues[:5]:
            click.echo(
                f"    {click.style('issue  :', fg='yellow')} {issue.description}"
            )

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
                    f"      {cls.format_ts(failure.timestamp)}  "
                    f"prom={failure.prom:.8g}  "
                    f"es={failure.es:.8g}  "
                    f"abs={failure.abs_diff:.3e}"
                )

    @classmethod
    def print_summary(cls, results: Sequence[QueryComparison]) -> None:
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
                f"  {cls.status_label(status)} {count:>4}  "
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
                detail = click.style(cls.shorten(result.error, 70), fg="red")
            else:
                diff = (
                    "inf"
                    if math.isinf(result.max_abs_diff)
                    else f"{result.max_abs_diff:.3g}"
                )
                detail = click.style(f"max_abs={diff}", fg="red")

            click.echo(
                f"  {cls.status_label(result.status)} "
                f"{click.style(f'{result.case.name:<22}', bold=True)} {detail}"
            )
