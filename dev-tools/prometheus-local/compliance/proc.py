#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from __future__ import annotations

import asyncio
import struct
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from typing import Any, Awaitable, Protocol

import httpx
import snappy

from config import Config
from util import (
    Auth,
    HttpClient,
    HttpError,
    echo,
    ok,
    spinner,
    warn,
)


# ---------------------------------------------------------------------------
# Async subprocess + wait helpers
# ---------------------------------------------------------------------------


async def _run(
    *args: str,
    cwd: str | None = None,
    check: bool = False,
) -> tuple[int, bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    rc = proc.returncode or 0
    if check and rc != 0:
        raise subprocess.CalledProcessError(rc, list(args), stdout, stderr)
    return rc, stdout, stderr


async def wait_until(
    condition: Callable[[], Awaitable[bool]],
    *,
    timeout: int,
    interval: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if await condition():
            return
        await asyncio.sleep(interval)

    raise TimeoutError(f"timed out after {timeout}s")


# ---------------------------------------------------------------------------
# Prometheus wire layer: remote-write protobuf encoding + HTTP primitives
# ---------------------------------------------------------------------------


def _varint(value: int) -> bytes:
    if value < 0:
        value &= 0xFFFFFFFFFFFFFFFF

    buf = bytearray()
    while value > 0x7F:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value)
    return bytes(buf)


def _field_bytes(num: int, data: bytes) -> bytes:
    return _varint((num << 3) | 2) + _varint(len(data)) + data


def _field_varint(num: int, value: int) -> bytes:
    return _varint((num << 3) | 0) + _varint(value)


def _field_double(num: int, value: float) -> bytes:
    return _varint((num << 3) | 1) + struct.pack("<d", value)


def _encode_timeseries(
    labels: Mapping[str, str],
    samples: Sequence[tuple[float, int]],
) -> bytes:
    body = bytearray()

    # Prometheus requires labels sorted by name on the wire.
    for name in sorted(labels):
        label = _field_bytes(1, name.encode()) + _field_bytes(2, labels[name].encode())
        body.extend(_field_bytes(1, label))

    for value, ts_ms in samples:
        sample = _field_double(1, value) + _field_varint(2, ts_ms)
        body.extend(_field_bytes(2, sample))

    return bytes(body)


def _build_write_request(
    timeseries: Sequence[tuple[Mapping[str, str], Sequence[tuple[float, int]]]],
) -> bytes:
    return b"".join(
        _field_bytes(1, _encode_timeseries(labels, samples))
        for labels, samples in timeseries
    )


async def remote_write(
    http: HttpClient,
    url: str,
    timeseries: Sequence[tuple[Mapping[str, str], Sequence[tuple[float, int]]]],
    *,
    auth: Auth = None,
) -> None:
    """Send a Prometheus remote-write v1 request (snappy-compressed protobuf)."""
    compressed = snappy.compress(_build_write_request(timeseries))

    response = await http.post(
        url,
        content=compressed,
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


async def query_range(
    http: HttpClient,
    base_url: str,
    *,
    query: str,
    start: float,
    end: float,
    step: int,
    auth: Auth = None,
) -> tuple[dict[str, Any], float]:
    """Run a Prometheus-compatible `query_range`. Returns the raw payload and
    measured wall-clock latency in ms."""
    started = time.monotonic()

    response = await http.get(
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
            f"{base_url} returned HTTP {response.status_code}: {response.text[:300]}"
        )

    payload = response.json()

    if payload.get("status") != "success":
        raise HttpError(f"{base_url} returned non-success: {payload}")

    return payload, latency_ms


# ---------------------------------------------------------------------------
# Proc protocol
# ---------------------------------------------------------------------------


class Proc(Protocol):
    """A managed external process that participates in promcheck. Each Proc
    owns its lifecycle (setup/teardown) AND its data plane (write samples,
    run query_range queries). The orchestrator only sees this protocol -
    `control` and `test` are roles, not bound to any particular impl.
    """

    name: str

    async def setup(self) -> None: ...

    async def teardown(self) -> None: ...

    async def write(
        self,
        timeseries: Sequence[tuple[Mapping[str, str], Sequence[tuple[float, int]]]],
    ) -> None: ...

    async def query_range(
        self, *, query: str, start: float, end: float, step: int
    ) -> tuple[dict[str, Any], float]: ...

    async def __aenter__(self) -> Proc: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


# ---------------------------------------------------------------------------
# Prometheus (control)
# ---------------------------------------------------------------------------


class PrometheusProc:
    """Long-lived Prometheus container. `setup()` is idempotent - it brings
    the container up if needed, then deletes any leftover series from past
    runs via the admin API (so each run starts with a clean view). It does
    NOT wipe the TSDB. `teardown()` deletes only the series belonging to
    THIS run - the container itself stays running for future runs."""

    name = "control"

    def __init__(self, config: Config, http: HttpClient) -> None:
        self.config = config
        self.http = http

    async def __aenter__(self) -> PrometheusProc:
        await self.setup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.teardown()

    async def check_health(self) -> bool:
        try:
            response = await self.http.get(
                f"{self.config.control_url}/-/healthy",
                timeout=5,
                retries=1,
            )
            return response.status_code == 200
        except (HttpError, httpx.HTTPError):
            return False

    async def setup(self) -> None:
        # `docker compose up -d` is idempotent: it's a no-op if the
        # container is already running with the current config, and it
        # recreates the container when the compose file changes (e.g. when
        # the `--web.enable-admin-api` flag was added). Either way we don't
        # touch the TSDB.
        async with spinner("Ensuring Prometheus is up"):
            rc, _, stderr = await _run(
                "docker",
                "compose",
                "up",
                "-d",
                "prometheus",
                cwd=str(self.config.compose_dir),
            )

        if rc != 0 and not await self.check_health():
            raise RuntimeError(
                f"docker compose up failed:\n{stderr.decode(errors='replace')}"
            )

        async with spinner("Waiting for Prometheus to become healthy"):
            await wait_until(self.check_health, timeout=60)

        # Sweep any stragglers from a previous crashed run before we
        # ingest. `=~ .+` matches every value of the run-id label.
        await self._delete_series('{promcheck_run=~".+"}')
        ok(f"Prometheus ready (run_id={self.config.run_id})")

    async def teardown(self) -> None:
        async with spinner(f"Cleaning Prometheus run {self.config.run_id}"):
            await self._delete_series(f'{{promcheck_run="{self.config.run_id}"}}')
        ok("Prometheus run data cleared")

    async def _delete_series(self, matcher: str) -> None:
        """POST to the admin API to delete series matching the given
        matcher and free space via tombstone cleanup. Requires the server
        to have been started with `--web.enable-admin-api`."""
        try:
            response = await self.http.post(
                f"{self.config.control_url}/api/v1/admin/tsdb/delete_series",
                params={"match[]": matcher},
            )
            if response.status_code not in (200, 204):
                warn(
                    f"delete_series({matcher}) returned "
                    f"{response.status_code}: {response.text[:200]}"
                )
                return
            # Tombstones reclaim disk; without this `delete_series` only
            # marks data as deleted but leaves it on disk + queryable.
            await self.http.post(
                f"{self.config.control_url}/api/v1/admin/tsdb/clean_tombstones"
            )
        except (HttpError, httpx.HTTPError) as exc:
            warn(f"admin-api cleanup failed ({matcher}): {exc}")

    async def write(
        self,
        timeseries: Sequence[tuple[Mapping[str, str], Sequence[tuple[float, int]]]],
    ) -> None:
        await remote_write(
            self.http,
            f"{self.config.control_url}/api/v1/write",
            timeseries,
        )

    async def query_range(
        self, *, query: str, start: float, end: float, step: int
    ) -> tuple[dict[str, Any], float]:
        return await query_range(
            self.http,
            self.config.control_url,
            query=query,
            start=start,
            end=end,
            step=step,
        )


# ---------------------------------------------------------------------------
# Elasticsearch (test)
# ---------------------------------------------------------------------------


class ESProc:
    """Long-lived Elasticsearch process. `setup()` only spawns a JVM when
    nothing is already serving on `test_url`; otherwise it just verifies
    health and proceeds. Per-run isolation comes from the data stream
    `metrics-{dataset}.prometheus-{run_id}` which `teardown()` deletes
    while leaving the JVM running."""

    name = "test"

    def __init__(self, config: Config, http: HttpClient) -> None:
        self.config = config
        self.http = http

    async def __aenter__(self) -> ESProc:
        await self.setup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.teardown()

    @property
    def auth(self) -> Auth:
        if self.config.test_user and self.config.test_password:
            return (self.config.test_user, self.config.test_password)
        return None

    async def check_health(self) -> bool:
        try:
            response = await self.http.get(
                f"{self.config.test_url}/_cluster/health",
                auth=self.auth,
                timeout=5,
                retries=1,
            )
            return response.status_code == 200
        except (HttpError, httpx.HTTPError):
            return False

    async def setup(self) -> None:
        if await self.check_health():
            ok(f"Elasticsearch already running (run_id={self.config.run_id})")
            return

        echo(
            "Starting Elasticsearch via gradlew run "
            "(security disabled, logs suppressed)"
        )

        # `start_new_session=True` detaches the JVM from our session so it
        # outlives this Python process - intentional: ES startup costs
        # ~1 minute, so the first run starts it and subsequent runs reuse it.
        proc = await asyncio.create_subprocess_exec(
            str(self.config.es_root / "gradlew"),
            "run",
            "--configuration-cache",
            "-Dtests.es.http.host=0.0.0.0",
            "-Dtests.es.xpack.ml.enabled=false",
            "-Dtests.es.xpack.security.enabled=false",
            "-Drun.license_type=trial",
            "-Dtests.heap.size=4G",
            "-Dtests.jvm.argline=-da -dsa",
            cwd=str(self.config.es_root),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )

        async with spinner(f"Booting Elasticsearch (pid {proc.pid})"):
            await wait_until(self.check_health, timeout=300)

        ok(f"Elasticsearch ready (pid {proc.pid}, run_id={self.config.run_id})")

    async def teardown(self) -> None:
        async with spinner(f"Deleting data stream {self.config.es_data_stream}"):
            try:
                response = await self.http.delete(
                    f"{self.config.test_url}/_data_stream/"
                    f"{self.config.es_data_stream}",
                    auth=self.auth,
                )
                # 404 is fine: nothing to delete (e.g. teardown after a
                # setup that failed before any write).
                if response.status_code not in (200, 404):
                    warn(
                        f"delete data stream returned {response.status_code}: "
                        f"{response.text[:200]}"
                    )
            except (HttpError, httpx.HTTPError) as exc:
                warn(f"delete data stream failed: {exc}")
        ok("Elasticsearch run data cleared")

    async def write(
        self,
        timeseries: Sequence[tuple[Mapping[str, str], Sequence[tuple[float, int]]]],
    ) -> None:
        await remote_write(
            self.http,
            f"{self.config.test_url}/_prometheus/metrics/"
            f"{self.config.dataset}/{self.config.run_id}/api/v1/write",
            timeseries,
            auth=self.auth,
        )

    async def query_range(
        self, *, query: str, start: float, end: float, step: int
    ) -> tuple[dict[str, Any], float]:
        return await query_range(
            self.http,
            f"{self.config.test_url}/_prometheus/{self.config.es_data_stream}",
            query=query,
            start=start,
            end=end,
            step=step,
            auth=self.auth,
        )
