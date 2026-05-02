#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from __future__ import annotations

import asyncio
import re
import sys
import time
from collections.abc import Sequence
from pathlib import Path

import click

import state
from config import Config
from corpus import QueryCase
from comparator import CompareResult, Comparator
from util import (
    CHRONO,
    DEFAULT_SEED,
    EXIT_DIFFS,
    EXIT_ERROR,
    EXIT_OK,
    REGEX,
    SEED_WINDOW_SECONDS,
    err,
    ok,
    print_one_line,
    print_query_detail,
    print_summary,
    section,
    spinner,
)
from tsgen import TSGen


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def matches_err_filter(
    error: str | None, pattern: re.Pattern[str] | None
) -> bool:
    """True if the (case-folded) error matches `pattern`. Lowercasing here
    is the contract for `--filter-err`: users write a single lowercase
    pattern and we match it against `error.lower()`, so the pattern
    doesn't need to know whether the backend emitted `Function` or
    `function`, `HTTP 500` or `http 500`, etc."""
    return bool(error and pattern and pattern.search(error.lower()))


def filter_cases(
    cases: Sequence[QueryCase], pattern: re.Pattern[str] | None
) -> list[QueryCase]:
    if pattern is None:
        return list(cases)

    filtered = [case for case in cases if pattern.search(case.expr)]

    if not filtered:
        raise click.ClickException(f"no queries match regex: {pattern.pattern}")

    return filtered


def list_cases(cases: Sequence[QueryCase]) -> None:
    for case in cases:
        click.echo(f"  {case.name:<26} {case.expr}")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class Promcheck:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.comparator = Comparator(
            control_proc=config.control_proc,
            test_proc=config.test_proc,
        )

    async def run(self, ctx: click.Context) -> int:
        try:
            state.acquire(self.config.state_dir, self.config.run_id)
        except state.StateLockError as exc:
            err(str(exc))
            return EXIT_ERROR

        try:
            return await self._run(ctx)
        finally:
            state.release(self.config.state_dir)
            await self.config.http.aclose()

    async def _run(self, ctx: click.Context) -> int:
        params = ctx.params
        granularity_sec: int = params["spacing"]
        rng_seed: int = params["rng_seed"]
        corpus_csv: Path = params["corpus_csv"]
        filter_pattern: re.Pattern[str] | None = params["filter_regex"]
        err_pattern: re.Pattern[str] | None = params["filter_err_regex"]
        list_only: bool = params["list_only"]

        samples = SEED_WINDOW_SECONDS // granularity_sec
        range_window = max(60, SEED_WINDOW_SECONDS // 3)
        end_buffer = max(30, SEED_WINDOW_SECONDS // 6)

        cases, stats = self.config.corpus_loader.load(
            corpus_csv,
            range_seconds=range_window,
            step_seconds=30,
        )

        click.echo(stats)

        cases = filter_cases(cases, filter_pattern)

        if not cases:
            raise click.ClickException("no queries to run after filtering")

        if list_only:
            list_cases(cases)
            return EXIT_OK

        try:
            # Bring both processes up in parallel. If either setup raises,
            # the matching `finally` below still tears down whichever did
            # complete, so a partial setup can't leak processes.
            await asyncio.gather(
                self.config.control_proc.setup(), self.config.test_proc.setup()
            )
        except (TimeoutError, RuntimeError) as exc:
            err(f"service setup failed: {exc}")
            await self._teardown_services()
            return EXIT_ERROR

        try:
            section(
                "Seeding",
                f"({len(stats.dimensions)} metrics x {samples} samples @ "
                f"{granularity_sec}s, seed={rng_seed})",
            )

            try:
                end_ms = await self._seed(
                    dimensions=stats.dimensions,
                    granularity_sec=granularity_sec,
                    samples=samples,
                    rng_seed=rng_seed,
                )
            except RuntimeError as exc:
                err(f"seeding failed: {exc}")
                return EXIT_ERROR

            # Let test-side indexing settle so query-time data is consistent.
            await asyncio.sleep(2)

            end_sec = float(end_ms) / 1000.0 - end_buffer

            section("Comparing", f"({len(cases)} queries)")

            results = await self._run_comparisons(
                cases,
                end_sec=end_sec,
                err_pattern=err_pattern,
            )
        finally:
            await self._teardown_services()

        for result in results:
            if result.status not in ("OK", "SKIP"):
                print_query_detail(result)

        print_summary(results)

        has_diffs = any(not result.ok and not result.skipped for result in results)
        return EXIT_DIFFS if has_diffs else EXIT_OK

    async def _teardown_services(self) -> None:
        # `return_exceptions=True` so a teardown failure on one process
        # doesn't prevent the other from being cleaned up.
        await asyncio.gather(
            self.config.control_proc.teardown(),
            self.config.test_proc.teardown(),
            return_exceptions=True,
        )

    async def _seed(
        self,
        *,
        dimensions: dict[str, dict[str, set[str]]],
        granularity_sec: int,
        samples: int,
        rng_seed: int,
    ) -> int:
        now_ms = int(time.time() * 1000)
        interval_ms = granularity_sec * 1000
        end_ms = (now_ms // interval_ms) * interval_ms

        timeseries = TSGen(
            dimensions=dimensions,
            granularity_sec=granularity_sec,
            window_sec=SEED_WINDOW_SECONDS,
            seed=rng_seed,
        ).generate(now_ms=now_ms)

        if not timeseries:
            raise RuntimeError("no metrics extracted from corpus to seed")

        # Tag every series with the run id. On Prometheus this is what
        # `_delete_series` matches on for cleanup; on ES the data stream
        # already isolates this run, but we tag both sides identically so
        # query results stay symmetric in the comparator.
        timeseries = [
            ({**labels, "promcheck_run": self.config.run_id}, samples)
            for labels, samples in timeseries
        ]

        series_count = len(timeseries)

        # Each Proc owns its own remote-write URL + auth. The orchestrator
        # just hands the data off and lets both run in parallel.
        async with spinner(
            f"Writing {series_count} series x {samples} samples to control + test"
        ):
            await asyncio.gather(
                self.config.control_proc.write(timeseries),
                self.config.test_proc.write(timeseries),
            )

        ok(f"control + test seeded ({series_count} series)")
        return end_ms

    async def _run_comparisons(
        self,
        cases: Sequence[QueryCase],
        *,
        end_sec: float,
        err_pattern: re.Pattern[str] | None,
    ) -> list[CompareResult]:
        results: list[CompareResult] = []

        for case in cases:
            comparison = await self.comparator.run(case, end_sec=end_sec)

            if matches_err_filter(comparison.error, err_pattern):
                comparison.skipped = True

            results.append(comparison)
            print_one_line(comparison)

        return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument(
    "corpus_csv",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--filter",
    "filter_regex",
    type=REGEX,
    default=None,
    metavar="REGEX",
    help="Run only queries whose expression matches this regex.",
)
@click.option(
    "--filter-err",
    "filter_err_regex",
    type=REGEX,
    default=None,
    metavar="REGEX",
    help=(
        "Reclassify ERROR results whose message matches REGEX as SKIP. "
        "Matched against the lowercased error - write your pattern in "
        "lowercase only. Doesn't affect exit code."
    ),
)
@click.option(
    "--seed",
    "rng_seed",
    type=int,
    default=DEFAULT_SEED,
    show_default=True,
    help="RNG seed for synthetic data.",
)
@click.option(
    "--spacing",
    type=CHRONO,
    default="15s",
    show_default=True,
    metavar="DURATION",
    help="Sample spacing granularity",
)
@click.option("--list", "list_only", is_flag=True, help="List queries and exit.")
@click.pass_context
def main(ctx: click.Context, **_: object) -> None:
    sys.exit(asyncio.run(Promcheck(Config.default()).run(ctx)))


if __name__ == "__main__":
    main()
