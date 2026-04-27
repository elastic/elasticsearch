#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".


from __future__ import annotations

import re
import sys
import time
from collections.abc import Sequence
from pathlib import Path

import click
import requests

from util import (
    AppConfig,
    Auth,
    DEFAULT_SAMPLES,
    DEFAULT_SEED,
    EXIT_DIFFS,
    EXIT_ERROR,
    EXIT_OK,
    HttpError,
    QueryCase,
    QueryComparison,
    SAMPLE_INTERVAL_SECONDS,
    err,
    section,
)
from corpus import CorpusLoader, GrafanaCorpusLoader, MetricExtractor
from runner import (
    HttpClient,
    QueryClient,
    QueryRunner,
    Reporter,
    RWClient,
    Seeder,
    ServiceManager,
)


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def filter_cases(cases: Sequence[QueryCase], regex: str | None) -> list[QueryCase]:
    if not regex:
        return list(cases)

    pattern = re.compile(regex)
    filtered = [case for case in cases if pattern.search(case.expr)]

    if not filtered:
        raise click.ClickException(f"no queries match regex: {regex}")

    return filtered


def list_cases(cases: Sequence[QueryCase]) -> None:
    for case in cases:
        click.echo(f"  {case.name:<26} {case.expr}")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class Promcheck:
    def __init__(
        self,
        config: AppConfig,
        corpus_loader: CorpusLoader | None = None,
    ) -> None:
        self.config = config
        self.http = HttpClient(
            timeout=config.http_timeout,
            retries=config.http_retries,
        )
        self.services = ServiceManager(config, self.http)
        self.remote_write = RWClient(self.http)
        self.seeder = Seeder(config, self.http, self.services, self.remote_write)
        self.query_runner = QueryRunner(
            config=config,
            query_client=QueryClient(self.http),
        )
        self.corpus_loader: CorpusLoader = corpus_loader or GrafanaCorpusLoader()

    def run(self, ctx: click.Context) -> int:
        params = ctx.params
        samples: int = params["samples"]
        rng_seed: int = params["rng_seed"]
        corpus_csv: Path = params["corpus_csv"]
        filter_regex: str | None = params["filter_regex"]
        filter_err_regex: str | None = params["filter_err_regex"]
        es_user: str | None = params["es_user"]
        es_password: str | None = params["es_password"]
        list_only: bool = params["list_only"]

        if samples < 4:
            raise click.ClickException("--samples must be >= 4")

        if bool(es_user) ^ bool(es_password):
            raise click.ClickException(
                "--es-user and --es-password must be provided together"
            )

        err_pattern = re.compile(filter_err_regex) if filter_err_regex else None

        total_seed_sec = samples * SAMPLE_INTERVAL_SECONDS
        range_window = max(60, total_seed_sec // 3)
        end_buffer = max(30, total_seed_sec // 6)

        cases, stats = self.corpus_loader.load(
            corpus_csv,
            range_seconds=range_window,
            step_seconds=30,
        )

        Reporter.print_corpus_stats(stats)

        cases = filter_cases(cases, filter_regex)

        if not cases:
            raise click.ClickException("no queries to run after filtering")

        if list_only:
            list_cases(cases)
            return EXIT_OK

        section("Services")

        try:
            es_auth = self.services.ensure_elasticsearch()
            self.services.ensure_docker_services()
        except (TimeoutError, RuntimeError) as exc:
            err(f"service setup failed: {exc}")
            return EXIT_ERROR

        if es_user and es_password:
            es_auth = (es_user, es_password)

        metric_names = self._extract_metric_names(cases)

        section(
            "Seeding",
            f"({len(metric_names)} series x {samples} samples, seed={rng_seed})",
        )

        try:
            end_ms = self.seeder.seed(
                now_ms=int(time.time() * 1000),
                es_auth=es_auth,
                metric_names=metric_names,
                samples=samples,
                rng_seed=rng_seed,
            )
        except (HttpError, requests.RequestException, RuntimeError) as exc:
            err(f"seeding failed: {exc}")
            return EXIT_ERROR

        # Let Elasticsearch indexing settle so query-time data is consistent.
        time.sleep(2)

        end_sec = float(end_ms) / 1000.0 - end_buffer

        section("Comparing", f"({len(cases)} queries)")

        results = self._run_comparisons(
            cases,
            end_sec=end_sec,
            es_auth=es_auth,
            err_pattern=err_pattern,
        )

        for result in results:
            if result.status not in ("OK", "SKIP"):
                Reporter.print_query_detail(result)

        Reporter.print_summary(results)

        has_diffs = any(not result.ok and not result.skipped for result in results)
        return EXIT_DIFFS if has_diffs else EXIT_OK

    @staticmethod
    def _extract_metric_names(cases: Sequence[QueryCase]) -> set[str]:
        names: set[str] = set()
        for case in cases:
            names.update(MetricExtractor.extract_metric_names(case.expr))
        return names

    def _run_comparisons(
        self,
        cases: Sequence[QueryCase],
        *,
        end_sec: float,
        es_auth: Auth,
        err_pattern: re.Pattern[str] | None,
    ) -> list[QueryComparison]:
        results: list[QueryComparison] = []

        for case in cases:
            comparison = self.query_runner.run(case, end_sec=end_sec, es_auth=es_auth)

            if (
                comparison.error
                and err_pattern
                and err_pattern.search(comparison.error)
            ):
                comparison.skipped = True

            results.append(comparison)
            Reporter.print_one_line(comparison)

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
    default=None,
    metavar="REGEX",
    help="Run only queries whose expression matches this regex.",
)
@click.option(
    "--filter-err",
    "filter_err_regex",
    default=None,
    metavar="REGEX",
    help=(
        "Reclassify ERROR results whose message matches REGEX as SKIP. "
        "Doesn't affect exit code."
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
    "--samples",
    type=int,
    default=DEFAULT_SAMPLES,
    show_default=True,
    help=f"Samples per series, at {SAMPLE_INTERVAL_SECONDS}s spacing.",
)
@click.option("--es-user", default=None, help="Elasticsearch username.")
@click.option("--es-password", default=None, help="Elasticsearch password.")
@click.option("--list", "list_only", is_flag=True, help="List queries and exit.")
@click.pass_context
def main(ctx: click.Context, **_: object) -> None:
    sys.exit(Promcheck(AppConfig.from_environment()).run(ctx))


if __name__ == "__main__":
    main()
