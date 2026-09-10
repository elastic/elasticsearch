# ES|QL public-data combinatorial suite

Queries **real, publicly-published data on real object stores** across the combinatorial matrix
(format × codec × layout × partitioning × read shape × real storage latency), checks correctness
against **frozen, oracle-derived expected tables**, and produces a machine-readable pass/fail
verdict. Built for elastic/esql-planning#1650: the stabilization review found the defect pattern
is combinatorial — most field defects live in the *intersection* of dimensions no per-dimension IT
crosses, under real blob-store latency.

Everything here derives from one file: `src/main/resources/public-data-catalog.yml`. It describes
every corpus, every physical variant (provider/format/codec/layout + pinned remote objects), every
declared coverage gap. The parameter factory, pin verification, coverage report and validation all
read it — which is what makes the extensibility contract hold.

## What runs where

| Task | Network | On `check` | Purpose |
|---|---|---|---|
| `test` | no | yes | offline unit tests (validator, filters, pins, parsing) |
| `validatePublicDataCatalog` | no | yes | structural validation of catalog + shipped csv-specs |
| `compilePublicDataTestJava` | no | yes | the remote suite is compiled + linted on `check`, **never run** there |
| `verifyOracleToolsIsolation` | no | yes | suite sources must not reference the authoring-only oracle tools |
| `publicDataTest` | yes | **no** | the remote suite, on its own nightly schedule |
| `verifyPublicDataPins` | metadata only | no | HEAD/LIST drift check; the pipeline's step 0 (writes pin-report.{md,json}) |
| `refreshPublicDataPins` | metadata only | no | prints refreshed `pin:` blocks; `--args '--write <catalog>'` splices them in place for a reviewed diff |
| `publicDataCoverageReport` | no | no | emits `build/public-data-results/coverage.{json,md}` |
| `publicDataDefectReport` | no | no | emits `defects.md` from `-Ignore`+`// defect:` tests and `disabled:` failure variants |
| `publicDataVerdict` | no | no | the pipeline's merge step: JUnit XMLs + pin report → `verdict.{json,md}`; exit code = pass/fail |
| `generateOracleScripts` | no | no | authoring aid; extracts `// oracle-sql:` into runnable scripts |

`check` must not build a default distribution and must not show a `publicDataTest` dependency:

```bash
./gradlew :x-pack:plugin:esql:qa:public-data:check --dry-run | grep -i publicDataTest
# expect: only compile/lint tasks, never the test task itself
```

## Running the suite

```bash
# Everything (3-node DEFAULT cluster, anonymous S3 reads; snapshot builds only):
./gradlew :x-pack:plugin:esql:qa:public-data:publicDataTest

# Focused run — filters compose; empty matches fail loudly listing available labels:
./gradlew :x-pack:plugin:esql:qa:public-data:publicDataTest \
    -Dtests.public_data.source=clickbench \
    -Dtests.public_data.variant='*-s3-parquet-*'
```

System properties (all `tests.public_data.*`): `source`, `spec`, `variant` (glob over labels),
`provider`, `format`, `codec`, `layout`, `shape`, `scale`, `quality`, `max_variants_per_spec`,
`max_retries` (default 3), `record` (diagnostic capture, see below), `output_dir`, `heap`
(cluster node heap, default 8g).

Failure attribution: correctness mismatches fail plainly; transient store trouble is retried and,
on exhaustion, fails as `INFRA_FAIL: ...` — never skipped, so a throttled bucket cannot hide a
regression. Upstream re-publishes surface in the pipeline's pin pre-step as `PIN_DRIFT`
(maintenance), not as fake regressions.

## Adding a corpus — 2 files, no Java

1. Add a `corpora:` entry to `public-data-catalog.yml`: pinned variants (metadata via
   `refreshPublicDataPins`), `auth: anonymous` on every S3 data source, declared gaps for anything
   the corpus does not cover.
2. Write `public-<corpus>.csv-spec` through the authoring workflow below.

Two opt-in catalog features exist for corpora that need them; neither costs anything for an
ordinary corpus:

- **`sub_resources:`** — named fragments of the same corpus (`st_nyc: "s3://.../STATION=.../*"`),
  addressable from a spec as `{{corpus:st_nyc}}`. That is how a workload reads one corpus both as a
  single dataset and as a multi-source `FROM d1, ..., dN` union over its parts. Name the union test
  `<base>Multi`: the validator then requires the fragments to exist on every active variant, and
  `ShippedCatalogContractTests` requires its expected table to be byte-identical to `<base>`'s —
  so the union is a *correctness cross-check*, verified offline, not just another query.
- **`assertion_mode: invariant`** + **`pin: {volatile: true}`** — for corpora whose publisher
  rewrites the objects on a schedule (NOAA's by_year CSVs). Each query reduces to a drift-proof
  constant (a calibrated band, a date range, a fixed cardinality, an absent needle, an exact
  `LIMIT`) instead of a frozen table, every test must carry `// assertion-mode: invariant` and
  `// oracle-observed: <value measured at authoring time>`, the pin stops comparing ETags, and
  `coverage.md` reports the cell as `covered (invariant)` rather than `covered`. Use it only where
  the volatility is documented upstream — the validator demands a written `notes:` justification
  and refuses a volatile pin on an `exact` corpus.

That's it: `PublicDataIT` enumerates the whole catalog; a unit test
(`ShippedCatalogContractTests`) fails if a catalogued corpus is not reachable from its parameters,
so a new corpus cannot be silently unrun. `./gradlew :x-pack:plugin:esql:qa:public-data:check`
validates the rest (1:1 corpus↔workload, all four read shapes, pins, tie-breakers, row caps,
no `file://` anywhere, ...).

## The oracle: authoring-time only, never a suite component

Expected tables are established **once, at authoring time**, by running SQL through
`clickhouse local` (primary; DuckDB as a second opinion behind the same SPI in `tools/oracle/`)
against a **one-time local scratch download** of the pinned objects. What ships is the frozen,
human-reviewed expected table plus its SQL as `//` provenance. The running suite has no oracle
dependency and no comparison leg — a live oracle would drift against ES|QL semantics and turn
every scheduled run into a mending exercise; a frozen, reviewed expectation is what makes a red
run mean something.

Per corpus: survey the schema → adapt the query shapes (SQL first) → derive answers from the
scratch copy (`"$CLICKHOUSE_BIN" local`, default `~/.local/bin/clickhouse`) → translate to ES|QL
with deterministic `SORT` tie-breakers and row limits → freeze after review. The scratch copy
lives outside the repo, is never checked in, never referenced by the catalog, and the suite is
structurally incapable of reading it (the `ESQL_EXTERNAL_DATASOURCES_LOCAL` feature flag stays
off).

**On a mismatch**: if it is a translation error, fix the ES|QL. Anything else — candidate ES
defect or genuine semantic divergence — goes through the stop-and-ask gate: no change to expected
tables, queries or corpus selection without explicit sign-off.
`-Dtests.public_data.record=true` captures what ES|QL actually returned (under
`build/public-data-results/recorded/`) strictly as a diagnostic for that conversation; it is
never a source of expected values.

## Defects are the point

When a query fails and the cause is a product defect: never fix the product from here. Rename the
one failing test `<name>-Ignore`, keep its query + expected table in place as the reproducer, and
record a `// defect:` block above it (validator-enforced; an `-Ignore` without `// defect:` or
`// disabled:` fails `check`). Defect-disabled cells report as *exercised and known-broken* in
coverage — distinct from both `covered` and `gap`.

## Runtime budget

Nightly, ~12h orientative. Never query hundreds of GBs from a single source: huge corpora run
partial fractions, in-query filters, or catalog-declared `query_subset`s (still covering all four
read shapes — trimmed legs report as `covered (subset: n/m)`, never as full coverage). Per-leg
wall-clock is logged on every execution; tuning is iterative by design.

Measured Phase-5 legs (local, 3-node cluster, `-Dtests.public_data.heap=4g`, 2026-08-25):
`ghcnd-by-year-1750` 28 tests / 1m44s; `ghcnd-parquet-8st` 30 tests / 4m24s;
`ghcnd-by-year-2024` 15 tests / 35m49s. One outlier inside the last one is worth watching rather
than tuning away: `q06_y2024DatesWithinYear` took **1235s** on the 1.3 GB uncompressed object
against **156s** on its gzip twin (same logical rows) and ~65–75s for every other uncompressed test
on the same object — with no retries, no warnings and a correct answer. That is a performance
observation for triage, not a correctness defect, and it is recorded as a `// timing:` note on the
test itself. It is also exactly the shape #1650 says is under-tested, so the nightly timings are
where to watch it.

## Operational notes (learned from the first real runs, 2026-08-13)

Hard-won facts future maintainers should not have to rediscover:

- **REST client timeout**: the property `ESRestTestCase` honors is
  `tests.client.socket.timeout` (default 60s — which silently kills any minutes-long SYNC remote
  scan as a `SocketTimeoutException`). The `tests.rest.client_timeout`/`tests.rest.socket_timeout`
  names that appear in some sibling build files are **not** read by the REST client.
- **429 does not mean throttling**: ES|QL surfaces request-circuit-breaker trips to the client as
  `429 Too Many Requests`, the same status as store-throttling exhaustion. Read the response's
  cause chain (`CircuitBreakingException` vs throttle retries) before concluding anything. When it
  IS the breaker: fix the query's aggregation state with an in-query filter (reviewed into query +
  oracle SQL per the runtime-budget policy) — q19/q33/q34/q35 use the counter-62 July slice because
  their unfiltered forms hold 18M–100M-group states no non-spilling engine can carry.
- **Headerless-CSV declared schemas bind by name**: with `header_row: false` the reader
  synthesizes `col0..colN` names and declared mapping columns bind **by name**, not positionally —
  each declared column needs a `path: colN` rename or it silently reads null everywhere (with a
  "not present in some source files" warning). Related: parse catalog YAML with `mapOrdered()`;
  `XContentParser.map()` returns an unordered map and scrambles the 105 properties.
- **Engine-portable regex only in expectations**: Java regex (`REPLACE`) and RE2 (ClickHouse)
  disagree on `$` before a trailing newline; the `[\s\S]*` tail form behaves identically in both
  (see q29's provenance).
- **Exact count-distinct at scale**: `MV_COUNT(VALUES(x))` materializes the value set per group;
  use the two-stage `STATS BY x | STATS COUNT(*)` form instead (exact, streaming).
- **Per-leg wall-clock on a 12-core/32GB workstation** (first green pass, for budget tuning):
  parquet single-file 44 queries ≈ 27 min; 100-shard glob 44 queries ≈ 15 min; gzip-CSV 6-query
  subset ≈ 37 min (each query re-streams the full 15.5 GiB, ~5–6 min per query).
- **Known defect on file**: `q04_statsAvgUserid-Ignore` — ES|QL `AVG(long)` accumulates in double
  precision and is ~8 orders of magnitude off the exact mean on ±9.2e18-magnitude values (equals
  ClickHouse `avg(toFloat64(x))`; the oracle's exact accumulation differs). Kept in the spec as
  the reproducer with its `// defect:` block; goes into the Phase-4 defect report.

## The verdict threshold (pipeline pass/fail)

**PASS** iff (i) the executed test count equals the catalog-derived expectation **and is > 0** —
a silently skipped or self-disabled task can never produce a green run; (ii) every executed leg
passes; (iii) every non-executed cell is `blocked`, a declared `gap`, or defect-disabled.
A correctness mismatch is an unconditional **FAIL**. Retry-exhausted store trouble is
**INFRA_FAIL** (attributed, still red). A pin mismatch from the pre-step marks that corpus's legs
**PIN_DRIFT** and the run fails as *maintenance* (re-pin via `refreshPublicDataPins --write`,
re-derive, re-review). There is no oracle term at runtime.

The nightly pipeline is `.buildkite/pipelines/periodic-esql-public-data.yml`: pin pre-step →
one shard per corpus (ClickBench split per variant) → merge/verdict step whose exit code is the
run's pass/fail. Wire the nightly schedule in Buildkite settings; the red-run notification hook
is a marked TBD in the yml pending an owner/channel.

## Datasources and matrix coverage

The twelve catalogued corpora, all on **anonymous S3** (the only active provider). Every workload
covers **all four read shapes** (SCAN, AGGREGATE, TOPN, LIMIT) — including the ClickBench text
legs, whose 6-query `query_subset` was chosen to keep all four; read shape is therefore not
repeated per row.

| corpus | what it is | format | codec | layout | partitioning | scale | quality |
|---|---|---|---|---|---|---|---|
| `clickbench` | 100M-row web-analytics hits (immutable since 2022), one logical corpus in 5 physical variants | parquet | snappy | single_file | none | huge | clean |
| | — same rows, 100 Parquet shards (glob) | parquet | snappy | uniform_shards | none | huge | clean |
| | — same rows, one 15.5 GiB headerless gzip text file each | csv, tsv, ndjson | gzip | single_file | none | huge | clean |
| `cse-cic-ids2018` | Real dirty security logs (CICFlowMeter CSVs): embedded mid-file headers, Excel row-truncation, Infinity/NaN columns, clock-skew rows | csv | uncompressed | uniform_shards | none | medium | schema-drift |
| `openaq` | 298 few-hundred-byte gzip CSVs of air-quality readings in `year=/month=` subtrees; per-object overhead + detected partition columns are the case | csv | gzip | nested_hive | hive | small | clean |
| `clickbench-dirty` | Failure-only: deliberately wrong configs over pinned ClickBench objects (mislabeled format, mispointed glob, zero-byte, nonexistent key) — each must fail with a clean client error | csv, parquet | uncompressed | single_file, uniform_shards | none | small | mislabeled |
| `ookla-fixed-2024` | 26M broadband speed-test tiles, four 2024 quarters | parquet | snappy | hive_partitioned | hive | medium | clean |
| `ghcnd-usw3` | 195 NOAA station files × 2 mirrored trees with proven-identical rows but different dialects (headered vs systematically headerless) | csv | uncompressed, gzip | many_small | none | medium | schema-drift |
| `ghcnd-parquet-8st` | The same NOAA observations as `ghcnd-usw3`, Parquet-encoded: 8 US stations × all elements in a two-level `STATION=/ELEMENT=` tree. Carries the suite's only **multi-source** reads — 15 shapes × (one comma-list dataset vs eight datasets unioned by `FROM`), which must agree | parquet | snappy | nested_hive | hive | small | clean |
| `ghcnd-by-year-1750` | The one frozen object in NOAA's nightly-rebuilt by_year tree (it holds pre-GHCN-D dates, so the rebuild skips it); mirrored headered/headerless pair with sha1-proven identical rows | csv | uncompressed, gzip | single_file | none | small | schema-drift |
| `ghcnd-by-year-2024` | The large-uncompressed-text leg: a 1.3 GB CSV single object (37.1M rows) plus its 168 MB gzip twin. **`assertion_mode: invariant`** — NOAA rewrites these nightly, so the queries assert drift-proof constants while still streaming the whole object | csv | uncompressed, gzip | single_file | none | large | clean |
| `overture-divisions` | 4.7M map divisions, deeply nested schema (structs/maps/arrays + geometry); carries the nested-read probe (q30) | parquet | zstd | single_file | none | medium | clean |
| `btc-tx-skew` | Bitcoin transactions, six pinned dates spanning 2011 (~1 MB) vs 2024 (~600 MB): a genuine 1000× shard-size skew | parquet | snappy | skewed_shards | none | large | clean |
| `abo-listings` | 147K Amazon product listings, extreme per-attribute nesting (language-tagged value arrays), pins frozen since 2021 | ndjson | gzip | uniform_shards | none | small | clean |

Dimension closure against the issue's matrix:

- **Formats**: all four (parquet, csv, tsv, ndjson). **Codecs**: all four (snappy, gzip, zstd,
  uncompressed; parquet compression is internal, so text carries gzip/uncompressed).
- **Layouts**: single_file, uniform_shards, many_small, skewed_shards, hive_partitioned,
  nested_hive — all except **WIDE_SINGLE_ROW_GROUP** (declared gap: no anonymously-readable
  public carrier; Common Crawl denies anonymous S3, Overture parts carry 256 row groups).
- **Partitioning**: none, hive-detected (OpenAQ nested + Ookla flat), plus the disclosed
  partition-key-shadowing case (BTC runs `hive_partitioning: false` because `date=` path keys
  would shadow the physical column).
- **Scale**: small → huge. **Quality**: clean, schema-drift, mislabeled (failure corpus).
- **Providers**: S3 active; HTTPS backup-only by decision; GCS/Azure modeled but not yet usable
  (declared gaps, activation is a catalog edit).

## Phase status

Phases 0–4 delivered: ClickBench (5 legs incl. the gzip text subsets), CSE-CIC-IDS2018 (dirty
security logs), OpenAQ (nested hive), the dirty-data failure corpus, Ookla (hive partitions),
GHCN-D (many-small, mirrored codecs), Overture (zstd + nested schema), BTC transactions (1000×
skew), ABO (nested NDJSON) — all four read shapes each. Remaining
declared gaps: WIDE_SINGLE_ROW_GROUP (no anonymously-readable public carrier found; Common Crawl
denies anonymous S3 and sits as a backup entry), GCS/Azure providers, HTTPS (backup-only by
decision).

Phase 5 added the two NOAA datasets from the PR review: `ghcnd-parquet-8st` (parquet × nested_hive,
and the suite's first **multi-source** `FROM d1, ..., d8` coverage against real object storage) and
the by_year pair `ghcnd-by-year-1750` (exact) / `ghcnd-by-year-2024` (invariant, volatile pins) —
**12 corpora, 20 pinned variants**. The `ghcnd-by-year-exact-tables-impossible` gap records why the
by_year tree can never carry exact tables, with the upstream evidence, so it is not re-proposed.

Current defects on file: see `publicDataDefectReport`.
