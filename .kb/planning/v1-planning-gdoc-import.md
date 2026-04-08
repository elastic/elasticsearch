# ES|QL External Data Sources — Issue Tracker

Imported from [Google Doc](https://docs.google.com/document/d/1waqqZmXhn1F29SEjhEMl4Qas47ThN5C2clkqUHVD93Q/edit) on 2026-03-06. Each item is a task issue under [elastic/esql-planning#189](https://github.com/elastic/esql-planning/issues/189) (Goal). Issue titles use `Datasources: <name>` prefix.

---

## Context

### Where We Are Now

| Dimension | Current state (main, March 2026) |
|-----------|----------------------------------|
| User experience | Dev-gated `EXTERNAL "uri"` syntax — not available in production builds. No named datasources, no CRUD API. Credentials inlined in query text. |
| Formats & storage | Parquet (columnar reader via parquet-mr ColumnReadStoreImpl), NDJSON.gz (streaming), ORC (bulk ColumnBlockConversions), CSV/TSV (configurable, parallel parsing). S3, GCS, Azure, HTTP. ZSTD/BZIP2/GZIP compression. Distributed execution works for file-based sources. |
| Performance | Parquet reader is columnar (column-at-a-time via ColumnReader, PR #143703). LIMIT pushdown works (PR #143515). Column projection plumbed through FormatReader but optimizer doesn't narrow attributes (PruneColumns gap). No row-group filter pushdown, no page index, no bloom filters. No schema cache — metadata re-read on every query. L1 partition pruning works for Hive layouts. Two-phase STATS and distributed pipeline breakers work for external sources (PR #143696). |
| Correctness | Several commands and functions crash or fail on external sources. **Runtime crashes:** match(), match_phrase(), multi_match(), knn(), score() — Lucene assertion failure (no DocBlock). **Verifier errors:** kql(), qstr(). **Hardcoded blocks:** LOOKUP JOIN with external on right side (Mapper gate), ENRICH _remote (verifier), TS/METRICS commands (require TIME_SERIES IndexMode). **Integration gaps:** Views referencing EXTERNAL sources fail (metadata pre-fetch runs before view inlining). **Data integrity:** TSV parses with comma delimiter (no auto-detection by extension). **Works correctly:** all processing commands (WHERE, EVAL, STATS, SORT, LIMIT, KEEP, DROP, RENAME, DISSECT, GROK, MV_EXPAND), all scalar functions, ENRICH _coordinator/_any, LOOKUP JOIN (external left), INLINESTATS, SAMPLE, FORK, subqueries, async queries, CCS + EXTERNAL (serialization now works). |
| Kibana | No integration. Queries execute if typed manually in ESQL editor but no syntax highlighting for `FROM datasource:`, no autocomplete, no schema discovery, no management UI. |
| Not included | Iceberg is metadata-only (scan throws "not yet supported"). No cross-source queries. No thread pool isolation. |

### GA Product Shape

| Dimension | Outcome |
|-----------|---------|
| User experience | Persistent named datasources via REST CRUD API. `EXTERNAL` command syntax with named datasource references, query specs, and WITH overrides. Credentials stored in a secrets system index. Kibana integration for datasource management, syntax highlighting, and autocomplete. |
| Formats & storage | Parquet, NDJSON.gz, ORC, CSV/TSV. S3, GCS, Azure, HTTP. ZSTD/BZIP2/GZIP compression. |
| Performance | Parquet reader with row-group filter pushdown, column projection, and LIMIT pushdown. Persistent metadata inference cache. Two-phase STATS for external sources (same as ES indices — partial on data nodes, merge on coordinator). Parquet reader strategy (extend parquet-mr vs Arrow Dataset) under investigation. |
| Correctness | Unsupported commands (match, knn, etc.) produce clear error messages with alternatives. CSV/TSV RFC 4180 compliance. Union-by-name for schema drift across files. |
| Stability | Thread pool isolation prevents external I/O from starving regular ES operations. Full circuit breaker integration (source buffers + Arrow). Distributed execution hardened under node failure, large split counts, heterogeneous split sizes. Load/perf testing framework. Stabilization buffers for TP and GA. |
| Kibana | Datasource management UI, syntax highlighting, autocomplete via schema discovery API. |
| Not included | No Iceberg. No cross-source queries. HTTP and Flight connectors still disabled. |

---

## Summary Table

All items are tasks under [elastic/esql-planning#189](https://github.com/elastic/esql-planning/issues/189).

### User Experience
| # | Issue title | MS | Est |
|---|------|----|-----|
| 1 | Datasources: Secret management | TP | ~3w |
| 2 | Datasources: EXTERNAL command syntax | TP | ~2w |
| 14 | Datasources: CRUD API | GA | ~2w |
| 16 | Datasources: Schema discovery API | GA | ~1w |

### Correctness
| # | Issue title | MS | Est |
|---|------|----|-----|
| 3 | Datasources: Unsupported command errors | TP | ~0.5w |
| 4 | Datasources: CSV/TSV RFC 4180 compliance | TP | ~0.5w |
| 5 | Datasources: Schema union-by-name | TP | ~3w |

### Parquet Reader
| # | Issue title | MS | Est |
|---|------|----|-----|
| 10 | Datasources: Parquet reader strategy investigation | TP | ~1w |
| 11 | Datasources: [Parquet-MR] Reader improvements | TP | ~2w |
| 6a | Datasources: [Parquet-MR] Circuit breaker integration | TP | ~4w |
| 12 | Datasources: [Native Arrow] Arrow Dataset Parquet reader | TP | ~4w |
| 6 | Datasources: [Native Arrow] Arrow allocator circuit breaker | TP | ~1w |

### Performance
| # | Issue title | MS | Est |
|---|------|----|-----|
| 13 | Datasources: Metadata inference cache | TP | ~3w |
| 24 | Datasources: Intra-file parallelism | TP | ~1w |
| 26 | Datasources: Column pruning for external sources | TP | ~0.5w |

### Stability
| # | Issue title | MS | Est |
|---|------|----|-----|
| 7 | Datasources: JMH microbenchmarks | TP | ~1w |
| 7a | Datasources: Rally track | TP | ~2w |
| 17 | Datasources: Byte-based memory safety for source buffers | GA | ~2w |
| 18 | Datasources: Thread pool isolation | GA | ~1w |
| 19 | Datasources: Distributed execution hardening | GA | ~2w |

### Release
| # | Issue title | MS | Est |
|---|------|----|-----|
| 22 | Datasources: Pricing & licensing definition | GA | |

### Done (file as resolved with PRs attached)
| # | Issue title | PR |
|---|------|----|
| 9 | Datasources: LIMIT pushdown | elastic/elasticsearch#143515 |
| 23 | Datasources: Vectorized readers | elastic/elasticsearch#143703 |
| 21 | Datasources: Two-phase STATS | elastic/elasticsearch#143696 |
| 25 | Datasources: Distributed planning & execution | elastic/elasticsearch#143696 |

---

## Issues

### #1 — Datasources: Secret management

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~3w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Production deployments need secure credential storage. Without this, credentials must be inlined in queries — visible in logs, audit trails, and slow query reports. The mechanism must work consistently across self-managed, Cloud, and stateless deployments.

#### Approach

Secrets system index (`.secrets-datasource`), following the established pattern used independently by three existing features: inference endpoints (`.secrets-inference`), connectors (`.connector-secrets`), and Fleet (`.fleet-secrets`). Credentials are submitted via REST API and stored in a dedicated system index with restricted access. Works identically across all deployment modes — no filesystem dependency, no per-node CLI setup. Each datasource type declares its own credential shape (S3: access key + secret key, GCS: service account JSON, Azure: account + key or SAS token). Credentials are never exposed in API responses or log output. Standard cluster privileges (`write_datasource_secrets`, `read_datasource_secrets`) control access.

The connectors implementation (`ConnectorSecretsIndexService`) is ~164 lines and serves as the direct template: system index descriptor, simple CRUD service (store/get/delete by datasource ID), security privileges.

Estimate includes integration work: wiring credential resolution into each storage plugin (S3, GCS, Azure, HTTP) so they read from the secrets index instead of inline parameters. Each plugin already accepts credentials — the work is plumbing the lookup path and handling missing/expired credentials gracefully.

#### Open questions

- Confirm with the security team that the secrets system index pattern is the recommended path for new features requiring per-resource credential storage.

---

### #2 — Datasources: EXTERNAL command syntax

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~2w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

Finalize the `EXTERNAL` command as the production syntax for querying external data sources. The command exists in dev builds as `EXTERNAL "uri"` (querySpec only). This item extends it to support named datasource references, path expressions, and inline configuration overrides — the full syntax needed for production use.

Using a dedicated command (rather than FROM) avoids several complexities: FROM carries ES-specific clauses (METADATA, potentially NULLIFY) that don't apply to external sources, mixing indices and external sources in the same FROM clause raises unresolved cross-join semantics, and a separate command gives us a clean place for configuration parameters (WITH clause). FROM-based table functions remain a long-term option for cross-source queries but are not needed for MVP.

#### Approach

Extend the existing `EXTERNAL` grammar to the full format:

```
EXTERNAL [CONFIG? resourceRef]? querySpec? [WITH { ... }]?
```

- **resourceRef** — unquoted identifier referencing a provisioned datasource (created via CRUD API, #14). Contains the full source configuration (type, bucket, credentials, etc.).
- **querySpec** — quoted string specifying what to read (path, glob, SQL query, topic). Interpretation depends on source type.
- **WITH { ... }** — JSON object overriding specific parameters from the provisioned config. Uses the same structure as the CRUD REST API.

At least one of `resourceRef` or `querySpec` must be present. Resolution logic:
1. `resourceRef` present → load provisioned config by name; use `querySpec` as the query; merge WITH overrides.
2. `resourceRef` absent → infer source type from `querySpec` (URI scheme) or from WITH; querySpec can contain the full spec.
3. `querySpec` absent → the query target is fully defined in the provisioned config.

Examples:
```esql
## Named datasource + path
EXTERNAL s3_prod "sales/2024/data.parquet"
| KEEP emp_no, first_name, salary

## Named datasource only (path in config)
EXTERNAL s3_prod
| KEEP emp_no, first_name, salary

## Inline (no named datasource)
EXTERNAL "s3://my-bucket/sales/data.parquet" WITH {
  "region": "us-east-1",
  "secrets": { "access_key_id": "...", "secret_access_key": "..." }
}
| KEEP emp_no, first_name

## Named datasource + path + overrides
EXTERNAL s3_prod "sales/data.parquet" WITH { "region": "eu-west-1" }
| KEEP emp_no, first_name, salary
```

Parser changes: extend the EXTERNAL lexer mode to accept an optional unquoted identifier (resourceRef) before the existing quoted string (querySpec), and an optional WITH keyword followed by a JSON block. The existing `EXTERNAL "uri"` syntax is a natural subset — no breaking change.

#### Open questions

- How to resolve source type when no `resourceRef` is present and the URI is ambiguous (e.g., `s3://` could be raw Parquet or Iceberg). Likely needs explicit `type` in WITH clause.
- Keyword collision risk for `resourceRef` as unquoted identifier.
- Long-term coexistence with FROM-based table functions — will both syntaxes remain, or will EXTERNAL become an alias?

---

### #3 — Datasources: Unsupported command errors

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~0.5w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Several Lucene-dependent functions and commands crash with opaque errors on external sources instead of producing clear messages. For a Tech Preview, cryptic crashes on common patterns destroy confidence.

**Broken — must produce clear errors:**
- `match()`, `match_phrase()`, `multi_match()` — runtime crash (Lucene DocBlock assertion failure)
- `knn()` — runtime crash (no shard context)
- `score()` — runtime failure (no Lucene scorer)
- `kql()`, `qstr()` — verifier error (already caught, but error message could be clearer)
- `ENRICH _remote` — verifier error
- `LOOKUP JOIN` with external source on right side — hardcoded gate in Mapper
- `TS` / `METRICS` commands — require TIME_SERIES IndexMode, not applicable to external sources
- `RERANK` — Lucene-dependent (same pattern as match)

**Works correctly (no action needed):**
- All processing commands: `WHERE`, `EVAL`, `STATS`, `SORT`, `LIMIT`, `KEEP`, `DROP`, `RENAME`, `DISSECT`, `GROK`, `MV_EXPAND`
- All scalar functions, all operators, `LIKE`/`RLIKE`
- `ENRICH _coordinator/_any`
- `LOOKUP JOIN` with external source on left side
- `INLINESTATS` (Hash Join, not shard-dependent)
- `SAMPLE`, `FORK`, subqueries, async queries

#### Approach

Extend the verifier to check for external sources before execution — the pattern already exists for `kql`/`qstr` (checks `EsRelation` in plan ancestry). Apply the same check to `match`, `match_phrase`, `multi_match`, `score`, `knn`, and `RERANK`. Gate `LOOKUP JOIN` right-side and `ENRICH _remote` with actionable verifier errors. Produce error messages suggesting alternatives where applicable (`LIKE`, `RLIKE`, `contains()`, `starts_with()`, `ends_with()` all work on external sources).

---

### #4 — Datasources: CSV/TSV RFC 4180 compliance

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~0.5w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

CSV and TSV parsing has gaps in RFC 4180 compliance. PR #143779 fixed embedded quote handling and made the schema line parser use a configurable delimiter. **Remaining gap:** `.tsv` files still default to a comma delimiter — `FormatReaderRegistry` returns the same `CsvFormatReader` instance (comma default) for both `.csv` and `.tsv` extensions. Users must specify `WITH {"delimiter": "\t"}` for TSV, which is poor UX.

PR #143779 also added significant new infrastructure: `ErrorPolicy` (FAIL_FAST / SKIP_ROW / NULL_FIELD with error budgets), `CsvFormatOptions` (configurable delimiter, quote, escape, comment prefix, null value, encoding, datetime format, max field size), and `withConfig(Map)` for runtime configuration from the WITH clause.

#### Approach

Register separate format reader configurations for `.tsv` extension — either a second `CsvFormatReader` instance with `CsvFormatOptions.TSV` (tab delimiter), or have `CsvDataSourcePlugin.formatReaders()` register distinct entries for `csv` and `tsv` extensions. The `CsvFormatOptions.TSV` constant already exists but is only used in tests.

---

### #5 — Datasources: Schema union-by-name

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~3w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Real data lakes have schema drift — log files written months apart have different columns. Glob queries fail or return wrong results. This is the normal state of production log data, not an edge case.

#### Approach

The schema resolution enum already declares three strategies (first-file-wins, strict, union-by-name) but only first-file-wins is implemented. Needs: schema sampling from N files, a merge utility with type widening, per-file adapters injecting null blocks for missing columns, and wiring through the format reader interface.

---

### #6 — Datasources: [Native Arrow] Arrow allocator circuit breaker

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~1w
**Dependencies:** #12
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Arrow's native memory allocator (`RootAllocator`) operates outside ES's circuit breaker. Arrow Dataset reads (#10), Flight responses, and Iceberg metadata can allocate unbounded native memory without the JVM knowing.

#### Approach

Bridge Arrow's `AllocationListener.onPreAllocation()` to ES's circuit breaker — the breaker trips before Arrow allocates. Currently `AllocationManagerShim` disables Arrow's allocator in production — must be replaced with a real bridge. The pattern is straightforward: one listener class that calls `CircuitBreaker.addEstimatedBytes()` on pre-allocation and releases on deallocation.

---

### #6a — Datasources: [Parquet-MR] Circuit breaker integration

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~4w
**Dependencies:** #10
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

The parquet-mr columnar reader allocates memory that is invisible to ES's circuit breaker. Unlike Arrow (which exposes `AllocationListener` hooks), parquet-mr has no allocation interception points. Untracked allocations include:

- **Intermediate typed arrays** in `ParquetColumnIterator` — `long[rows]`, `double[rows]`, `boolean[rows]` per column per row group, allocated before being passed to `BlockFactory` (which is CB-aware). A 200-column row group with 100K rows could be ~160MB of untracked heap.
- **parquet-mr internal buffers** — column chunk reads from disk, decompression buffers (Snappy/ZSTD/LZ4). Managed internally by parquet-mr with no external visibility.
- **ORC internal buffers** — same pattern: ORC's own column vectors and decompression buffers are untracked.

Without CB integration, a query over a wide schema with large row groups can OOM the JVM without the breaker ever tripping.

#### Approach

Investigation-first. Possible paths:

- **Wrap our own allocations** — register intermediate typed arrays with CB before allocation. Directly addressable but covers only part of the problem.
- **Estimate from metadata** — use row-group and column-chunk metadata (compressed/uncompressed sizes) to pre-register an estimated memory reservation with CB before reading. Bounded accuracy.
- **Cap row-group read size** — configure parquet-mr to limit row-group batch sizes, bounding the worst case. Trades throughput for safety.
- **Patch or fork parquet-mr** — add allocation hooks internally. Significant maintenance burden.
- **Accept bounded risk** — if parquet-mr internals are bounded by row-group size and row-group size is capped, the untracked allocation is bounded. Document the bound.

ORC has the same gap — any solution should apply to both. The investigation outcome is a direct input to #10 (strategy decision): if no clean CB path exists for parquet-mr, that favors the Arrow path.

---

### #7 — Datasources: JMH microbenchmarks

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~1w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

JMH benchmarks isolating format reader throughput from network I/O. PR #143703 added two benchmarks (CSV error policy, parallel parsing). Still missing: Parquet columnar reader, ORC bulk conversion, NDJSON streaming, and comparative benchmarks across formats for the same dataset.

#### Approach

Extend the existing `ParallelParsingBenchmark` pattern. Create JMH benchmarks for each format reader using local files (no S3/GCS fixture overhead). Measure: rows/sec, bytes/sec, block creation throughput, memory allocation per page. Include benchmarks for column projection (all columns vs subset) and LIMIT pushdown (full scan vs early termination). Use results to establish baselines for regression detection.

---

### #7a — Datasources: Rally track

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~2w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

No Rally track exists for external data sources — zero coverage in `elastic/rally-tracks`. All existing ES|QL benchmarks query Elasticsearch indices only. Without end-to-end benchmarks, we cannot measure real-world query latency, track regressions across releases, or validate performance claims against competing engines.

#### Approach

Create a new Rally track in `elastic/rally-tracks` for ES|QL external data sources. Requires: test datasets in Parquet/NDJSON/CSV on S3 (or local fixture), operations using the `esql` runner type with `EXTERNAL` queries, and measurement of end-to-end latency including object store I/O. Track should cover: single-file scan, glob multi-file scan, filtered scan, aggregation, LIMIT pushdown, and schema-wide vs narrow projection. Rally already has an `Esql` runner class in `esrally/driver/runner.py` — the infrastructure exists, just needs a track.

---

### #9 — ~~[ES|QL] [External Sources] LIMIT pushdown to external sources~~

**Status:** DONE (PR #143515)

`PushLimitToExternalSource` optimizer rule, `ExternalSourceExec.pushedLimit` field, `FormatReader.read(rowLimit)` with `LimitingIterator`, and Parquet `rowBudget` — all merged and working.

---

### #10 — Datasources: Parquet reader strategy investigation

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~1w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)
**Status:** In progress (Oleg)

#### Description

Determine the short-term Parquet reading strategy: extend the current parquet-mr columnar reader (#10a) vs replace with Arrow Dataset (#10b).

The current reader (PR #143703) uses parquet-mr's `ColumnReader`/`ColumnReadStoreImpl` — columnar reads, column projection, and LIMIT pushdown all work. Missing capabilities that need to come from one path or the other:

- Row-group filter pushdown (statistics-based skipping)
- Page index filtering (skip pages within row groups)
- Bloom filter integration (equality predicates)
- Dictionary filtering (row-group skipping by dictionary)
- SIMD-accelerated decoding (Arrow C++ only)
- Async pre-buffering and I/O coalescing (Arrow C++ only)

The parquet-mr path is incremental (each capability is a separate PR) but has a lower ceiling. Arrow Dataset provides all of the above built-in via C++ but adds a 74MB native JAR and JNI complexity. The decision informs which of #10a and #10b to prioritize.

---

### #11 — Datasources: [Parquet-MR] Reader improvements

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~2w
**Dependencies:** #10
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

Fill gaps in the current parquet-mr columnar reader. Each improvement is independently valuable and can be a sub-issue:

- **Row-group filter pushdown** — translate ES|QL filter expressions to parquet-mr `FilterPredicate` objects; use `ParquetFileReader.readFilteredRowGroup()` to skip row groups whose column statistics exclude the predicate. Highest impact: ~10-100x I/O reduction on selective queries over large files.
- **Page index filtering** — use parquet-mr's `ColumnIndex`/`OffsetIndex` APIs to skip individual pages within row groups. Finer-grained than row-group filtering; useful when row groups are large.
- **Bloom filter integration** — use parquet-mr's `BloomFilter` API for equality predicates (`WHERE status = "error"`). Complements statistics-based filtering for high-cardinality columns where min/max is useless.
- **Dictionary filtering** — skip row groups where the column dictionary does not contain the target value. Near-zero cost check before reading column data.
#### Approach

The `FilterPushdownSupport` SPI and `PushFiltersToSource` optimizer wiring already exist. No Parquet `FilterPushdownSupport` implementation is registered — the plugin initialization code never collects it from Parquet plugins. The Iceberg filter converter (`IcebergPushdownFilters`) provides a model for the ES|QL → `FilterPredicate` translation.

Each bullet can land independently as a sub-issue.

---

### #12 — Datasources: [Native Arrow] Arrow Dataset Parquet reader

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~4w
**Dependencies:** #10
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

Replace the parquet-mr reader with the Arrow Java Dataset API (`arrow-dataset`). The C++ engine provides vectorized I/O, column pruning, statistics-based row-group skipping, SIMD-accelerated decoding, async pre-buffering, and I/O coalescing — all built-in. Four areas of work:

1. Add the `arrow-dataset` dependency (74MB JAR with bundled native libs for Linux x64/aarch64, macOS x64/aarch64, Windows)
2. Bridge Arrow's memory allocator to ES circuit breaker (overlaps with #6)
3. Wire the Arrow Dataset Scanner as the Parquet format reader — column projection via `ScanOptions.columns()`, filter pushdown via `ScanOptions.substraitFilter()`
4. Convert Arrow vectors to ES|QL blocks (Arrow-to-Block conversion already exists in `ArrowToBlockConverter`)

#### Risks

- Native lib loading in ES's classloader/security environment. The pattern exists (libvec, zstd-jni) but is untested for Arrow's `JniLoader`.
- 74MB JAR size increase to the distribution.
- Arrow Java Dataset API stability — marked "early development" (applies to Java API surface, not C++ backend).

---

### #13 — Datasources: Metadata inference cache

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~3w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Every query re-reads file metadata (Parquet footers, schema, row-group statistics) from the object store over the network. In dashboard scenarios (same query every 30s), this adds hundreds of milliseconds per query for work that produces identical results. Metadata changes infrequently — hours or days for most data lake use cases.

#### Approach

Cache at the block store layer — every metadata read from S3/GCS/Azure gets cached, keyed by file path + ETag/last-modified. This is not "schema inference" (the schema is known, not guessed) — it's caching submitted metadata to avoid redundant network reads. The cache is persisted locally in Elasticsearch via a system index (e.g., `.esql-metadata-cache`), so it survives node restarts and is shared across nodes in the cluster. TTL-based invalidation with configurable retention. Cluster settings for max entries and TTL.

---

### #14 — Datasources: CRUD API

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~2w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Without persistent definitions, every query must inline URIs, storage type, and credentials — unreadable, insecure, unshareable.

#### Approach

Standard ES CRUD service — `PUT/GET/DELETE /_datasource/{name}`, cluster state persistence as project-scoped custom metadata. Follows existing patterns (views, transforms). Independent of #1 (secrets) — either works without the other; when both exist, datasources reference stored credentials by convention. Independent of #2 (syntax) — the syntax defines how queries reference datasources by name; CRUD defines how those names are persisted. Either can land first.

---

### #16 — Datasources: Schema discovery API

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~1w
**Dependencies:** #14
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Powers Kibana autocomplete for external datasources. Without it, users must memorize field names.

#### Approach

New REST endpoint (`GET /_esql/datasource/{name}/_schema`) modeled after `_field_caps`. Resolves schema via the existing metadata resolution chain and returns field names, types, and partition annotations. Uses metadata inference cache (#13) when available.

---

### #17 — Datasources: Byte-based memory safety for source buffers

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~2w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

External source queries can consume unbounded memory and crash the JVM. The async source buffer is hardcoded to 10 pages with no byte-based accounting — a 100-column page uses far more memory than a 3-column page but both count as "1".

#### Approach

First step: audit what the external source pipeline currently tracks and what it doesn't — identify every allocation point (source buffers, format reader internals, decompression buffers, block builders) and whether each is wired to the circuit breaker. The Arrow allocator bridge is a separate item (#6); this issue covers everything else. The audit may reveal the gap is smaller than expected if most allocations already flow through tracked block builders.

Based on audit findings: add byte-based tracking to the async source buffer, wire the circuit breaker from the driver context, and make buffer limits configurable (page count, byte limit, drain timeout).

---

### #18 — Datasources: Thread pool isolation

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~1w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

External source I/O shares the SEARCH thread pool with regular search operations. Heavy external queries can starve regular ES operations.

#### Approach

Dedicated thread pool for external source I/O, following the existing `esql_worker` pool pattern. Register a new named pool, update operator factories to use it instead of SEARCH.

---

### #19 — Datasources: Distributed execution hardening

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~2w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

The distributed execution framework merged recently and needs exercising under realistic failure conditions before GA. `ExternalDistributedSpecIT` already runs on a real 2-node cluster with all three distribution modes (adaptive, round-robin, weighted round-robin), exercising the real `DataNodeComputeHandler`. What's missing is failure and stress testing.

#### Approach

Extend existing integration tests with: node failure mid-query (kill a data node during split processing), 1000+ splits across a small cluster (stress the split assignment and exchange infrastructure), heterogeneous split sizes (verify no straggler effects), and partial result handling under failure.

---

### #21 — ~~[ES|QL] [External Sources] Two-phase distributed aggregation~~

**Status:** DONE (PR #143696)

ExternalRelation is now wrapped in FragmentExec, so Aggregate (INITIAL/FINAL), TopN, and Limit are all distributed to data nodes automatically via the same ExchangeExec mechanism used by ES indices.

---

### #22 — Datasources: Pricing & licensing definition

**Labels:** `Team:ES Analytical Engine`
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

*(No detail provided in source document.)*

---

### #23 — ~~[ES|QL] [External Sources] Vectorized format readers~~

**Status:** DONE (PR #143703)

Columnar Parquet reading (ColumnReadStoreImpl/ColumnReader), bulk ORC conversion (ColumnBlockConversions), parallel text parsing (ParallelParsingCoordinator + SegmentableFormatReader for CSV/NDJSON), and positional ByteBuffer I/O. JMH benchmarks show 63-67% improvement for text formats.

---

### #24 — Datasources: Intra-file parallelism

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~1w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)


#### Description

Reading a single file using multiple concurrent workers. PR #143703 built the infrastructure for text formats: `SegmentableFormatReader` (implemented by CSV and NDJSON), `ParallelParsingCoordinator` (segment splitting, ordered reassembly), and positional I/O via `StorageObject.readBytes()`. However, `parallelRead()` is never called from production code — it needs wiring into `AsyncExternalSourceOperatorFactory`. Parquet row-group parallelism is completely unimplemented.

#### Approach

Two remaining pieces:
- **Text formats (CSV, NDJSON):** Wire `ParallelParsingCoordinator.parallelRead()` into the production operator factory. The coordinator, segment splitting, and ordered reassembly all exist and are tested — this is plumbing.
- **Parquet:** Extend split discovery to produce sub-file splits at row-group granularity. Use row group metadata to create one split per row group. The columnar reader (`ParquetColumnIterator`) already reads one row group at a time — the change is making each row group a separately schedulable unit.

---

### #25 — ~~[ES|QL] [External Sources] Distributed planning and execution~~

**Status:** DONE (PR #143696)

Full distributed pipeline: coordinator planning → split discovery → distribution strategy → data node dispatch → local plan expansion → compute with splits → exchange → coordinator reduction. All distribution strategies (adaptive, round-robin, weighted round-robin, coordinator-only) work. All three pipeline breakers (Aggregate, TopN, Limit) distributed automatically.

---

### #26 — Datasources: Column pruning for external sources

**Labels:** `Team:ES Analytical Engine`
**Estimate:** ~0.5w
**Dependencies:** None
**Parent:** [#189](https://github.com/elastic/esql-planning/issues/189)

#### Description

The `FormatReader.read()` SPI accepts a `projectedColumns` list and the Parquet columnar reader does true projection via `buildProjectedSchema()` — only requested columns are read from disk. However, the `PruneColumns` optimizer rule does not handle `ExternalRelation` (falls through to the `default -> p` no-op in its switch), so the attribute list is never narrowed. Every query reads all columns from storage even when only a few are needed.

This affects all format readers, not just Parquet. For columnar formats (Parquet, ORC), the impact is 5-20x I/O reduction on wide schemas. For row-oriented formats (NDJSON, CSV), rows are still fully parsed but unwanted columns are skipped during block materialization, saving memory.

#### Approach

Add an `ExternalRelation` case to the `PruneColumns` optimizer rule. Use `ExternalRelation.withAttributes()` to narrow the attribute list to only the columns referenced downstream. The plumbing from `ExternalSourceExec.attributes` through to `FormatReader.read(projectedColumns)` already works — the gap is purely in the optimizer.

---

## Unresolved

> **Tyler Perkins** on Post-MVP docs: *Please share these when you get a chance*
> Action: share post-mvp-planning.md and post-mvp-product-shape.md

---

## Attribute Legend

| Attribute | Values |
|-----------|--------|
| **Est** | H+AI–Human estimate range. H+AI = with AI coding assistant, Human = experienced dev alone |
| **Area** | func (functionality), perf (performance), correct (correctness), stab (stability), scale (scalability), sec (security) |
| **Size** | S (couple of days), M (1-2 weeks), L (2+ weeks) |
