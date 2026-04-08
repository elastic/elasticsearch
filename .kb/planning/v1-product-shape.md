# ES|QL Data Sources — Full Scope

**Tracking:** [elastic/esql-planning#189](https://github.com/elastic/esql-planning/issues/189) (MVP) | [#302](https://github.com/elastic/esql-planning/issues/302) (Post-MVP)
**Timeline:** GA targets Elasticsearch 9.5 (Q2 FY27, June 2026)

**Scope:** Primary formats are Parquet and NDJSON.gz. Storage: S3, GCS, Azure. ORC and CSV work out of the box but are not the focus. HTTP and Flight connectors are disabled for the MVP.

**Attribute legend:**

| Attribute | Values |
|-----------|--------|
| **Est** | H+AI–Human estimate range. H+AI = with AI coding assistant, Human = experienced dev alone |
| **Area** | func (functionality), perf (performance), correct (correctness), stab (stability), scale (scalability), sec (security) |
| **Crit** | P0 (blocker — must ship), P1 (important — should ship), P2 (nice-to-have) |
| **Deps** | Other items that must complete first |

---

## MVP Tech Preview

Functionality-focused. Features work end-to-end but may have gaps in stability, performance, and scale. The goal is a credible demo: register a datasource, query Parquet/NDJSON on S3 from ES|QL, get results.

**Total:** ~13w–25.5w
**With 3 engineers:** ~4.3w–8.5w

| # | Item | Area | Crit | Deps | Est | Impact |
|---|------|------|------|------|-----|--------|
| [TP-3](#tp-3-secret-management) | Secret management | func | P0 | — | 1w–2w | Secrets system index — no plaintext credentials in queries; works across all deployment modes |
| [TP-2](#tp-2-datasource-crud-api) | Datasource CRUD API | func | P0 | — | 1w–2w | Persistent named datasources — no more inlining URIs and credentials per query |
| [TP-1](#tp-1-external-command-syntax) | EXTERNAL command syntax | func | P0 | — | 4d–1.5w | Finalize production EXTERNAL syntax, remove dev gate |
| [TP-4](#tp-4-limit-pushdown) | ~~LIMIT pushdown~~ **DONE** | perf | P0 | — | ~~1d–2d~~ | Merged: `PushLimitToExternalSource` optimizer rule (#276, PR #143515) |
| [TP-5](#tp-5-arrow-dataset-parquet-reader) | Arrow Dataset Parquet reader | perf | P0 | — | 2w–4w | 10-50x Parquet read perf via C++ vectorized I/O; column projection free |
| [TP-6](#tp-6-row-group-filter-pushdown) | Row-group filter pushdown (Substrait) | perf | P0 | TP-5 | 1.5d–3d | Timestamp range skips 90%+ of row groups; I/O reduction compounds with partition pruning |
| [TP-9](#tp-9-schema-inference-cache) | Schema inference cache | perf | P1 | — | 4d–1.5w | Repeated queries skip metadata I/O; dashboard scenarios 10x faster |
| [TP-12](#tp-12-unsupported-command-errors) | Unsupported command errors | correct | P0 | — | 1d–2d | match/knn/score produce helpful errors instead of crashes |
| [TP-7](#tp-7-csvtsv-bug-fixes-rfc-4180) | ~~CSV/TSV bug fixes (RFC 4180)~~ **DONE** | correct | P1 | — | ~~1d–2d~~ | Merged: #275 — correct delimiter, quote escaping, ErrorPolicy |
| [TP-8](#tp-8-schema-union-by-name) | Schema union-by-name | correct | P1 | — | 1.5w–3w | Glob queries across files with different schemas produce correct results |
| [TP-10](#tp-10-circuit-breaker-memory-control) | Circuit breaker memory control (**partially DONE**) | stab | P0 | — | 1w–2w | Byte-based buffer backpressure merged (#289, PR #144218). Arrow allocator bridge and format reader internals still open. |
| [TP-11](#tp-11-loadperformance-testing-framework) | Load/performance testing framework | stab | P0 | — | 1w–2w | JMH + Rally benchmarks; regression detection for format readers |
| [TP-13](#tp-13-bug-fix--stabilization-buffer) | Bug fix & stabilization buffer | stab | P0 | TP-11 | 1w–2w | Buffer for bugs found during load testing and integration |

Delivers the core user-facing feature: users register external datasources via a REST API with credentials stored in a secrets system index, then query Parquet and NDJSON.gz files on S3, GCS, and Azure using the `EXTERNAL` command. LIMIT pushdown enables fast exploratory queries without full dataset scans. Arrow Dataset Parquet reader with Substrait row-group filter pushdown provides competitive analytical performance (10-50x over current). Schema is cached across queries (TTL-based) and supports union-by-name for schema drift across files. CSV/TSV bug fixes ensure data integrity. Circuit breaker integration prevents OOM from untracked source buffers. ORC works out of the box but is not the optimization focus.

**Market coverage:** S3 + GCS + Azure covers ~80-90% of cloud data lakes. Parquet (~80% of optimized log storage) + NDJSON (~80-90% of raw log data — every major shipper defaults to it) covers ~95%+ of log formats. ORC (~10-15%, legacy Hadoop) and CSV/TSV are also readable. This is the core observability entry point: ~65% of enterprises have or are building a data lake, and the dominant use case is querying log data that's already landing on cloud storage.

**Gaps:** No Iceberg. No Kibana UI integration (syntax highlighting, management UI). No heterogeneous queries (ES + external in same query). No thread pool isolation (external I/O shares `GENERIC` pool). HTTP and Flight connectors disabled.

#### TP-1. EXTERNAL command syntax

**GitHub issue:** [elastic/esql-planning#251](https://github.com/elastic/esql-planning/issues/251)

**Current State:** The `EXTERNAL "uri"` command already works end-to-end in snapshot builds — gated behind `isDevVersion()` at both lexer and parser level. Example: `EXTERNAL "s3://bucket/path" WITH { "access_key": "..." }` resolves metadata, reads data, and returns results today.

**Target syntax:**
```esql
-- Direct URI:
EXTERNAL "s3://bucket/logs/*.parquet"

-- Named datasource (after CRUD API):
EXTERNAL my_s3_logs "*.parquet"

-- With parameters:
EXTERNAL "s3://bucket/logs" WITH { "format": "ndjson" }

-- In a pipeline:
EXTERNAL "s3://bucket/logs/2026-03/**/*.parquet" | WHERE level == "ERROR" | LIMIT 100
```

**What to change:** Finalize the `EXTERNAL` command as the production syntax. Remove the `isDevVersion()` gate so the command is available in production builds. Extend to support named datasource references (created via CRUD API), path expressions, and inline configuration overrides via `WITH` clause.

Since `EXTERNAL` has never appeared in a GA release, there are zero backward-compatibility concerns.

**User Impact:** The `EXTERNAL` command works but is dev-gated — users on production builds cannot access it. This item removes the gate and finalizes the production syntax. It's the entry point for everything else.

**Estimate:** 4d–1.5w

---

#### TP-2. Datasource CRUD API

**Current State:** No persistent datasource definitions. Config flows purely at query time from the `WITH` clause as plaintext `Map<String, Object>`. No cluster-state storage. No `_datasource` API. Zero existing datasource CRUD infrastructure. All four storage plugins (S3, GCS, Azure, HTTP) receive credentials as plaintext strings extracted from the query's `WITH` clause — there is zero keystore integration in the ESQL datasource path.

**Target syntax:**
```json
PUT /_datasource/my_s3_logs
{
  "type": "s3",
  "uri": "s3://my-bucket/logs/",
  "settings": {
    "region": "us-east-1",
    "format": "parquet"
  }
}

GET /_datasource/my_s3_logs
DELETE /_datasource/my_s3_logs
```

**What to change:** Build a standard ES CRUD API following the `ViewMetadata`/`ViewService` pattern — modern, uses cluster-state-persisted metadata with `AbstractNamedDiffable` and `SequentialAckingBatchedTaskExecutor`. A `DataSourceMetadata` object persists datasource definitions (name, type, URI pattern, settings, credential references) in cluster state as `Metadata.ProjectCustom`. REST handlers expose `PUT/GET/DELETE /_datasource/{name}`. Transport actions follow the `AcknowledgedTransportMasterNodeAction` pattern. ~12 new files.

**Resolution flow:** `EXTERNAL my_s3_logs "*.parquet"` → parser sees `my_s3_logs` → resolver looks up `DataSourceMetadata` from cluster state → merges stored config with expression → dispatches to appropriate factory.

**Relationship to TP-3 (secrets):** Independent. The CRUD API provides named datasources; secret management provides secure credential storage. Either works without the other. When both exist, named datasources can reference stored credentials by convention.

**User Impact:** Without this, every query must inline full URIs and configuration. Unusable in production — no sharing across users, no management.

**Estimate:** 1w–2w

---

#### TP-3. Secret management

**Current State:** ESQL datasource plugins have zero keystore integration — all four storage plugins receive credentials as plaintext strings extracted from the query's `WITH` clause.

**Product impact:** Production deployments need secure credential storage. Without this, credentials must be inlined in queries — visible in logs, audit trails, and slow query reports. The mechanism must work consistently across self-managed, Cloud, and stateless deployments.

**Approach:** Secrets system index (`.secrets-datasource`), following the established pattern used independently by three existing features: inference endpoints (`.secrets-inference`), connectors (`.connector-secrets`), and Fleet (`.fleet-secrets`). Credentials are submitted via REST API and stored in a dedicated system index with restricted access. Works identically across all deployment modes — no filesystem dependency, no per-node CLI setup. Each datasource type declares its own credential shape (S3: access key + secret key, GCS: service account JSON, Azure: account + key or SAS token). Credentials are never exposed in API responses or log output. Standard cluster privileges (`write_datasource_secrets`, `read_datasource_secrets`) control access.

The connectors implementation (`ConnectorSecretsIndexService`) is ~164 lines and serves as the direct template: system index descriptor, simple CRUD service (store/get/delete by datasource ID), security privileges.

**Open question:** Confirm with the security team that the secrets system index pattern is the recommended path for new features requiring per-resource credential storage.

**Estimate:** 1w–2w

---

#### TP-4. LIMIT pushdown — DONE

**Status:** Merged ([#276](https://github.com/elastic/esql-planning/issues/276), PR #143515).

`PushLimitToExternalSource` optimizer rule pushes `rowLimit` into `ExternalSourceExec`. `FormatReader.read()` accepts `rowLimit` parameter. `LimitingIterator` trims final page to exact count. `AdaptiveStrategy` keeps LIMIT-only queries local (`isLimitOnly → LOCAL`).

**Original estimate:** 1d–2d

---

#### TP-5. Arrow Dataset Parquet reader

**Current State:** The Parquet reader uses parquet-mr's `GroupRecordConverter` — the "example" record-oriented API from the parquet-mr documentation, intended for demos rather than production. It reads row-by-row, deserializing each record into a `Group` object, then converts Groups to ESQL blocks one value at a time. This is the slowest possible path through parquet-mr: intermediate `Group` allocation, no column-level access, no vectorized reads. There is no row-group filter pushdown and no real column projection.

**Approach:** Replace the current parquet-mr row-oriented reader with the Arrow Java Dataset API (`arrow-dataset` module), which provides C++-backed vectorized Parquet reads. This gives competitive read performance (10-50x over current) and built-in support for column projection and Substrait filter pushdown — avoiding throwaway work on a parquet-mr intermediate step.

**What to change:**
1. **Arrow Dataset dependency integration** — add `arrow-dataset` (74MB JAR with bundled native libs for Linux/macOS/Windows, x64/aarch64). `JniLoader` extracts and loads them at runtime — same pattern as zstd-jni and sqlite-jdbc.
2. **Arrow memory allocator bridge** — replace `AllocationManagerShim` with a real Arrow `RootAllocator` backed by ES circuit breaker via `AllocationListener.onPreAllocation()`. Overlaps with TP-10.
3. **FormatReader SPI wiring** — wire `FileSystemDatasetFactory` as the Parquet reader behind the format reader SPI. Arrow Dataset Scanner natively supports column projection (parameter) and row-group skipping.
4. **Arrow-to-Block conversion** — convert Arrow vectors to ES|QL compute engine blocks. ES already has Arrow vector types on the classpath (arrow-vector, arrow-format, arrow-memory-core) for query result serialization.

**What comes for free:** Column projection (previously a separate item — now a Scanner parameter, no separate work needed). Dictionary encoding exploitation. Page-level column statistics. ORC reading improvement (Arrow Dataset reads ORC too). This also absorbs the Post-MVP Arrow Dataset reader — that work is now front-loaded here instead of deferred.

**What this does NOT cover:** Row-group filter pushdown via Substrait (TP-6 — requires ES|QL → Substrait filter translation). Page-level filtering beyond statistics. Zero-copy Arrow output.

**Native library precedent:** ES already ships platform-specific native libraries: `libvec` (SIMD vector ops for KNN) and `libzstd` (Zstandard compression) are distributed as `.so`/`.dylib`/`.dll` files in `lib/platform/<os>-<arch>/`, loaded at runtime via `LoaderHelper.java`. `zstd-jni` bundles native libraries inside the JAR itself — same pattern as `arrow-dataset`. As of version 18.3.0, **`arrow-dataset` publishes pre-built JNI native libraries inside the JAR on Maven Central** — the 74MB JAR bundles `.so` (Linux), `.dylib` (macOS), and `.dll` (Windows) for both x86_64 and aarch64.

**Risk:** Native lib loading in ES's classloader/security environment. The pattern exists (libvec, libzstd, zstd-jni) but is untested for Arrow's JniLoader. This is the main integration risk.

**User Impact:** Parquet is the dominant format for analytical data. Row-by-row reading makes any benchmark uncompetitive with DuckDB, Trino, or ClickHouse. Arrow Dataset puts us on the same engine used by DataFusion, DuckDB, and Polars.

**Estimate:** 2w–4w. Critical path item.

---

#### TP-6. Row-group filter pushdown (Substrait)

**Current State:** The Parquet reader reads all row groups unconditionally. The L2 filter infrastructure exists at the SPI level: `FilterPushdownSupport` interface with `pushFilters()` returning `PushdownResult`, the `PushFiltersToSource` optimizer rule handles external sources, `FilterPushdownRegistry` collects per-format support, and `ExternalSourceExec` carries a `pushedFilter()` field. However, there's a wiring gap: the plugin module collects filter pushdown support from plugins but the registration path to the optimizer isn't fully connected. Iceberg has a reference implementation (`IcebergPushdownFilters`, 24 test methods) that converts all comparison operators + AND/OR/NOT/IsNull — it's the direct template.

**Approach:** Arrow Dataset Scanner (from TP-5) accepts Substrait filter expressions and handles row-group skipping internally using column statistics (min/max) from the Parquet footer. The work here is: (1) fix the wiring gap so filter pushdown support is registered in the optimizer, and (2) translate ES|QL filter expressions into Substrait predicates that Arrow Dataset can consume. The Iceberg filter converter provides the pattern.

**TP scope:** Comparison filters only (=, !=, <, >, <=, >=). Logical operators (AND, OR, NOT), IN, IS NULL are passed as remainder to the engine. This covers the dominant use case: `WHERE @timestamp > '2026-01-01'` skips old row groups.

**User Impact:** After L1 partition pruning removes most files, this skips irrelevant chunks within the remaining files. For a 10GB Parquet file with 100 row groups, a date range filter might skip 90 row groups — reading 1GB instead of 10GB.

**Estimate:** 1.5d–3d

---

#### TP-7. CSV/TSV bug fixes (RFC 4180)

**Current State:** The CSV reader (Jackson `jackson-dataformat-csv` 2.15.0) has three bugs that are all **configuration errors**, not missing functionality. (1) `.withEscapeChar('\\')` overrides Jackson's correct default (`CsvSchema.NO_ESCAPE_CHAR`) — RFC 4180 uses `""` for double-quote escaping, not backslash. (2) `.withColumnSeparator(',')` is hardcoded — `.tsv` is registered as a supported extension but all parsing is comma-delimited, making TSV completely non-functional. (3) `schemaLine.split(",")` hardcodes comma for parsing the typed header (`column:type,...`), breaking TSV schema lines. Jackson CSV already supports everything needed: `withColumnSeparator(char)`, `withQuoteChar(char)`, `withEscapeChar(char)`.

**What to change:** Three targeted fixes — all Jackson parser configuration changes, no new code logic: (1) remove the `.withEscapeChar('\\')` call entirely (Jackson's default handles RFC 4180 correctly); (2) pass delimiter from the file extension (`.tsv` → `'\t'`, `.csv` → `','`) to both `CsvSchema.builder().setColumnSeparator()` and schema parsing; (3) replace `schemaLine.split(",")` with `schemaLine.split(delimiter)`. Add tests for embedded quotes, TSV files, and mixed CRLF/LF.

| Bug | Standard | Current | Fix |
|-----|----------|---------|-----|
| Delimiter | Configurable | Hardcoded comma | Pass from extension |
| Quote escaping | `""` (double-quote) | `\\` (backslash) | Remove override — Jackson default is correct |
| Schema parsing | Match data delimiter | Hardcoded comma | Use same delimiter |

**User Impact:** TSV files are silently corrupted. CSV files with embedded quotes fail. These are data integrity bugs.

**Estimate:** 1d–2d

---

#### TP-8. Schema union-by-name

**Current State:** The `SchemaResolution` enum defines three strategies — `FIRST_FILE_WINS` (implemented), `STRICT` (TODO stub), and `UNION_BY_NAME` (TODO stub). The default always returns `FIRST_FILE_WINS` and is never called by production code — completely disconnected from the multi-file resolver. Multi-file resolution reads schema from the first file only. The NDJSON inferrer has within-file type widening (priority: DATETIME > KEYWORD > DOUBLE > LONG > INTEGER). Per-file adaptation at read time doesn't exist: the same projected columns are passed to all files. Parquet handles missing columns gracefully (returns null blocks), but CSV throws an exception for missing columns.

**What to change:** When union-by-name is selected (via `WITH schema_resolution = 'union_by_name'`), sample N files (configurable, default 10) and resolve schema from each. A new `SchemaMerger` class merges the field lists: union of all field names, type widening for conflicts (int→long→double→keyword), missing fields marked nullable. A `SchemaAdaptingIterator` wraps per-file page iterators to inject null blocks for missing columns and apply type widening. CSV's missing-column exception must be changed to return null blocks instead.

**Why TP (not later):** Real data lakes have schema drift — log files written weeks apart have different columns. Without union-by-name, a glob query over `s3://logs/2026-*/*.parquet` fails or returns wrong results if files have different schemas. This is the normal state of production log data, not an edge case.

**User Impact:** Glob queries across files with different schemas fail or silently drop columns. Common in log analytics where schema evolves over time.

**Estimate:** 1.5w–3w

---

#### TP-9. Schema inference cache

**Current State:** No caching of any kind — zero memoization, TTL cache, or result caching for schema metadata. The resolver is created fresh per query. Every query re-reads file metadata over the network. Elasticsearch has a proven `Cache<K,V>` class (256-segment concurrent, LRU eviction, `expireAfterAccess`, `expireAfterWrite`, `maximumWeight`, `computeIfAbsent`) used by API key service, script cache, and others.

**What to change:** Add a schema cache (thin wrapper around ES's `Cache<String, SourceMetadata>`, ~80 lines) as a singleton per node. The cache is checked at the top of the resolve path before factory iteration. Cache key is path-only — credentials don't affect schema. TTL default 5 minutes, configurable via cluster settings. ~340 lines total (production + test).

**User Impact:** Repeated queries against the same datasource re-read metadata every time. Dashboard scenarios (same query every 30 seconds) pay unnecessary I/O latency on every execution.

**Estimate:** 4d–1.5w

---

#### TP-10. Circuit breaker memory control — PARTIALLY DONE

**Status:** Byte-based buffer backpressure merged ([#289](https://github.com/elastic/esql-planning/issues/289), PR #144218). Arrow allocator bridge and format reader internals remain open.

**What's done:**
1. **(DONE) Byte-based buffer cap:** `AsyncExternalSourceBuffer` now tracks bytes via `ramBytesUsedByBlocks()` in addition to page count. Backpressure triggers on both thresholds.

**What remains:**
2. **(Open) Arrow `AllocationListener` bridge (#281):** Flight's `RootAllocator` is still created with `Long.MAX_VALUE` limit. #281 was closed prematurely — code only in unmerged PR #142981.
3. **(Defer) Format reader internals:** Parquet/ORC library allocations bypass circuit breakers. The Arrow Dataset reader (TP-5) would address Parquet via Arrow's allocator; ORC's pre-allocated batch is bounded by batch size.

**User Impact:** Buffer-level OOM risk addressed. Arrow/library-level OOM risk remains.

**Original estimate:** 1w–2w

---

#### TP-11. Load/performance testing framework

**Current State:** No format reader or external source benchmarks — no JMH macrobenchmarks, no Rally tracks, no load tests for external sources. All existing tests are purely functional. However, substantial infrastructure exists to build on:

- **Rally has ES|QL support** (operation type added in Rally 2.10.0). An existing `esql/` rally track in `elastic/rally-tracks` tests queries against indexed ES data with 20 index mappings. It does NOT test external data sources.
- **In-process HTTP fixtures** for S3/GCS/Azure with request logging (`S3RequestLog` records request type, path, content length, timestamp) and fault injection (`FaultInjectingS3HttpHandler` with 503/500/connection-reset). Lightweight but not representative of production latency.
- **MinIO Docker fixture** exists in `test/fixtures/minio-fixture/` but is only used by repository-s3 tests, not ESQL external sources.
- **JMH microbenchmarks** (10+ benchmarks in `benchmarks/`) with nightly CI runner, including 3 ESQL-specific: query planning, TopN, field extraction. All pure compute — zero I/O.
- **ESQL profile data** available via `profile: true` response parameter.

**What to change:** Two tracks:

1. **Rally track for external sources** (new `esql-external/` track): Defines `esql` operations with datasource queries, parameterized by storage/format/query pattern. MinIO container alongside Rally runner for S3. Pre-generated Parquet/NDJSON corpora at scale (100MB, 1GB, 10GB). Measures throughput (queries/sec), latency percentiles (p50/p90/p99), error rates.

2. **JMH microbenchmarks** (in `benchmarks/_nightly/esql/`): Format reader throughput benchmarks (Arrow Dataset Parquet reader, NDJSON reader, Arrow-to-block conversion). Uses existing JMH infrastructure and `run.sh` nightly runner.

Neither Gatling nor k6 are used anywhere in the repository — Rally is the sole macrobenchmarking tool.

**User Impact:** No direct user impact — engineering infrastructure. Without it, performance regressions ship undetected and we can't make credible performance claims for the Tech Preview.

**Estimate:** 1w–2w

---

#### TP-12. Unsupported command errors

**Area:** correct | **Crit:** P0 | **Deps:** —

**Current State:** Several ES|QL commands that depend on Lucene internals crash with opaque errors when used with external data sources. The failure modes are:

| Command | Failure mode | Code path |
|---------|-------------|-----------|
| match, match_phrase, multi_match | Runtime crash — `FullTextFunction.toEvaluator()` (`FullTextFunction.java:444-445`) calls `toShardConfigs(toEvaluator.shardContexts())`. External sources return `EmptyIndexedByShardId.instance()`. `LuceneQueryEvaluator` constructor (`LuceneQueryEvaluator.java:58`) asserts `shards.isEmpty() == false`. With assertions off, searches for `DocBlock` in page — NPE | `FullTextFunction.java:444` → `LuceneQueryEvaluator.java:58,73` |
| kql, qstr | Verifier error — `FullTextFunction.java:257-267` checks `lp instanceof EsRelation` in plan ancestry. `ExternalRelation` doesn't match → clean error | `FullTextFunction.java:263` |
| score() | Runtime crash — `Score.toEvaluator()` (`Score.java:90-92`) calls `ScoreMapper.toScorer()` (`ScoreMapper.java:23-38`) which passes empty `shardContexts` to child `FullTextFunction.toScorer()` (`FullTextFunction.java:460-461`) → same crash path | `Score.java:91` → `FullTextFunction.java:460` |
| knn() | Runtime crash — inherits `FullTextFunction.toEvaluator()` (same path as match). Additionally, `evaluatorQueryBuilder()` (`Knn.java:280-288`) creates `ExactKnnQueryBuilder` requiring Lucene `IndexSearcher` | `FullTextFunction.java:444` + `Knn.java:280` |
| ENRICH _remote: | Verifier error — `Enrich.java:284` checks `ExecutesOn.Coordinator` in plan descendants. `ExternalRelation` implements `ExecutesOn.Coordinator` → error: "ENRICH with remote policy can't be executed after [EXTERNAL ...]" | `Enrich.java:284` |
| CCS + EXTERNAL | `UnsupportedOperationException` — `ExternalRelation.writeTo()` (`ExternalRelation.java:74-76`) throws "ExternalRelation is not yet serializable for cross-cluster operations". Intentional guard — external sources are network resources, not cluster-local data, so CCS is not a meaningful operation for them | `ExternalRelation.java:74` |

**What to change:** Extend the verifier to check for external sources before execution for ALL full-text functions and knn(). The existing pattern for kql/qstr (`FullTextFunction.java:257-267`) already checks for `EsRelation` in plan ancestry — extend it to cover `match`, `match_phrase`, `multi_match`, `score`, and `knn` when the plan ancestry includes `ExternalRelation`. Produce actionable error messages:

> `match() requires an Elasticsearch index. For text filtering on external data sources, use LIKE, RLIKE, contains(), starts_with(), or ends_with().`

Six changes: (1) Add external-source checks to the full-text function verification pass for match/match_phrase/multi_match. (2) Add external-source check for knn(). (3) Add external-source check for score(). (4) Improve the existing kql/qstr error message to suggest alternatives. (5) Improve the ENRICH _remote: error message to explain the coordinator-only limitation. (6) **Block CCS + EXTERNAL** — replace the `UnsupportedOperationException` guard in `ExternalRelation.writeTo()` with a verifier-level check that produces a clear error before execution (e.g., "External data sources cannot be used in cross-cluster queries. Run the EXTERNAL query on the cluster that has network access to the source.").

~~(7) Block EXTERNAL in views~~ — **REMOVED**. Deep dive on 2026-03-17 confirmed the pipeline ordering is correct: view inlining happens BEFORE pre-analysis and external source resolution. Views with EXTERNAL should work. See #373 for test coverage.

**What works and doesn't change:** LIKE, RLIKE (use `AutomataMatch` pure-compute evaluator on `BytesRef` blocks), contains(), starts_with(), ends_with() (all pure-compute string evaluators) — these work correctly on external data today and are the recommended alternatives.

**Competitive context:** This is not a feature gap — it is a trust gap. Every analytics engine has operations that don't work on every source type. What differentiates a quality product is whether the user gets a helpful error message or a stack trace. For a Tech Preview targeting early adopters, a cryptic crash on `| WHERE match(message, "error")` — a query pattern every Elasticsearch user tries first — would immediately damage confidence. The fix is small.

**User Impact:** Users get crashes instead of error messages. P0 for Tech Preview credibility.

**Estimate:** 1d–2d

---

#### TP-13. Bug fix & stabilization buffer

**Area:** stab | **Crit:** P0 | **Deps:** TP-11

**Current State:** Not applicable — this is reserved buffer time, not a defined deliverable.

**What this covers:**
- Bugs found during TP-11 load/performance testing (the most likely source — testing exists to find problems, and problems need time to fix)
- Integration bugs across CRUD API + syntax + format readers + cloud storage authentication
- Edge cases in schema inference, type mapping, and multi-file queries
- Performance regressions discovered during development
- Correctness issues in compression codec combinations (ZSTD/BZIP2/GZIP × Parquet/NDJSON/CSV)

**What this does NOT cover:** New features, scope expansion, or items deferred from TP. Strictly for fixing what's broken in the items that are already scoped.

**User Impact:** Without buffer, either bugs ship unfixed (damaging TP credibility) or other items get silently cut (damaging scope commitments). Every milestone surfaces unexpected issues — allocated buffer time makes this explicit rather than hidden.

**Estimate:** 1w–2w

---

## MVP GA

Production-ready. What's included must be stable, performant, and safe. Stability hardening, Kibana integration, critical query optimizations, and the schema discovery API that powers Kibana autocomplete.

**Total:** ~6w–10.5w (excl. Kibana)
**With 3 engineers:** ~2w–3.5w

| # | Item | Area | Crit | Deps | Est | Impact |
|---|------|------|------|------|-----|--------|
| [GA-5](#ga-5-kibana-datasource-integration) | Kibana datasource integration | func | P0 | TP-2, GA-6 | *(Kibana)* | Datasource management UI, syntax highlighting, autocomplete |
| [GA-6](#ga-6-schema-discovery-api) | Schema discovery API | func | P0 | TP-2 | 4d–1.5w | REST endpoint for field names/types; powers Kibana autocomplete |
| [GA-1](#ga-1-thread-pool-isolation) | Thread pool isolation | stab | P0 | — | 3d–1w | External I/O can't starve regular ES indexing and search |
| [GA-2](#ga-2-distributed-execution-hardening) | Distributed execution hardening | stab | P0 | — | 4d–1.5w | Multi-node integration tests under node failure, large split counts |
| [GA-9](#ga-9-bug-fix--stabilization-buffer) | Bug fix & stabilization buffer | stab | P0 | GA-2, GA-4 | 3d–1w | Buffer for TP user feedback and GA regressions |
| [GA-4](#ga-4-buffer-tuning) | Buffer tuning | perf | P1 | TP-10 | 3d–1w | Configurable buffer sizes; dynamic sizing by schema width |
| [GA-7](#ga-7-topn-pushdown) | TopN pushdown | perf | P1 | TP-4 | 3d–1w | `SORT @timestamp DESC \| LIMIT 10` reads only what's needed |
| [GA-3](#ga-3-split-size-aware-distribution) | Split size-aware distribution | scale | P1 | GA-2 | 3d–1w | Work assigned proportional to data volume, not file count |
| [GA-8](#ga-8-distributed-aggregation) | ~~Distributed aggregation~~ **DONE** | scale | P1 | GA-2 | ~~4d–1.5w~~ | Merged: two-phase STATS (#277) — INITIAL on data nodes, FINAL on coordinator |
| [GA-10](#ga-10-views-with-external-sources) | Views with external sources | func | P1 | — | 0.5–1.0w | Pipeline ordering correct (confirmed 2026-03-17); needs test coverage (#373) |

Extends TP into production with thread pool isolation, distributed execution hardening, TopN pushdown, distributed aggregation, buffer tuning, and views with external sources. Schema discovery API powers Kibana autocomplete. Kibana gets syntax highlighting and datasource management UI.

---

#### GA-1. Thread pool isolation

**Current State:** External source I/O runs on the generic thread pool (shared with HTTP, S3 SDK, all plugin I/O). Exchange uses the search pool. A dedicated ESQL worker pool exists for compute but not for external I/O.

**What to change:** Dedicated thread pool for external source I/O. Configurable pool size. Queue depth metrics.

**User Impact:** Heavy external source queries can starve regular ES operations (indexing, search) or vice versa. No observability into contention. Risk of cascading slowdowns in mixed workloads.

**Estimate:** 3d–1w

---

#### GA-2. Distributed execution hardening

**Current State:** Distributed execution tests are **already substantial** — ~180 individual test methods across 20+ test classes:

- **Unit tests (14 classes):** Split discovery and partition pruning (39 tests including L1 filters), distribution strategy (38 randomized property tests covering all 4 strategies), data node execution (23 tests including error contexts), local parallelism (5 tests with real thread pools), fault injection (7 tests including non-retryable 403s).
- **Multi-node integration tests (2 classes):** 15 csv-spec query patterns × 3 distribution modes × 5 storage backends = up to 225 parameterized test cases on a 3-node cluster. Fault-injecting S3 HTTP handler tests with 503/500 and countdown responses.
- **Test fixtures:** All in-process HTTP servers (no Docker). S3/GCS/Azure emulated with request logging.

**Gaps that remain:**
1. **(HIGH) No `internalClusterTest`** for distributed execution — zero tests exercise the real data-node compute handler in a single-JVM multi-node cluster. All distribution tests use mocked discovery nodes or REST API.
2. **(HIGH) No node-failure-during-execution tests** — stale node detection is tested but not recovery/retry when a data node goes down mid-query.
3. **(MEDIUM) No mixed external+Lucene query tests** — dispatch condition is tested but not real hybrid queries.

**What to change:** Add `internalClusterTest` cases for the full distributed path (coordinator → data node dispatch → parallel split execution → result aggregation). Add node-failure scenarios (node drop during split execution). Add tests that mix external sources with ES index queries.

**User Impact:** No direct user impact — engineering infrastructure. Without it, distributed execution regressions ship undetected.

**Estimate:** 4d–1.5w

---

#### GA-3. Split size-aware distribution

**Current State:** Split size-aware distribution is **substantially implemented** with 4 strategies:

| Strategy | Size-Aware? | How It Works |
|----------|-------------|--------------|
| **Adaptive** (default) | Yes — delegates | Decides whether to distribute based on plan shape (aggregation, split-to-node ratio). Selects Weighted when sizes available, Round-Robin otherwise. |
| **Weighted Round-Robin** | Yes — LPT algorithm | Sorts splits largest-first, greedily assigns to least-loaded node. Proven guarantee: max_load ≤ ideal_load + largest_split. |
| **Round-Robin** | No | Simple modulo assignment. |
| **Coordinator-Only** | N/A | Never distributes. |

Size propagation works end-to-end for file-based sources: file splits report byte-range length, coalesced splits sum children's sizes, the split coalescer bin-packs into 128MB groups, and sub-file splitting provides finer granularity for row-based formats.

**Gaps that remain:**
1. **Flight/connector splits return -1 for size** — distribution falls back to plain round-robin for connector sources.
2. **No runtime adaptation** — "adaptive" refers to whether-to-distribute, not runtime load balancing. No work-stealing or speculative execution.
3. **No Parquet row-group splitting** — columnar files are monolithic splits. A 10GB Parquet file is one split.
4. **Static coalescing targets** — 128MB group size and 32-split threshold are compile-time constants.

**What to change:** Ensure connector split types report estimated size (Flight tickets carry row count, can estimate bytes). Make coalescing targets configurable. Investigate Parquet row-group-level splitting for large files.

**User Impact:** When files vary wildly in size (1KB config files mixed with 10GB data files), round-robin assigns equal counts to each node — one node becomes the bottleneck. The weighted strategy handles this but only when sizes are reported.

**Estimate:** 3d–1w

---

#### GA-4. Buffer tuning

**Current State:** Buffer is hardcoded to 10 pages. No byte-based limit (addressed by TP-10's circuit breaker work, but additional tuning is needed). No per-query configuration. The max buffer size field exists on the operator context but the default is always 10.

**What to change:** Make buffer size configurable per query via query pragmas. Dynamic sizing based on schema width (narrow schema = more pages, wide schema = fewer pages for same memory budget). Tunable default based on production experience from TP. The drain timeout (currently hardcoded at 5 minutes) should also become configurable — cancelled queries should stop reading within seconds.

**User Impact:** For high-throughput workloads, the 10-page buffer creates a bottleneck where the producer frequently stalls. Users see lower throughput than the source can deliver.

**Estimate:** 3d–1w

---

#### GA-5. Kibana datasource integration *(Kibana-side — excluded from totals)*

**Current State:** Queries with `EXTERNAL` already work in Kibana's ESQL editor — Kibana sends the query string to the `_query` REST endpoint and displays results in the table view. What's missing: syntax highlighting for `EXTERNAL`, autocomplete for datasource names and columns, schema discovery panel, datasource management UI.

**What to change:** Kibana-side work: syntax highlighting, autocomplete (powered by the schema discovery API — GA-6), schema discovery panel, and datasource management UI. No additional backend work required beyond GA-6.

**User Impact:** Without autocomplete and schema discovery, users must know column names by heart. Matters for adoption but not a functional blocker.

**Estimate:** Kibana team — not included in totals above.

---

#### GA-6. Schema discovery API

**Current State:** No endpoint for discovering datasource schemas at runtime. ESQL has its own fork of `_field_caps` for ES index fields, demonstrating the precedent for ESQL-specific field resolution endpoints. The internal `resolveMetadata()` chain is fully functional — schema resolution just isn't exposed via REST.

**Modeled after `_field_caps`:** The `_field_caps` API returns field names keyed by type, with per-type metadata (searchable, aggregatable, metadata_field, dimension/metric flags) and a per-index breakdown when there are type conflicts across indices. For external sources, the equivalent returns field names and ESQL types, with annotations for partition columns and native type information (e.g., Parquet's `INT64/TIMESTAMP_MILLIS`).

**Target endpoint:**
```
GET /_esql/datasource/{name}/_schema
POST /_esql/datasource/_schema   (for ad-hoc paths)
```

**Response structure:**
```json
{
  "fields": {
    "@timestamp": { "type": "datetime", "partition_column": true },
    "message": { "type": "keyword" },
    "level": { "type": "keyword", "partition_column": true },
    "duration_ms": { "type": "long" }
  },
  "source_type": "parquet",
  "location": "s3://my-bucket/logs/"
}
```

**What to change:** ~4-5 new files: action, transport action (coordinator-only), REST handler, request/response classes. Reuses the existing resolve chain and schema cache (TP-9). The transport action runs on the `GENERIC` thread pool, calls the resolver, and returns field names and types as `DataType.esType()` strings (compatible with what Kibana expects from `_field_caps`).

**User Impact:** Without this, Kibana can't offer column autocomplete for external datasources. Users must memorize or guess field names. Blocks meaningful Kibana integration.

**Estimate:** 4d–1.5w

---

#### GA-7. TopN pushdown

**Area:** perf | **Crit:** P1 | **Deps:** TP-4

**Current State:** `SORT @timestamp DESC | LIMIT 10` reads every row, sorts in the ES|QL engine, then discards all but 10. The existing `PushTopNToSource` optimizer rule (`PushTopNToSource.java:144-148`) only matches `TopNExec` above `EsQueryExec` — `if (child instanceof EsQueryExec queryExec && queryExec.canPushSorts()...)`. The rule skips `ExternalSourceExec`. `ExternalSourceExec` has no `limit` or `sort` fields.

**Key insight:** Every ES|QL query has an implicit LIMIT (default 1000) added by `Analyzer.AddImplicitLimit` (`Analyzer.java:1851-1875`). The `ReplaceLimitAndSortAsTopN` rule (`ReplaceLimitAndSortAsTopN.java:15-25`) always fuses `Limit` + `OrderBy` into `TopN`. This means SORT is never unbounded — it always becomes TopN. Therefore, TopN pushdown benefits ALL SORT queries, not just those with explicit LIMIT.

The LIMIT pushdown from TP-4 adds a `pushedLimit` field, but TopN (sort + limit) is a separate optimization because it requires both a limit AND the engine to respect that the source may not return sorted data.

**What to change:** New `PushTopNToExternalSource` optimizer rule matching `TopNExec` above `ExternalSourceExec`. The rule extracts the limit from the TopN node and pushes it to the source as a `pushedLimit` (reusing the field from TP-4). The sort remains in the engine — external sources don't guarantee sort order (Parquet has no native sort metadata, though Iceberg does via `sort-orders`). The key insight: even without sort pushdown, pushing the limit alone is valuable because it prevents reading the entire dataset. The source reads up to `limit` rows and stops.

**Changes required:**
1. New `PushTopNToExternalSource` rule in the physical optimizer (parallel to `PushTopNToSource`)
2. Rule matches `TopNExec` with `ExternalSourceExec` child, pushes limit
3. The `TopNExec` stays in the plan (engine still sorts) but reads far fewer rows
4. Tests: optimizer rule unit tests, integration tests with SORT+LIMIT on external data

**Competitive context:** `SORT @timestamp DESC | LIMIT 10` is the single most common log analytics query — "show me the latest events." DuckDB, Trino, and Spark all push LIMIT to the scan layer. For a 1TB dataset, the difference is 1TB of I/O versus effectively nothing. This is the query pattern that defines whether external source queries feel "fast" or "broken" in production. It belongs in GA because every competitive product optimizes this, and every user runs this query.

**User Impact:** The most common log query pattern is a full table scan. This makes it fast.

**Estimate:** 3d–1w

---

#### GA-8. Distributed aggregation — DONE

**Status:** Merged ([#277](https://github.com/elastic/esql-planning/issues/277)). Two-phase distributed aggregation for external sources: INITIAL on data nodes, FINAL on coordinator.

**Original estimate:** 4d–1.5w

---

#### GA-10. Views with external sources

**Area:** func | **Crit:** P1 | **Deps:** none | **Issue:** #373

**~~Current State (CORRECTED 2026-03-17):~~** Prior analysis incorrectly claimed views with EXTERNAL don't work due to a pipeline ordering bug. Deep dive confirmed the actual order in `EsqlSession.java`:

1. Parse outer query
2. **View inlining** (`ViewResolver.replaceViews()`) — happens FIRST
3. Pre-analysis (`PreAnalyzer.preAnalyze()` on the already-inlined plan)
4. External source resolution
5. Analysis

The pipeline ordering is correct — `UnresolvedExternalRelation` nodes from view bodies ARE correctly discovered and resolved. **The code path should work.**

**Remaining work:** Zero test coverage exists for views + EXTERNAL. The interaction is entirely untested. Add integration tests to confirm end-to-end correctness and fix any bugs that surface.

**What to change:**
1. Add integration tests: basic view with EXTERNAL, view with processing pipeline, nested views, mixed ES + external
2. Fix any bugs discovered during testing
3. TP-12 item 7 (block EXTERNAL in views) removed — no longer needed

**Estimate:** 0.5–1.0w (test infrastructure + potential bug fixes)

---

#### GA-9. Bug fix & stabilization buffer

**Area:** stab | **Crit:** P0 | **Deps:** GA-2, GA-4

**Current State:** Not applicable — this is reserved buffer time, not a defined deliverable.

**What this covers:**
- Issues surfaced by GA-2 (distributed execution hardening) — node failure scenarios, large split counts, heterogeneous split sizes
- Performance problems found during GA-4 (buffer tuning) that require code fixes beyond tuning
- Bug reports from Tech Preview early adopters
- Regressions introduced by GA items (TopN pushdown, distributed aggregation, thread pool isolation)

**What this does NOT cover:** New features or scope expansion. Strictly for fixing what's broken in GA-scoped items and addressing TP feedback.

**User Impact:** GA is the production-ready milestone — shipping known bugs is not an option. Without buffer, either bugs ship unfixed or scope gets silently cut.

**Estimate:** 3d–1w

---

## Post-MVP

Tracked under [elastic/esql-planning#302](https://github.com/elastic/esql-planning/issues/302).

Post-MVP type support gaps:
- [#336](https://github.com/elastic/esql-planning/issues/336) — CSV type support gaps (post-MVP)
- [#338](https://github.com/elastic/esql-planning/issues/338) — NDJSON type support gaps (post-MVP)
- [#333](https://github.com/elastic/esql-planning/issues/333) — ORC type support gaps (post-MVP)
- [#339](https://github.com/elastic/esql-planning/issues/339) — Parquet type support gaps (post-MVP)

Post-MVP feature groups:
- **Iceberg** — e2e catalog integration, time-travel queries
- **Federation** — UNION ALL, cross-source JOINs
- **Flight Production** — TLS+auth, optimized Arrow zero-copy
- **SQL Database Bridge** — SQL pushdown, JDBC connectors
- **Metrics Federation** — PromQL integration
- **Platform & Polish** — encrypted credentials, user-declared schema, auto partition tuning

