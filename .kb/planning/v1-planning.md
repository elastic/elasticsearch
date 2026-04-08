# ES|QL Data Sources — Planning Document

**Tracking:** [elastic/esql-planning#189](https://github.com/elastic/esql-planning/issues/189) (MVP) | [#302](https://github.com/elastic/esql-planning/issues/302) (Post-MVP)
**Target:** GA in Elasticsearch 9.5 (Q2 FY27, June 2026)
**Estimate:** 43.3 person-weeks total (Parquet-MR path) | 32.0w remaining | ~9.1 calendar weeks at 3.5 engineers

---

## Product Shape

ES|QL Data Sources extends Elasticsearch's query language to query data stored outside Elasticsearch — in cloud object stores like S3, GCS, and Azure Blob Storage — without ingesting it first. Users register named datasources with securely stored credentials, then query files directly using the `EXTERNAL` command in ES|QL pipelines. The result is a single query language that spans both indexed Elasticsearch data and external data lakes.

**Target audience.** The primary audience is observability and security analytics teams who already use Elasticsearch for log search and alerting. These teams increasingly land raw log data on cloud storage (S3, GCS, Azure) for cost reasons — hot data is indexed in Elasticsearch for fast search, while cold and archival data sits in object stores as Parquet or NDJSON files. Today, querying that cold data requires a separate tool (Athena, BigQuery, Trino). ES|QL Data Sources eliminates the tool switch: the same ES|QL queries, the same Kibana interface, the same team workflows — just pointing at a different data tier.

**Formats and storage.** The MVP covers the formats and storage backends that matter for log analytics. Parquet and NDJSON.gz together represent 95%+ of log data in cloud storage — Parquet for optimized columnar storage, NDJSON for raw log output from every major shipper. ORC and CSV/TSV are also supported. S3, GCS, and Azure cover ~90% of cloud data lakes. ZSTD, BZIP2, and GZIP compression are supported natively with streaming decompression.

**Key scenarios.** (1) *Exploratory queries* — `EXTERNAL "s3://bucket/logs/*.parquet" | WHERE level == "ERROR" | LIMIT 100` returns results in seconds via LIMIT pushdown, column projection, and row-group filter pushdown. (2) *Aggregation across large datasets* — `EXTERNAL "s3://bucket/logs/2026-Q1/**" | STATS count() BY region` runs distributed across data nodes with two-phase aggregation, same as native ES indices. (3) *Schema-drifted data* — glob queries across files with different schemas produce correct results via union-by-name, handling the normal state of production log data that evolves over time. (4) *Dashboard-driven analysis* — repeated queries against the same datasource benefit from metadata caching, eliminating redundant object store round-trips.

**Product shape at GA.** Named datasources are provisioned via a REST CRUD API (`PUT/GET/DELETE /_datasource/{name}`) with credentials stored in a secrets system index — no plaintext credentials in queries, logs, or audit trails. Kibana provides datasource management, syntax highlighting, and autocomplete via a schema discovery API. The compute engine enforces memory safety through circuit breaker integration for all source buffers, and external I/O runs on a dedicated thread pool so it cannot starve regular Elasticsearch operations. Distributed execution spreads file reads across data nodes with hardened failure handling. Unsupported Lucene-dependent commands (match, knn, score) produce clear error messages with ES|QL alternatives that work on external data.

**What is not included.** Iceberg catalog integration, cross-source queries (joining ES indices with external data in a single query), and the HTTP and Arrow Flight connectors are deferred to post-MVP milestones.

---

## Current State (March 2026)

### Already Merged

| Item | Issue | What shipped |
|------|-------|-------------|
| LIMIT pushdown | [#276](https://github.com/elastic/esql-planning/issues/276) | `PushLimitToExternalSource` optimizer rule; FormatReader respects rowLimit |
| Two-phase STATS | [#277](https://github.com/elastic/esql-planning/issues/277) | Distributed aggregation for external sources (INITIAL on data nodes, FINAL on coordinator) |
| Vectorized readers | [#278](https://github.com/elastic/esql-planning/issues/278) | Columnar Parquet reader via parquet-mr ColumnReader API |
| Distributed execution | [#279](https://github.com/elastic/esql-planning/issues/279) | SplitProvider → SplitDiscoveryPhase → AdaptiveStrategy → DataNodeComputeHandler |
| CSV/TSV RFC 4180 | [#275](https://github.com/elastic/esql-planning/issues/275) | Correct delimiter, quote escaping, ErrorPolicy, CsvFormatOptions |
| Arrow allocator CB | [#281](https://github.com/elastic/esql-planning/issues/281) | **⚠ Closed prematurely** — code only in unmerged PR #142981. Should reopen. |
| Column pruning | [#294](https://github.com/elastic/esql-planning/issues/294) | PruneColumns handles ExternalRelation |
| Intra-file parallelism (parent) | [#293](https://github.com/elastic/esql-planning/issues/293) | Split into #341 and #342 (both now closed) |
| Text format parallel parsing | [#341](https://github.com/elastic/esql-planning/issues/341) | `SegmentableFormatReader` SPI for text format parallel parsing (0.5w) |
| Parquet row-group parallelism | [#342](https://github.com/elastic/esql-planning/issues/342) | `RangeAwareFormatReader` SPI for Parquet row-group splitting (1.0w) |
| CSV type support gaps | [#334](https://github.com/elastic/esql-planning/issues/334) | All 4 MVP CSV type issues fixed (1.8w) |
| Parquet type support gaps | [#337](https://github.com/elastic/esql-planning/issues/337) | All 7 MVP Parquet type issues fixed (4.0w) |
| Byte-based memory safety | [#289](https://github.com/elastic/esql-planning/issues/289) | `AsyncExternalSourceBuffer` byte-based backpressure (PR #144218) (2.0w) |
| Stats-via-metadata | *(untracked)* | `PushStatsToExternalSource` optimizer rule — COUNT(*)/MIN/MAX from file metadata (PR #143940) |

### Remaining Gaps

| Dimension | Gap |
|-----------|-----|
| **Syntax** | Dev-gated `EXTERNAL "uri"` — not available in production builds. No named datasources, no CRUD API. |
| **Credentials** | Inlined in query text — visible in logs, audit trails, slow query reports. |
| **Performance** | No metadata cache (re-read on every query). Parquet-MR reader improvements (#285) still needed. No TopN pushdown. |
| **Correctness** | match/knn/score crash on external sources. No union-by-name for schema-drifted files. ~~Type support gaps across all 4 formats~~ → CSV and Parquet types fixed; NDJSON (#335, 3.0w) and ORC (#332, 1.5w) type gaps remain. |
| **Stability** | ~~No byte-based memory safety~~ → Fixed (#289). No thread pool isolation. No load/perf testing. |
| **Kibana** | No syntax highlighting, autocomplete, or datasource management UI. |

---

## Work Items

All items tracked as sub-issues of [#189](https://github.com/elastic/esql-planning/issues/189). Estimates in person-weeks.

### Functionality

| Issue | Title | Est | Notes |
|-------|-------|-----|-------|
| [#251](https://github.com/elastic/esql-planning/issues/251) | EXTERNAL command syntax | 2.0w | Finalize production syntax, remove dev gate |
| [#287](https://github.com/elastic/esql-planning/issues/287) | CRUD API | 2.0w | `PUT/GET/DELETE /_datasource/{name}`, cluster state persistence |
| [#273](https://github.com/elastic/esql-planning/issues/273) | Secret management — scoping | 0.5w | Investigation, security team alignment, design doc |
| [#296](https://github.com/elastic/esql-planning/issues/296) | Secret management — implementation | 2.5w | Secrets system index, per-plugin credential wiring |
| [#288](https://github.com/elastic/esql-planning/issues/288) | Schema discovery API | 1.0w | REST endpoint for field names/types; powers Kibana autocomplete |
| [#292](https://github.com/elastic/esql-planning/issues/292) | Pricing & licensing definition | 0w | Non-engineering |

### Performance

Two Parquet reading paths under investigation ([#284](https://github.com/elastic/esql-planning/issues/284)). Parquet-MR is the main estimate; Arrow is the alternative.

| Issue | Title | Est | Path |
|-------|-------|-----|------|
| [#284](https://github.com/elastic/esql-planning/issues/284) | Parquet reader strategy investigation | 2.0w | Both |
| [#285](https://github.com/elastic/esql-planning/issues/285) | [Parquet-MR] Reader improvements | 2.0w | Parquet-MR |
| [#282](https://github.com/elastic/esql-planning/issues/282) | [Parquet-MR] Circuit breaker integration | 4.0w | Parquet-MR |
| [#254](https://github.com/elastic/esql-planning/issues/254) | Native Arrow Blocks/Vectors | 2.0w | Arrow |
| [#295](https://github.com/elastic/esql-planning/issues/295) | [Native Arrow] Arrow Dataset Parquet reader | 4.0w | Arrow |
| [#286](https://github.com/elastic/esql-planning/issues/286) | Metadata inference cache | 3.0w | Both |
| [#252](https://github.com/elastic/esql-planning/issues/252) | Rally track | 2.0w | Both |
| [#283](https://github.com/elastic/esql-planning/issues/283) | JMH microbenchmarks | 1.0w | Both |

### Correctness

| Issue | Title | Est | Notes |
|-------|-------|-----|-------|
| [#274](https://github.com/elastic/esql-planning/issues/274) | Unsupported command errors | 0.5w | match/knn/score produce helpful errors instead of crashes |
| [#280](https://github.com/elastic/esql-planning/issues/280) | Schema union-by-name | 3.0w | Glob queries across schema-drifted files |
| [#334](https://github.com/elastic/esql-planning/issues/334) | ~~CSV type support gaps~~ | 1.8w | **CLOSED** — all 4 MVP issues fixed |
| [#335](https://github.com/elastic/esql-planning/issues/335) | NDJSON type support gaps | 3.0w | 6 MVP issues ([#303](https://github.com/elastic/esql-planning/issues/303), [#306](https://github.com/elastic/esql-planning/issues/306), [#311](https://github.com/elastic/esql-planning/issues/311), [#321](https://github.com/elastic/esql-planning/issues/321), [#325](https://github.com/elastic/esql-planning/issues/325), [#328](https://github.com/elastic/esql-planning/issues/328)) |
| [#332](https://github.com/elastic/esql-planning/issues/332) | ORC type support gaps | 1.5w | 2 MVP issues ([#305](https://github.com/elastic/esql-planning/issues/305), [#313](https://github.com/elastic/esql-planning/issues/313)) |
| [#337](https://github.com/elastic/esql-planning/issues/337) | ~~Parquet type support gaps~~ | 4.0w | **CLOSED** — all 7 MVP issues fixed |
| [#300](https://github.com/elastic/esql-planning/issues/300) | Uniform IT source test data | 0.5w | Shared test datasets across formats |

### Stability

| Issue | Title | Est | Notes |
|-------|-------|-----|-------|
| [#289](https://github.com/elastic/esql-planning/issues/289) | ~~Byte-based memory safety~~ | 2.0w | **CLOSED** — byte-based backpressure in AsyncExternalSourceBuffer (PR #144218) |
| [#290](https://github.com/elastic/esql-planning/issues/290) | Thread pool isolation | 1.0w | Dedicated pool for external I/O |
| [#291](https://github.com/elastic/esql-planning/issues/291) | Distributed execution hardening | 2.0w | Node failure, large split counts, internalClusterTest |

---

## Estimate Summary

| | Parquet-MR (main) | Native Arrow |
|---|---|---|
| **Total scope** | 43.3w | 44.3w |
| Done | 11.3w | 12.3w |
| **Remaining** | **32.0w** | **32.0w** |
| At 3.5 engineers | **~9.1 cal weeks** | **~9.1 cal weeks** |

Newly closed since last update: #341 (0.5w), #342 (1.0w), #334 (1.8w), #337 (4.0w), #289 (2.0w) = 9.3w.
Items with 0w estimate (non-engineering or covered elsewhere): #292 (pricing/licensing), #299 (multivalue — covered by type support gaps).
Untracked work on main: `PushStatsToExternalSource` (stats-via-metadata, PR #143940) — not associated with any issue.

---

## Post-MVP

Tracked under [#302](https://github.com/elastic/esql-planning/issues/302).

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
