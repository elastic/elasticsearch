## ES|QL Data Sources — MVP

ES|QL Data Sources extends Elasticsearch's query language to query data stored outside Elasticsearch — in cloud object stores like S3, GCS, and Azure Blob Storage — without ingesting it first. Users query files directly using the `EXTERNAL` command in ES|QL pipelines, with the same processing commands, functions, and distributed execution that work on native ES indices. The result is a single query language that spans both indexed Elasticsearch data and external data lakes.

### Focus: Log Analytics & Observability

The MVP targets the narrowest useful scenario first: **querying log data on blob stores, without catalogs**. This is the most common real-world pattern — observability and security teams landing raw logs on S3/GCS/Azure as Parquet or NDJSON files, organized in Hive-style directory layouts. No Iceberg, no table catalogs, no cross-source joins. Just point at files and query them.

This focus is deliberate. The primary audience is observability and security analytics teams who already use Elasticsearch for log search and alerting. These teams increasingly land raw log data on cloud storage for cost reasons — hot data is indexed in Elasticsearch for fast search, while cold and archival data sits in object stores. Today, querying that cold data requires a separate tool (Athena, BigQuery, Trino). ES|QL Data Sources eliminates the tool switch: the same ES|QL queries, the same Kibana interface, the same team workflows — just pointing at a different data tier.

S3 + GCS + Azure covers ~90% of cloud data lakes. Parquet (~80% of optimized log storage) + NDJSON (~80-90% of raw log data — every major shipper defaults to it) covers ~95%+ of log formats. ORC (~10-15%, legacy Hadoop) and CSV/TSV are also readable. This is the core observability entry point: ~65% of enterprises have or are building a data lake, and the dominant use case is querying log data that's already landing on cloud storage.

Catalog integration (Iceberg), cross-source federation (UNION ALL with ES indices), connectors (Flight, JDBC), and advanced optimizations all come later. By shipping the simple case first, we validate the core infrastructure — syntax, credentials, distributed execution, format readers, memory safety — before layering on complexity.

### Product Shape

**Syntax and credentials.** Named datasources and datasets are provisioned via a REST CRUD API under the `_query/` namespace, following the same pattern as views. Datasources hold credentials and connection details; datasets are index abstractions that reference a datasource and specify what to query. Credentials are encrypted in cluster state using the [Encrypted Cluster State Secrets](https://docs.google.com/document/d/1bqDmBQaeyEC_EAvjpuSOvLc_kbLyw3CygfU_4VWX6Is/) design — no plaintext credentials in queries, logs, or audit trails. See the [CRUD API Design](https://github.com/elastic/esql-planning/issues/414) and the [Security Design](https://docs.google.com/document/d/1P9BFEwBgliunLSPDdFmX5TNuI9Z02HpFyKoEtKuACtQ/) for full details.

```json
// Register a datasource (connection + credentials)
PUT _query/datasource/prod_s3_logs
{
  "type": "s3",
  "region": "us-east-1",
  "access_key": "AKIA...",
  "secret_key": "wJal..."
}

// Define a dataset (what to query)
PUT _query/dataset/access_logs
{
  "datasource": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet"
}
```

The `EXTERNAL` command is the initial query syntax; the target is `FROM`, since datasets are index abstractions and external data should be queried the same way as native indices:

```
FROM access_logs
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
| SORT error_count DESC
| LIMIT 20
```

**Formats and storage.**

Formats:
- **Parquet** — optimized columnar storage (~80% of analytical log data)
- **NDJSON** (.gz) — raw log output from every major shipper (~80-90% of raw logs)
- **ORC** — legacy Hadoop ecosystems
- **CSV/TSV** — general-purpose tabular data

Storage:
- **S3**, **GCS**, **Azure Blob Storage** — covers ~90% of cloud data lakes

Compression:
- **ZSTD**, **BZIP2**, **GZIP** — streaming decompression, no full-file buffering

**Performance.** LIMIT pushdown stops reading at the requested row count instead of scanning entire datasets. Column projection reads only the requested columns. Metadata caching eliminates redundant object store round-trips for repeated queries (dashboard scenarios). Distributed execution spreads file reads across data nodes via split discovery, adaptive distribution strategy, and two-phase aggregation — the same partial-on-data-nodes, merge-on-coordinator pattern used for ES indices.

The current Parquet reader uses the Parquet-MR columnar path. [Benchmarking](https://github.com/elastic/esql-parquet-bench/blob/main/VALIDATION.md) against Arrow's zero-copy reader on S3 shows Arrow is 7-15x faster across all scenarios:

![S3 validation latency](https://github.com/elastic/esql-planning/releases/download/benchmark-charts/validation-s3.png)

A key finding: parquet-mr's filter pushdown is **2x slower than full scan** on S3 due to extra round trips for dictionary, bloom filter, and column index metadata — each a separate S3 HTTP range request. Arrow evaluates filters inline during decoding with no extra I/O. The plan is to adopt Arrow Dataset as the Parquet reader for GA.

**Correctness.** Unsupported Lucene-dependent commands (match, knn, score) produce clear error messages. Schema union-by-name handles the normal state of production log data — files written weeks or months apart with different columns produce correct results instead of failures. Type support gaps across all four formats are addressed: silent data corruption bugs (Parquet decimals wrong by 10^scale, timestamps off by 1000x), crash bugs (ORC decimals, NDJSON nested objects), and missing type mappings.

**Stability.** Circuit breaker integration prevents OOM from untracked source buffers with byte-based tracking. Thread pool isolation ensures external I/O cannot starve regular Elasticsearch indexing and search. Distributed execution is hardened under node failure, large split counts, and heterogeneous split sizes via internal cluster tests. Load and performance testing via JMH microbenchmarks and Rally tracks provides regression detection.

**Kibana.** Datasource management UI, syntax highlighting, and autocomplete powered by a schema discovery API (`GET /_esql/datasource/{name}/_schema`).

### Key Scenarios

The target syntax is `FROM` — since datasets are index abstractions, external data is queried the same way as native indices.

1. **Exploratory queries** — `FROM access_logs | WHERE level == "ERROR" | LIMIT 100` returns results in seconds via LIMIT pushdown, column projection, and partition pruning.
2. **Aggregation across large datasets** — `FROM access_logs | STATS count() BY region` runs distributed across data nodes with two-phase aggregation.
3. **Schema-drifted data** — glob queries across files with different schemas produce correct results via union-by-name, handling the normal state of production log data that evolves over time.
4. **Dashboard-driven analysis** — repeated queries against the same datasource benefit from metadata caching, eliminating redundant object store round-trips.

### What Is Not Included

- Iceberg catalog integration
- Cross-source queries (joining ES indices with external data in a single query)
- HTTP and Arrow Flight connectors
- SQL database bridging (JDBC/ODBC)
- Metrics federation (PromQL)

These are tracked under the [post-MVP milestone](https://github.com/elastic/esql-planning/issues/302).

### Ownership

| Lane | Owner | Focus |
|------|-------|-------|
| **Infrastructure** | @costin | Execution framework, parallelization, performance |
| **Formats** | @swallez | NDJSON correctness, Parquet types (remaining), Arrow |
| **Schema, Secrets & Consistency** | @quackaplop | Secrets, CRUD, performance, Arrow, Syntax |
| **Scale, Perf & Infra Ramp-Up** | @bpintea | ORC, Rally, JMH, memory safety |

### Timeline

| Milestone | Tagline | Est Delivery | Issues | Description |
|-----------|---------|-------------|--------|-------------|
| DS-M1 (^) | It Works | March 27 ✅ | [DS-M1 issues](https://github.com/elastic/esql-planning/milestone/4) | Parquet + NDJSON queries work correctly, no crashes, thread pool isolation in place. |
| DS-M2 (^) | Demo Ready | April 17 (~1 week after 9.4 FF)  🔄 | [DS-M2 issues](https://github.com/elastic/esql-planning/milestone/5) | CRUD API, basic secrets, circuit breaker, schema union-by-name, Serverless. TP candidate. |
| DS-M3 (^) | Real-World Ready | May 15 | [DS-M3 issues](https://github.com/elastic/esql-planning/milestone/7) | TP feedback, correctness, performance baseline, telemetry, product surface items that aren't TP-critical but are for GA. |
| DS-M4 (^) | GA Ready | June 15 (2 weeks from 9.5) | [DS-M4 issues](https://github.com/elastic/esql-planning/milestone/6) | Full type coverage, performance, hardening, Rally track. 2-week buffer to GA. |

(^) Internal milestones

**GA:** Elasticsearch 9.5 (Q2 FY27, June 2026)
