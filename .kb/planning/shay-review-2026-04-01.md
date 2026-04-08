# ES|QL Data Sources

## The Elastic stack is powerful — but you have to bring the data in

- Elastic stack offers search, analytics, alerting, dashboards, security — incredibly capable
- But today, all of that requires ingesting data into Elasticsearch first
- Customers have massive amounts of data accumulated outside — cloud storage, databases, streaming systems
- We want to apply the power of the Elastic stack to all of it
- Their data is already there. We should be able to use it.

## ES|QL Everywhere. No, Really.

**Users don't have to think about where data is.**

- Query data where it lives — no ingestion required
- External data is a first-class citizen, not an afterthought
- ES|QL doesn't change — same language for all data
- Bring all your data into Elastic's reach — search, dashboards, alerting, agents
- Performance target: ClickHouse-level

## Two concepts. That's it.

**Connection** — where to connect
- S3/GCS/Azure storage, Snowflake, PostgreSQL, Arrow Flight, PromQL, ...
- Credentials + endpoint

**Dataset** ("virtual index") — what to query
- New index abstraction — same namespace as indices, aliases, views
- References a connection, points to the data
- ES|QL sees it as an index

## Working with external data

```json
// Register a connection to S3
PUT _query/datasource/s3_logs_aws
{ "type": "s3", "region": "us-east-1", "access_key": "..." }

// Create a virtual index pointing at Parquet files
PUT _query/dataset/logs_s3
{ "datasource": "s3_logs_aws", "resource": "s3://bucket/logs/**/*.parquet" }
```

```
FROM logs*
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
| SORT error_count DESC
| LIMIT 20
```

`logs*` matches both local indices and virtual indices. Same query. No distinction.

## MVP: Logs on External Storage

Core architecture, user experience, production-ready for logs.

**Data sources supported:**
- Storage: Amazon S3, Google Cloud Storage, Azure Blob Storage
- Formats: Parquet, NDJSON (.gz), ORC, CSV/TSV
- Compression: ZSTD, BZIP2, GZIP — streaming

**ES|QL capabilities on external data:**
- All processing commands: WHERE, EVAL, STATS, SORT, LIMIT, KEEP, DROP, RENAME, DISSECT, GROK, ...
- All scalar functions
- Joins: ENRICH, LOOKUP JOIN (external on left), INLINESTATS
- Distributed execution across data nodes — same compute engine as local indices
- Optimizations: LIMIT pushdown, column projection, Parquet filter pushdown (bloom + statistics), stats from metadata

**Not yet supported:**
- Full-text search (MATCH, KQL, KNN), relevance scoring
- LOOKUP JOIN with external on right side
- Time series mode

## After MVP: Expanding in Two Dimensions

**Breadth — more data sources:**
- Iceberg for data lakes (Snowflake, Glue, Hive)
- JDBC for relational databases and lookup capabilities
- PromQL for external metrics
- Arrow Flight for streaming endpoints

**Depth — deeper ES|QL support:**
- Dynamic mode — schema-on-read, defer type validation to runtime
- Joins with external data in both directions
- Materialized views on external data sources
- Specialized commands: full-text search, vector search, relevance scoring on external data

## Timeline

**Release schedule:**
- **GA:** Elasticsearch 9.5 (June 2026)
- **Tech Preview:** April–May — depends on security readiness

**MVP milestones:**

| Milestone | What | Target |
|-----------|------|--------|
| **M1** ✅ | It Works — formats, storage, execution, safety | March 27 — done |
| **M2** 🔄 | Demo Ready — CRUD, encryption, RBAC, union-by-name | April 17 |
| **M3** | Real-World Ready — performance, telemetry, testing | May 15 |
| **M4** | GA Ready — Arrow reader, Rally track, hardening | June 15 |

## M1: Where we are

March 27 — everything works end to end:

```
EXTERNAL "s3://logs-bucket/access/**/*.parquet" WITH { "access_key": "...", ... }
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
```

What shipped:
- Distributed execution engine — split discovery, adaptive distribution across data nodes. Similar architecture to Spark and Trino.
- Implemented support for: Parquet, NDJSON, ORC, CSV on S3, GCS, Azure with ZSTD/BZIP2/GZIP compression
- Engine optimizations and source pushdowns: LIMIT, column projection, Parquet filter pushdown (bloom + statistics), stats from file metadata
- Safety: byte-based circuit breaker, dedicated thread pool, cloud API rate limiting
- Intra-file parallelism for text formats and Parquet row-group splitting
- Simple schema inference from file metadata
- Views on external data sources

## M2–M3: What's next

```json
PUT _query/datasource/s3_logs_aws
{ "type": "s3", "region": "us-east-1", "access_key": "..." }

PUT _query/dataset/s3_logs
{ "datasource": "s3_logs_aws", "resource": "s3://bucket/logs/**/*.parquet" }
```

```
FROM s3_logs
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
```

- CRUD API (connections + virtual indices)
- FROM syntax for external data
- Encryption & RBAC
- Advanced schema inference (union-by-name) and caching
- Migrate Parquet reader to Arrow Dataset (7-15x faster on S3)
- Stability, resilience, scale testing, and performance benchmarking
- Telemetry, timezone handling

## Dependencies

- **Security** — RBAC and encrypted credentials. Dependency on security team.
- **Kibana** — need to plan datasource management UI and editor integration
- **Pricing & metering** — haven't started, important for GA
- **Serverless** — autoscaler work needed, not yet started
- **TP customers** — need to identify design partners for early feedback

## Project Execution

**Velocity (4 engineers, 6 weeks):**
- 67 PRs merged — 11 per week
- ~89K net new lines of code
- 36 of 72 issues closed
- M1 delivered on schedule

**Claude / Cursor used for everything:**

- Coding — *"Build an Arrow Dataset-based Parquet reader using the existing SPI"*
- Project planning — *"Reconcile all 72 issues against the codebase, re-estimate, close stale ones"*
- Design docs — *"Write the CRUD design, validate every claim against source code"*
- Deep dives — *"What does it take to implement the index abstraction?"*
- Design review — *"Review the security proposal against our design, list gaps"*
- Benchmarking — *"Build a harness to test 3 Parquet readers against S3, run on EC2"*

## Design Principles

1. **Single, consistent language and engine** — ES|QL uniform across all data sources.
2. **External data feels like an index** — not an afterthought.
3. **Built for extensibility** — adding a new data source requires no engine changes.
4. **Zero data preparation** — query data as-is, no prep required. Catalogs are first-class when available, but never mandatory.
5. **Strict separation of concerns** — all commands and functions are source-agnostic. Data source logic separated from the core execution path.

## Architecture: Built for extensibility

```
                    ES|QL Core Engine
          (Parser, Analyzer, Optimizer, Compute)
                         |
               ExternalSourceExec (generic)
                         |
              OperatorFactoryRegistry
               /         |         \
        StorageProvider  TableCatalog  ConnectorFactory
        (file-based)    (Iceberg)     (Flight, JDBC)
             |
      +------+------+
      |             |
  StorageProvider  FormatReader
  S3/GCS/Azure    Parquet/ORC/NDJSON/CSV
      |             |
  Compression    FilterPushdown
  ZSTD/BZIP2/GZIP
```

- 17 plugins on main today
- New storage = one plugin, no core changes
- New format = one plugin, no core changes
- New connector = one plugin, no core changes
- Core engine never knows where data comes from

