# External Data Sources on Main: Full Overview

## What Exists

ES|QL on main supports querying external data via the `EXTERNAL` command (dev-gated, not released). The system has four layers: a plugin framework with lazy loading, a dual SPI (file-based and query-based), distributed execution with split-based parallelism, and 11+ concrete plugins.

### Pipeline Integration

The query pipeline is fully wired. `ExternalSourceResolver` resolves an `EXTERNAL "expression"` into an `ExternalRelation` logical plan node with inferred schema. The optimizer handles it through standard rules. `ExternalSourceExec` is the physical plan node. After physical planning, `SplitDiscoveryPhase` populates splits, and `ExternalDistributionStrategy` decides whether to distribute across data nodes or keep on coordinator. `LocalExecutionPlanner.planExternalSource()` creates operators with `ExternalSliceQueue` for local parallelism. Async execution is supported via `AsyncExternalSourceOperator` with backpressure buffering.

**What works well:** End-to-end from parse → resolve → optimize → plan → split discovery → distribute → execute is complete. The split-based model enables both local parallelism (multiple drivers per node) and cluster-wide distribution (splits assigned to data nodes via exchange infrastructure).

**What's missing:** The command syntax is `EXTERNAL "uri"`, not the target `FROM datasource::expression`. No named connection management or credential storage API. No persistent datasource definitions.

### Dual SPI: Files and Queries

The framework has two distinct execution paths, unified under `ExternalSourceFactory`:

**File-based path** (StorageProvider + FormatReader):
- `StorageProvider` → `StorageObject` → `InputStream` for file access
- `FormatReader` reads formats (Parquet, ORC, NDJSON, CSV)
- `TableCatalog` for catalog-managed sources (Iceberg)
- Suited for: S3 Parquet, GCS NDJSON, Iceberg tables, any files-on-storage scenario

**Query-based path** (Connector + ResultCursor):
- `Connector.execute(QueryRequest, Split) → ResultCursor` for query execution
- `ConnectorFactory` extends `ExternalSourceFactory` — registers via same plugin mechanism
- `ResultCursor` streams Pages back incrementally
- `AsyncConnectorSourceOperatorFactory` wraps connectors for async execution on background threads
- Suited for: Arrow Flight, JDBC databases, PromQL, REST APIs — any source where you send a query and get results back

**OperatorFactoryRegistry** dispatches between these paths: if the factory is a `ConnectorFactory`, it opens a connector and wraps in async; otherwise it uses `SourceOperatorFactoryProvider` for file-based reading.

**What works well:** Clean separation. File sources compose storage × format. Query sources implement a single interface. Both share the same plugin lifecycle, split model, and async execution infrastructure.

**What's missing:** Only one query-based connector exists (Arrow Flight). No JDBC, PromQL, or REST connector implementations. The Connector SPI is proven by Flight but not yet validated against SQL databases or HTTP APIs.

### Distributed Execution

Split-based distribution across the cluster is **fully implemented** (merged Feb 25-27, 2026 — issue #142996, 6 PRs):

**Split model:**
- `ExternalSplit` (NamedWriteable interface) — serializable work unit
- `FileSplit` — concrete implementation: path, byte offset, length, format, partition values
- `SplitProvider` — pluggable: each source factory provides split discovery logic
- `SplitDiscoveryPhase` — walks physical plan, calls `SplitProvider.discoverSplits()` per source, collects ancestor filter expressions for partition pruning context

**Local parallelism:**
- `ExternalSliceQueue` — thread-safe split queue (AtomicInteger-based claiming)
- `LocalExecutionPlanner` sets `DATA_PARALLELISM = min(splitCount, taskConcurrency)` — multiple drivers per node, each processing different splits

**Cluster distribution:**
- `AdaptiveStrategy`: single split → coordinator-only; LIMIT-only → coordinator-only; aggregations + multiple splits → distribute across data nodes; falls back to coordinator if no eligible nodes
- `RoundRobinStrategy`: assigns splits round-robin across eligible nodes
- `DataNodeRequest` carries `List<ExternalSplit>` with transport version gating
- `DataNodeComputeHandler.startExternalComputeOnDataNodes()` sends split assignments to data nodes via exchange infrastructure
- Data nodes inject splits into `ExternalSourceExec.withSplits()` and execute locally

**Partition detection:**
- `HivePartitionDetector`: auto-detects `key=value` path segments, infers types (Integer → Long → Double → Boolean → Keyword)
- `TemplatePartitionDetector`: uses `{name}` path templates for non-Hive layouts (Kinesis Firehose, CloudTrail)
- `AutoPartitionDetector`: tries Hive first, falls back to template
- `VirtualColumnInjector`: partition values exposed as queryable columns via ConstantBlocks

**Three-level filter model:**
- L1: Partition pruning at split discovery (full expression evaluation against path-derived values)
- L2: Per-node filter pushdown via `FilterPushdownRegistry` (translated to source-native format)
- L3: Engine remainder filter in the plan (standard FilterExec)

**What works well:** This is real, production-ready distributed execution. Zero stubs. Proper serialization with backward compatibility. Adaptive strategy avoids distribution overhead for simple queries.

**What's missing:** The `ExternalSourceExec` still nominally implements `ExecutesOn.Coordinator` (planning is coordinator-initiated), but execution fans out via exchanges. Not all source factories implement `SplitProvider` yet — sources without a provider default to `Split.SINGLE` (coordinator-only, single-threaded).

### Storage Providers

Three storage providers:

- **S3** (`esql-datasource-s3`): AWS SDK v2. `s3://bucket/path` URIs, glob patterns, range reads. Environment/instance profile credentials.
- **GCS** (`esql-datasource-gcs`): Google Cloud Storage. `gs://bucket/path` URIs, glob patterns, range reads.
- **HTTP/Local** (`esql-datasource-http`): `http://`, `https://`, `file://` URIs. HTTP uses Java `HttpClient`. Local uses `Files.newInputStream()`.

**What works well:** Production-quality for basic reads. Range reads enable efficient columnar access. Clean abstraction.

**What's missing:** Azure ADLS Gen2 — a hard requirement for enterprise adoption. No configurable authentication beyond environment defaults. Glob-based file discovery is linear, not optimized for millions of objects.

### Format Readers

Four format readers plus streaming decompression:

- **Parquet** (`esql-datasource-parquet`): `parquet-mr` (Hadoop's Group API). Column projection, row group reads. Schema from footer.
- **ORC** (`esql-datasource-orc`): Vectorized via Apache ORC `VectorizedRowBatch`. Column projection, predicate pushdown at ORC level.
- **NDJSON** (`esql-datasource-ndjson`): Jackson streaming parser. Line-by-line with batching. Schema from first 100 lines. Error recovery skips malformed lines.
- **CSV** (`esql-datasource-csv`): Basic. Header-based schema.
- **Compression codecs** (merged Feb 27): `CompressionDelegatingFormatReader` wraps any format reader with streaming decompression. ZSTD and BZIP2 plugins registered. Compound extensions like `.csv.zst`, `.ndjson.zstd` handled automatically.

**What works well:** Column projection works across all formats. NDJSON streaming with error recovery is well-designed. ZSTD/BZIP2 support via codec delegation is clean and extensible.

**What's missing:**
- Parquet uses `parquet-mr`, not Arrow-native readers — no vectorized batch reads, no dictionary encoding exploitation, no page-level filtering. For competitive Parquet performance (vs. DuckDB, Trino, ClickHouse), Arrow-native reading is essential.
- No predicate pushdown into Parquet row group statistics.
- CSV support is minimal — no configurable delimiters, limited type inference.
- No schema caching — every query re-reads file metadata.
- Gzip (`.gz`) not yet in the codec registry (only ZSTD and BZIP2 so far).

### Arrow Flight / gRPC

A **fully working** Flight connector (`esql-datasource-grpc`):

- `FlightConnectorFactory`: handles `flight://` and `grpc://` URIs, resolves Arrow schema via `FlightClient.getSchema()`, converts Arrow types to ES|QL Attributes
- `FlightConnector`: implements `Connector.execute()` — gets `FlightInfo`, obtains ticket, opens `FlightStream`
- `FlightResultCursor`: converts Arrow `VectorSchemaRoot` batches to ES|QL Pages via `FlightTypeMapping.toBlock()`
- Registered via `GrpcDataSourcePlugin` with lazy loading

**What works well:** End-to-end query execution over Arrow Flight. Streaming (not all-in-memory). Type mapping from Arrow to ES|QL. This proves the `Connector` SPI works for non-file sources.

**What's missing:** No authentication or TLS. No split discovery (single-stream). No filter pushdown to the Flight server. These are implementation gaps, not SPI gaps.

### Iceberg

The Iceberg plugin implements `TableCatalog`:

- Reads Iceberg metadata (table metadata JSON, manifest lists, manifests)
- Partition pruning via manifest-level min/max statistics
- Uses Arrow-based Parquet reader for data files (better than standalone Parquet plugin)
- `FilterPushdownSupport` translates ES|QL expressions to Iceberg predicates

**What works well:** Manifest-based pruning is correct and functional. Arrow reader for data files. Filter pushdown.

**What's missing:** Only stub REST catalog client — no Glue, Hive Metastore, or Nessie integration. No time-travel. No schema evolution.

### Optimizer Integration

- **Filter pushdown:** Infrastructure works. Iceberg implements it. Parquet/ORC/NDJSON/CSV do not push filters — applied after reading. The three-level filter model (partition prune → per-node pushdown → engine remainder) is now wired.
- **LIMIT pushdown:** `PushLimitToSource` only handles `EsQueryExec`. External sources read the full dataset even for `| LIMIT 10`.
- **Column projection:** Works across all formats.
- **Aggregate/sort pushdown:** Not implemented for any external source.

### Schema Handling

- `FIRST_FILE_WINS` is the only implemented strategy
- `STRICT` and `UNION_BY_NAME` are TODOs
- No schema caching
- No type coercion across files

### Plugin Architecture

Well-designed:

- Lazy loading via `DataSourceModule` — heavy dependencies (Arrow, AWS SDK, JDBC drivers) loaded on first use
- `DataSourcePlugin` registers `StorageProviderFactory`, `FormatReaderFactory`, `ConnectorFactory`, `TableCatalogFactory`, and `DecompressionCodec` instances
- 11+ concrete modules: S3, GCS, HTTP, Parquet, ORC, NDJSON, CSV, Iceberg, gRPC/Flight, ZSTD, BZIP2

### Testing

- Happy-path coverage is good per plugin
- Integration tests for S3/GCS with testcontainers
- **Gaps:** No distributed execution tests yet (infrastructure just merged). No performance benchmarks. No chaos/failure tests. No schema drift tests.

## Summary: Current State vs. North Star

| Capability | Current State | North Star (GA) | Gap |
|---|---|---|---|
| **Distributed execution** | Implemented: splits, local parallelism, data-node dispatch, adaptive strategy | Production hardening, all sources wired | **Small** — infrastructure done, needs per-source SplitProvider implementations |
| **Partition detection** | Hive-style and template-based auto-detection with virtual columns | Validated at scale | **Small** — implemented, needs production testing |
| **Query-based connectors** | Connector SPI + working Flight implementation | JDBC, PromQL, REST | **Medium** — SPI proven, needs concrete implementations |
| **Parquet performance** | parquet-mr (row-oriented) | Arrow-native, vectorized | **Large** — competitive table stakes |
| **Storage coverage** | S3, GCS, HTTP | + Azure ADLS | **Medium** — enterprise blocker |
| **Limit pushdown** | Not implemented | Early termination | **Medium** — UX and cost |
| **Filter pushdown** | Iceberg only; L1-L3 model wired | All columnar formats | **Medium** — performance |
| **Schema handling** | First-file-wins only | Union-by-name, caching | **Medium** — data lake reality |
| **Iceberg catalog** | Stub REST client | Glue, Hive, Nessie | **Medium** — adoption blocker |
| **Syntax** | `EXTERNAL "uri"` | `FROM ds::expr` | **Small** — parser change |
| **Compression** | ZSTD, BZIP2 via codec delegation | + gzip | **Small** — add GzipDecompressionCodec |
| **Aggregate/sort pushdown** | None | Connector-specific | **Future** — optimization |

**Bottom line:** The framework is further along than expected. Distributed execution, partition detection, the dual SPI (files + queries), Flight connector, and streaming decompression are all implemented. The remaining gaps are: production syntax (`FROM ds::expr`), Arrow-native Parquet, Azure ADLS, LIMIT pushdown, filter pushdown for non-Iceberg formats, Iceberg catalog integration, schema drift handling, and concrete connector implementations (JDBC, PromQL). The critical architectural work is done — what remains is mostly feature implementation on top of a solid foundation.
