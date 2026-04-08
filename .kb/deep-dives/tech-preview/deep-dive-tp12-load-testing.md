# Deep Dive: Load/Performance Testing Framework for ESQL External Sources

## 1. Existing ESQL Test Infrastructure

### qa/ Subdirectories

The main ESQL QA tree lives at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/` and contains:

| Directory | Purpose |
|-----------|---------|
| `qa/action/` | Internal cluster integration tests (`src/internalClusterTest/`) |
| `qa/security/` | Security-related Java REST tests (`src/javaRestTest/`) |
| `qa/server/` | Main test infrastructure (shared base classes, fixtures, spec test runner) |
| `qa/server/single-node/` | Single-node Java REST tests and YAML REST tests |
| `qa/server/multi-node/` | Multi-node Java REST tests and YAML REST tests |
| `qa/server/multi-clusters/` | Cross-cluster query tests |
| `qa/server/mixed-cluster/` | Mixed-version BWC tests |
| `qa/testFixtures/` | Shared test fixtures: csv-spec files, data files, spec reader, data loader |

### Test Kinds

1. **Unit tests** (`src/test/`): Standard JUnit tests for parsers, optimizers, plan nodes. Run via `./gradlew :x-pack:plugin:esql:test`.

2. **Internal cluster tests** (`src/internalClusterTest/`): In-process multi-node cluster tests. Run via `./gradlew :x-pack:plugin:esql:internalClusterTest`.

3. **CSV-spec tests** (`EsqlSpecTestCase`): Declarative tests defined in `.csv-spec` files. Each file contains named tests with a query and expected tabular output. The runner (`EsqlSpecTestCase` at line 78 of `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/EsqlSpecTestCase.java`) executes these against single-node, multi-node, and mixed-cluster configurations.

4. **YAML REST tests**: Standard Elasticsearch YAML REST tests for ESQL endpoints.

5. **Java REST tests**: Custom integration tests using `ESRestTestCase` (e.g., `RestEsqlTestCase`, `FieldExtractorTestCase`, etc.).

6. **Generative tests**: `GenerativeForkRestTest` (line 33 of `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/generative/GenerativeForkRestTest.java`) appends FORK suffixes to existing csv-spec tests to generate additional test permutations.

### Key Test Base Classes

- **`EsqlSpecTestCase`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/EsqlSpecTestCase.java`): Core csv-spec runner. Uses `@ParametersFactory` to load tests from `.csv-spec` files. Has `ProfileLogger` rule (line 81) that captures ESQL profile data on failure. Also has `TookMetrics` integration (line 38, usage at lines 360-393) that randomly verifies the `took` histogram is incremented -- this is the closest thing to latency measurement in functional tests, but it only verifies the histogram bucket increments, not absolute timing.

- **`RestEsqlTestCase`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/RestEsqlTestCase.java`): Lower-level REST client wrapper for executing ESQL queries.

- **`AbstractExternalSourceSpecTestCase`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/AbstractExternalSourceSpecTestCase.java`): Extends `EsqlSpecTestCase` for external data sources. Handles template replacement, storage backend parameterization, and fixture loading.

---

## 2. External Source Tests

### Per-Plugin Unit Tests

Each datasource plugin has unit tests in `src/test/`:

| Plugin | Test Files | What They Test |
|--------|-----------|----------------|
| `esql-datasource-parquet` | `ParquetFormatReaderTests.java`, `ParquetStorageObjectAdapterTests.java` | Schema reading, page iteration, data type mapping |
| `esql-datasource-ndjson` | `NdJsonPageIteratorTests.java`, `NdJsonSchemaInferrerTests.java` | Streaming page iteration, schema inference |
| `esql-datasource-orc` | `OrcFormatReaderTests.java`, `OrcStorageObjectAdapterTests.java` | ORC format reading |
| `esql-datasource-csv` | `CsvFormatReaderTests.java` | CSV parsing |
| `esql-datasource-s3` | (no test files found in src/test) | |
| `esql-datasource-gcs` | `GcsConfigurationTests.java`, `GcsDataSourcePluginTests.java`, `GcsStorageObjectTests.java`, `GcsStorageProviderTests.java` | Configuration parsing, storage provider behavior |
| `esql-datasource-azure` | `AzureConfigurationTests.java`, `AzureDataSourcePluginTests.java`, `AzureStorageObjectTests.java`, `AzureStorageProviderTests.java` | Configuration parsing, storage provider behavior |
| `esql-datasource-http` | `HttpStorageObjectTests.java`, `HttpStorageProviderTests.java`, `local/LocalStorageProviderTests.java` | HTTP storage access |
| `esql-datasource-iceberg` | `IcebergCatalogAdapterTests.java`, `IcebergPushdownFiltersTests.java`, `IcebergTableMetadataTests.java`, `S3ConfigurationTests.java` | Catalog metadata, filter pushdown translation |
| `esql-datasource-grpc` | `FlightConnectorFactoryTests.java`, `FlightSplitProviderTests.java`, `FlightSplitTests.java`, `AsyncConnectorFactoryFlightTests.java`, `EmployeeFlightServerTests.java`, `FlightResultCursorTests.java`, `FlightSplitCollectionSerializationTests.java` | Flight connector, split management, Arrow IPC |
| `esql-datasource-bzip2` | `Bzip2DecompressionCodecTests.java` | BZIP2 decompression |
| `esql-datasource-gzip` | `GzipDecompressionCodecTests.java` | GZIP decompression |
| `esql-datasource-zstd` | `ZstdDecompressionCodecTests.java` | ZSTD decompression |

All of these are **purely functional** -- they verify correctness of data reading, schema inference, and format handling. None measure performance.

### Per-Plugin Integration Tests (qa/)

Each datasource plugin with a `qa/` directory has integration tests:

| Plugin QA | Key Test Classes | What They Test |
|-----------|-----------------|----------------|
| `esql-datasource-parquet/qa` | `ParquetFormatSpecIT.java` (line 27), `S3GlobDiscoveryIT.java` (line 36) | End-to-end Parquet queries across all storage backends; S3 glob/listing operations |
| `esql-datasource-ndjson/qa` | `NdJsonFormatSpecIT.java`, `NdJsonCompressedFormatSpecIT.java` | NDJSON and compressed NDJSON queries |
| `esql-datasource-csv/qa` | `CsvFormatSpecIT.java`, `CsvCompressedFormatSpecIT.java` | CSV and compressed CSV queries |
| `esql-datasource-orc/qa` | `OrcFormatSpecIT.java`, `S3GlobDiscoveryIT.java` | ORC queries and glob discovery |
| `esql-datasource-gcs/qa` | `GcsGlobDiscoveryIT.java` (line 41), `GcsStorageObjectIT.java` | GCS discovery and storage object reading |
| `esql-datasource-iceberg/qa` | `IcebergSpecIT.java`, `IcebergSpecTestCase.java`, `InteractiveFixtureManual.java` | Iceberg table discovery |
| `esql-datasource-grpc/qa` | `FlightFormatSpecIT.java` | Arrow Flight end-to-end |

### Distributed External Source Tests

**`ExternalDistributedSpecIT.java`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/multi-node/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/multi_node/ExternalDistributedSpecIT.java`): Runs `external-basic.csv-spec` on a **3-node cluster**, cross-producted with three distribution strategies: `coordinator_only`, `round_robin`, `adaptive` (line 48). Key invariant: all three modes must produce identical results.

**`ExtendedDistributionPropertyTests.java`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExtendedDistributionPropertyTests.java`): Property-based unit tests for weighted round-robin distribution and split coalescing. Tests invariants like "all splits assigned exactly once" and load balancing bounds (line 36-80).

### CSV-Spec Test Files for External Sources

- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/testFixtures/src/main/resources/external-basic.csv-spec`: Core functional tests -- read all employees, filter by columns, WHERE clauses, SORT, LIMIT (100 rows of employee data).
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/testFixtures/src/main/resources/external-multifile.csv-spec`: Multi-file glob pattern tests.
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-ndjson/qa/src/javaRestTest/resources/external-ndjson.csv-spec`: NDJSON-specific tests.
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/qa/src/javaRestTest/resources/external-grpc.csv-spec`: gRPC/Flight-specific tests.

---

## 3. Testcontainers / Mock Cloud Storage Usage

**No Testcontainers, MinIO, or LocalStack are used in the ESQL external sources tests.** Instead, the tests use in-process HTTP fixture servers:

### S3: `S3HttpFixture` from `test:fixtures:s3-fixture`

- **`S3FixtureUtils`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/datasources/S3FixtureUtils.java`): Core utility class. Creates `DataSourcesS3HttpFixture` (extends `S3HttpFixture`), which starts an in-process HTTP server emulating S3 API.
- Uses `LoggingS3HttpHandler` (line 587) that wraps `S3HttpHandler` with request classification and logging.
- Blobs stored in a `ConcurrentHashMap` in memory (via `S3HttpHandler.blobs()`).
- Request tracking: `CopyOnWriteArrayList<S3RequestLog>` records every request with type, path, content length, and timestamp (line 408-440).
- Auth: Fixed access key/secret key validation via `AwsCredentialsUtils.fixedAccessKey()` (line 471).

### GCS: `GoogleCloudStorageHttpFixture` from `test:fixtures:gcs-fixture`

- **`AbstractExternalSourceSpecTestCase`** instantiates `GoogleCloudStorageHttpFixture` as a `@ClassRule` (line 179).
- Fake service account JSON generated via `TestUtils.createServiceAccount()` (line 209).
- Blobs loaded via `gcsFixture.getHandler().putBlob()`.

### Azure: `AzureHttpFixture` from `test:fixtures:azure-fixture` (inferred)

- **`AzureFixtureUtils`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/datasources/AzureFixtureUtils.java`): Creates `DataSourcesAzureHttpFixture` (extends `AzureHttpFixture`).
- Uses real Azure SDK `BlobServiceClient` pointed at the fixture endpoint (line 49-53).
- Auth: `StorageSharedKeyCredential` with test account/key (line 33).

### Fault Injection

- **`FaultInjectingS3HttpHandler`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/datasources/FaultInjectingS3HttpHandler.java`): Wraps any `HttpHandler` with configurable fault injection -- HTTP 503, HTTP 500, and connection reset. Supports countdown-based auto-clearing and path-based filtering.
- **`FaultInjectingS3HttpHandlerIT`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/multi-node/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/multi_node/FaultInjectingS3HttpHandlerIT.java`): Tests the fault injection handler itself.

### Fixture File Loading

All fixtures load from `/iceberg-fixtures` classpath resource. Compressed variants (.gz, .zst, .zstd, .bz2, .bz) are generated on the fly by `AbstractExternalSourceSpecTestCase.generateCompressedFixtures()` (line 230-256). The fixture data is the same 100-row employee dataset used across all formats and backends.

---

## 4. Rally Integration

**There are no Rally tracks or configurations for ESQL in this repository.**

The `benchmarks/README.md` (line 8-9) at `/Users/oleglvovitch/github/root/elasticsearch/benchmarks/README.md` references Rally as the macrobenchmark tool and links to `https://elasticsearch-benchmarks.elastic.co/`, but Rally tracks are maintained in a **separate repository** (`elastic/rally-tracks`), not in the Elasticsearch codebase.

The word "rally" appears in the codebase only in:
- The benchmarks README (reference to macrobenchmarks)
- Test data strings (e.g., "Borealis Rally" in `62_extra_enrich.yml` -- this is test data, not Rally configuration)
- Release notes mentioning Rally-related changes

**Conclusion**: ESQL has no Rally track. Any load testing today would need to use either (a) the JMH microbenchmark framework or (b) a custom harness.

---

## 5. JMH Benchmarks

### ESQL-Specific JMH Benchmarks

The `benchmarks/` module at `/Users/oleglvovitch/github/root/elasticsearch/benchmarks/` uses JMH. It depends on `esql`, `esql-core`, and `esql:compute` (lines 47-49 of `build.gradle`).

#### Nightly ESQL Benchmarks (in `_nightly/esql/`)

These are run on a schedule (implied by the `_nightly` package name):

1. **`QueryPlanningBenchmark.java`** (`/Users/oleglvovitch/github/root/elasticsearch/benchmarks/src/main/java/org/elasticsearch/benchmark/_nightly/esql/QueryPlanningBenchmark.java`):
   - Benchmarks parse -> analyze -> optimize pipeline for a `FROM test | LIMIT 10` query against a 10,000-field index.
   - Fork 1, Warmup 5, Measurement 10, AverageTime in milliseconds (lines 64-69).
   - Single `@Benchmark` method: `manyFields()` (line 142).

2. **`TopNBenchmark.java`** (`/Users/oleglvovitch/github/root/elasticsearch/benchmarks/src/main/java/org/elasticsearch/benchmark/_nightly/esql/TopNBenchmark.java`):
   - Benchmarks `TopNOperator` with various data types (longs, ints, doubles, booleans, bytes_refs), sort orders (asc/desc), input orderings (sorted/unsorted), and top counts (10, 1000, 4096, 10000).
   - 1024 pages of 4096 elements each per invocation (line 255).
   - Measures nanoseconds per operation (line 56).

3. **`ValuesSourceReaderBenchmark.java`** (`/Users/oleglvovitch/github/root/elasticsearch/benchmarks/src/main/java/org/elasticsearch/benchmark/_nightly/esql/ValuesSourceReaderBenchmark.java`):
   - Benchmarks `ValuesSourceReaderOperator` reading from Lucene indexes.
   - Tests different read patterns, field types, and page sizes.
   - Uses in-memory Lucene indexes (ByteBuffersDirectory).

#### Compute Operator Benchmarks (in `compute/operator/`)

4. **`AggregatorBenchmark.java`** (`/Users/oleglvovitch/github/root/elasticsearch/benchmarks/src/main/java/org/elasticsearch/benchmark/compute/operator/AggregatorBenchmark.java`):
   - Benchmarks aggregation operators (SUM, MIN, MAX, COUNT, COUNT_DISTINCT) with grouping (none, longs, ints, keywords, etc.) and filtering.

5. **`EvalBenchmark.java`** (`/Users/oleglvovitch/github/root/elasticsearch/benchmarks/src/main/java/org/elasticsearch/benchmark/compute/operator/EvalBenchmark.java`):
   - Benchmarks eval expressions: Abs, Add, DateTrunc, Equals, LessThan, RLike, CASE, MvMin, Coalesce, ToLower, ToUpper, RoundTo.

6. **`BlockBenchmark.java`**, **`BlockKeepMaskBenchmark.java`**, **`BlockReadBenchmark.java`**: Block-level data structure benchmarks.

7. **`MultivalueDedupeBenchmark.java`**: Multivalue deduplication.

8. **`ParseIpBenchmark.java`**: IP parsing.

9. **`ValuesAggregatorBenchmark.java`**: VALUES aggregation.

### How to Run

```bash
# Run all benchmarks
gradlew -p benchmarks run

# Run specific benchmark
gradlew -p benchmarks run --args 'QueryPlanningBenchmark'

# Run nightly suite (from benchmarks/)
./run.sh
```

The `run.sh` script at `/Users/oleglvovitch/github/root/elasticsearch/benchmarks/run.sh` defines the nightly suite (lines 37-44):
```bash
run 'esql_agg' 'AggregatorBenchmark ...'
run 'esql_block_keep_mask' 'BlockKeepMaskBenchmark ...'
run 'esql_block_read' 'BlockReadBenchmark ...'
run 'esql_eval' 'EvalBenchmark'
run 'esql_parse_ip' 'ParseIpBenchmark'
run 'esql_topn' 'TopNBenchmark'
run 'esql_values_agg' 'ValuesAggregatorBenchmark'
run 'esql_values_source_reader' 'ValuesSourceReaderBenchmark'
```

### What Is NOT Benchmarked

There are **no JMH benchmarks** for:
- Format readers (Parquet, NDJSON, ORC, CSV)
- Storage providers (S3, GCS, Azure, HTTP)
- Split discovery or distribution
- End-to-end external source queries
- Compression codec performance (ZSTD, GZIP, BZIP2)

---

## 6. Performance Measurement in Existing Tests

### The answer: Almost none

The existing test infrastructure is **purely functional**. Specific findings:

1. **`TookMetrics` verification** (in `EsqlSpecTestCase`, lines 358-393): The csv-spec runner randomly verifies that the `took` histogram bucket in `_xpack/usage` is incremented after a query. This validates the telemetry pipeline, not query performance. It does not record or assert on absolute timing.

2. **`ProfileLogger`** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/ProfileLogger.java`): Captures ESQL profile data (operator timings, page counts) from the response and logs it **only on test failure** (line 56-58). Profile data is available in every response when `profile: true` is set, but is not systematically collected or analyzed.

3. **S3 request tracking** (in `S3FixtureUtils`): The `S3RequestLog` records include timestamps (line 276, `System.currentTimeMillis()`), and `printRequestSummary()` (line 201) counts requests by type. This could theoretically be used for performance analysis (e.g., "this query made N GET_OBJECT requests") but is currently only used for debugging.

4. **No throughput, latency, or memory measurements** exist anywhere in the external source test infrastructure.

---

## 7. What a Load Testing Framework Needs (and What Exists to Build On)

### Existing Infrastructure to Build On

| Component | Location | Reusability |
|-----------|----------|-------------|
| **In-process S3/GCS/Azure fixtures** | `S3FixtureUtils`, `AzureFixtureUtils`, `GoogleCloudStorageHttpFixture` | High -- already start in-process HTTP servers with configurable data; no external services needed |
| **Fixture data loading** | `AbstractExternalSourceSpecTestCase.loadExternalSourceFixtures()` | High -- loads Parquet/NDJSON/CSV/ORC fixtures into all backends automatically |
| **S3 request logging** | `S3FixtureUtils.S3RequestLog` | Medium -- records request type, path, content length, timestamp. Could be extended with response size and latency |
| **Fault injection** | `FaultInjectingS3HttpHandler` | High -- already supports 503/500/connection-reset with countdown and path filtering |
| **Cluster configuration** | `Clusters.java` (per-plugin) | High -- configures multi-plugin single/multi-node clusters with all required settings |
| **JMH infrastructure** | `benchmarks/` module with `run.sh` | High -- established build, runner, and CI integration for JMH benchmarks |
| **Profile data** | ESQL `profile: true` response parameter | High -- contains per-operator timings, page counts, memory usage |
| **Compression generation** | `AbstractExternalSourceSpecTestCase.compress()` | Medium -- generates .gz/.zst/.bz2 variants on the fly |

### What Needs to Be Built

1. **Data generation at scale**: The current fixture is 100 rows of employee data. Load testing needs:
   - Configurable row counts (1K, 100K, 1M, 10M)
   - Configurable column counts and types
   - Multi-file datasets (e.g., 100 Parquet files x 100K rows)
   - Generated Parquet with configurable row group sizes (for row-group-level benchmarking)

2. **Measurement harness**: A test runner that:
   - Executes queries N times with warmup
   - Records per-query latency (wall clock, P50/P95/P99)
   - Captures memory usage (circuit breaker, heap)
   - Records S3/GCS/Azure request counts and bytes transferred
   - Captures ESQL profile data (operator timings, page counts)
   - Compares results across runs for regression detection

3. **Scenarios**: Specific load test scenarios:
   - **Format reader throughput**: Parquet vs NDJSON vs CSV reading speed for same data
   - **Compression overhead**: Raw vs GZIP vs ZSTD vs BZIP2 for same data
   - **Storage latency**: S3 vs GCS vs Azure vs HTTP vs LOCAL for same data (fixture-based, so really measuring client overhead)
   - **Distribution scaling**: 1-node vs 3-node vs N-node for same query, measuring split assignment overhead
   - **Large file streaming**: Single large file (1GB+) streaming without OOM
   - **Multi-file fan-out**: 100+ files with glob discovery
   - **Filter pushdown benefit**: Queries with/without pushable filters on Parquet row groups
   - **Concurrent queries**: Multiple simultaneous external source queries

4. **Two possible approaches**:

   **Approach A: JMH Microbenchmarks** (fits existing infra)
   - Add benchmarks in `benchmarks/src/main/java/org/elasticsearch/benchmark/_nightly/esql/` for format readers and compute operators with external source data
   - Pro: Uses existing JMH infrastructure, CI integration, and `run.sh`
   - Con: Cannot test end-to-end (no real cluster), cannot test distribution or S3 client behavior

   **Approach B: Integration Load Tests** (new harness)
   - Add a new QA module (e.g., `esql/qa/server/load-test/`) with `ESRestTestCase`-based tests
   - Use existing fixtures but with larger generated data
   - Run queries N times, collect profile data, assert on timing bounds
   - Pro: Tests end-to-end including S3 client, distribution, and REST layer
   - Con: Slower, noisier measurements, harder to detect small regressions

   **Recommended: Both.** JMH for format reader throughput (Approach A is easy -- add Parquet/NDJSON reader benchmarks). Integration harness for end-to-end scenarios (Approach B is more work but catches real issues).

### Minimal Viable Load Test Harness

The simplest useful starting point would be:
1. A new JMH benchmark class `ParquetReaderBenchmark` in `benchmarks/_nightly/esql/` that benchmarks `ParquetFormatReader.read()` with various file sizes and column counts
2. A new integration test class `ExternalSourceLoadTestIT` in `esql/qa/server/single-node/` that:
   - Generates Parquet files of configurable size
   - Runs the same query 10 times
   - Records `took` from each response
   - Records ESQL profile data
   - Records S3 request counts from the fixture
   - Logs a summary table
   - Optionally asserts on regression bounds

Both would build directly on existing infrastructure with no new dependencies.
