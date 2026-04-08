# Deep Dive: Distributed Execution Tests for ES|QL External Data Sources

Date: 2026-03-03

## 1. Tracking Issue and PRs

The distributed execution work was tracked by **#142996** (ESQL: Distributed execution for external data sources - Meta Issue), now CLOSED. It comprised 6 PRs merged Feb 25-27, plus 2 follow-up PRs:

| PR | Title | Tests Added |
|----|-------|-------------|
| #143005 | Split SPI, Hive partition detection, filter hint extraction | `FileSplitProviderTests`, `SplitDiscoveryPhaseTests` |
| #143114 | Split discovery phase, L1 partition pruning | `SplitDiscoveryPhaseErrorTests` |
| #143120 | Pluggable partition detection and virtual columns | (wired into existing partition tests) |
| #143154 | Local parallelism (ExternalSliceQueue, multiple drivers) | `ExternalSourceParallelismTests` |
| #143194 | Distribution strategy and plan structure | `AdaptiveStrategyTests`, `ExternalDistributionTests`, `ExternalDistributionPropertyTests` |
| #143209 | Data node external source execution | `ExternalSourceDataNodeTests`, `DataNodeComputeHandlerExternalErrorTests` |
| #143335 | Split coalescing for many small files | `CoalescedSplit` tests integrated into `ExtendedDistributionPropertyTests` |
| #143349 | External source parallel execution and distribution | `WeightedRoundRobinStrategyTests`, `RangeStorageObjectTests`, `FileSplitProviderTests` (sub-file splits), `FlightSplitCollectionSerializationTests` |
| #143420 | Extended distribution tests and fault injection | `ExtendedDistributionPropertyTests`, `FaultInjectionRetryTests`, `FaultInjectingS3HttpHandlerIT` |

## 2. Test File Inventory

### 2.1 Unit Tests (`x-pack/plugin/esql/src/test/`)

#### Split Discovery and Partition Pruning

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `datasources/SplitDiscoveryPhaseTests.java` | 9 | Split resolution from physical plan; filter hint collection from FilterExec above ExternalSourceExec; multiple sources get own splits; unknown source type defaults; nested filters accumulate |
| `datasources/SplitDiscoveryPhaseErrorTests.java` | 5 | Error wrapping with source context (path, type); UncheckedIOException, RuntimeException, SecurityException handling; ElasticsearchException not double-wrapped |
| `datasources/FileSplitProviderTests.java` | ~25 | N files produce N splits; partition value attachment; format extraction; empty/unresolved FileSet; config passthrough; L1 partition pruning with `Equals`, `GreaterThanOrEqual`, `LessThan`, `NotEquals`, `In`; combined year+month filters; non-partition column filter no-op; sub-file splitting for CSV/NDJSON; splittable format detection; Parquet files not split |

#### Distribution Strategy

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `plugin/AdaptiveStrategyTests.java` | 8 | Single split stays local; empty splits local; aggregation with multiple splits distributes; limit-only stays local; many splits no-agg distributes; few splits no-agg stays local; no eligible nodes returns local; even round-robin distribution |
| `plugin/ExternalDistributionTests.java` | 11 | Mapper creates single-mode AggregateExec; no Exchange for limit+external; context validation (null rejection); strategy resolution from pragma (adaptive/coordinator_only/round_robin); Exchange collapsing removes ExchangeExec wrapping ExternalSourceExec; collapse preserves non-external exchanges; collapse preserves splits |
| `plugin/ExternalDistributionPropertyTests.java` | 11 | **Randomized property tests**: all splits assigned exactly once; max load imbalance bounded; deterministic; single split always local; empty nodes returns local; empty splits returns local; round-robin assigns to all nodes; more nodes than splits; adaptive distributes with agg; coordinator-only never distributes; strategy resolution from pragma |
| `plugin/ExtendedDistributionPropertyTests.java` | 8 | **Weighted round-robin**: all splits assigned exactly once; load balancing (LPT bound guarantee); deterministic; single large split goes to least loaded. **Coalescing**: preserves all files; below threshold is no-op; coalesced split size equals children sum. **Integration**: coalesced splits distribute correctly; round-robin and weighted agree on completeness |
| `plugin/WeightedRoundRobinStrategyTests.java` | N/A | (not read, but exists) |

#### Data Node Execution

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `plugin/ExternalSourceDataNodeTests.java` | 18 | Distribution result with/without splits/aggregation; coordinator-only pragma override; round-robin pragma distribution; plan breaks at exchange boundary; split injection into ExchangeSinkExec; preserves other fields; node assignment (even/uneven); dispatch conditions; full distribution flow builds correct assignments; plan breaks correctly for data nodes with aggregation; collapse preserves splits |
| `plugin/DataNodeComputeHandlerExternalErrorTests.java` | 5 | Error message format for missing nodes; distribution plan assigns only known nodes; assignments sum to total splits; split discovery error wraps source context; stale node detection when cluster shrinks |

#### Local Parallelism

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `datasources/ExternalSourceParallelismTests.java` | 5 | Multiple drivers read all splits via ExternalSliceQueue (concurrent with thread pool); driver parallelism matches split count; single split no parallelism; empty splits no parallelism; result correctness across drivers (unique values); slice queue with partition values |

#### Operator Infrastructure

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `datasources/AsyncExternalSourceOperatorFactoryTests.java` | ~15 | Constructor validation; sync-wrapper vs native-async mode descriptions; accessors; operator creation in both modes; multi-file iteration; unresolved FileSet fallback; read error propagation; slice queue reads splits sequentially; slice queue exhaustion; multiple drivers claim different splits |
| `datasources/AsyncExternalSourceBufferTests.java` | 12 | Constructor validation; add/poll page; backpressure (waitForSpace when full, waitForWriting when full, waitForReading when empty); finish; finish with draining; onFailure; addPage after finish/failure; completion listener on success/failure |
| `datasources/ExternalSourceOperatorFactoryTests.java` | 3 | Mock storage+format integration; factory validation; describe output |
| `datasources/RangeStorageObjectTests.java` | N/A | (exists, not read - sub-file range reads) |

#### Resolution and Schema

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `datasources/ExternalSourceResolverTests.java` | ~15 | FIRST_FILE_WINS schema resolution; FileSet threading; glob no-match throws; single-file returns UNRESOLVED FileSet; schema type preservation; multiple paths resolved independently; config passthrough; partition columns appended at tail; partition column conflict (partition wins); multiple partition columns; enrichSchemaWithPartitionColumns |

#### Serialization

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `plan/physical/ExternalSourceExecSerializationTests.java` | (inherits) | Random ExternalSourceExec with splits serialization round-trip; mutation testing |
| `plan/logical/ExternalRelationTests.java` | 10 | FileSet threading through constructors, toPhysicalExec, withAttributes, withPushedFilter, equals/hashCode; ExternalSourceExec FileSet preservation |

#### Fault Injection

| File | Test Count | What It Tests |
|------|-----------|---------------|
| `datasources/FaultInjectionRetryTests.java` | 7 | RetryPolicy: transient faults recover within budget; persistent faults exhaust budget; connection reset retried; 403 non-retryable fails immediately; timeout retried; countdown fault pattern; zero-retries policy; successful call returns immediately |

### 2.2 Multi-Node Integration Tests (`x-pack/plugin/esql/qa/server/multi-node/`)

| File | What It Tests |
|------|---------------|
| `ExternalDistributedSpecIT.java` | **End-to-end multi-node CSV-spec tests**: Runs `external-basic.csv-spec` (15 query patterns) on a 3-node cluster. Cross-products every test with 3 distribution modes (coordinator_only, round_robin, adaptive). Key invariant: all three modes produce identical results. Uses S3HttpFixture. |
| `ExternalDistributedClusters.java` | Cluster configuration: 3-node cluster with repository-s3, repository-gcs, Arrow JVM args |
| `FaultInjectingS3HttpHandlerIT.java` | **HTTP-level fault injection**: Tests FaultInjectingS3HttpHandler with a real HTTP server. 503/500 injection; countdown decrement; path-based filtering; clear fault |

### 2.3 Shared Test Infrastructure

| File | What It Provides |
|------|-----------------|
| `AbstractExternalSourceSpecTestCase.java` | Base class for all external source integration tests. Template-based query transformation (`{{employees}}` -> actual paths). Storage backend cross-product (S3, HTTP, GCS, Azure, LOCAL). S3/GCS/Azure fixture setup with compressed variants (.gz, .zst, .bz2). |
| `S3FixtureUtils` / `DataSourcesS3HttpFixture` | S3 HTTP fixture with blob storage, request logging, unsupported-operation detection |
| `AzureFixtureUtils` / `DataSourcesAzureHttpFixture` | Azure HTTP fixture |
| `GoogleCloudStorageHttpFixture` (from test:fixtures:gcs-fixture) | GCS HTTP fixture with OAuth2 token support |
| `FaultInjectingS3HttpHandler` | Wraps S3HttpHandler with configurable fault injection (503, 500, path filter, countdown) |

### 2.4 CSV-Spec Test Data

`external-basic.csv-spec` contains 15 test queries:
- `readAllEmployees` - full scan with 10 columns
- `selectSpecificColumns` - column projection
- `filterByEmployeeNumber` - equality filter
- `filterBySalaryRange` - range filter (AND)
- `filterByGender` - keyword equality filter
- `filterByEmploymentStatus` - boolean filter
- `aggregateCount` - COUNT(*)
- `aggregateByGender` - COUNT(*) BY gender (includes null group)
- `aggregateAverageSalary` - AVG()
- `aggregateSalaryStats` - MIN/MAX/AVG
- `aggregateSalaryByGender` - AVG + COUNT BY gender
- `filterAndSort` - WHERE + SORT DESC
- `evalComputedColumn` - EVAL with arithmetic
- `complexQuery` - WHERE + EVAL(CASE) + STATS BY
- `selectAdditionalColumns` / `selectHeightVariants` - sub-field columns

When run via `ExternalDistributedSpecIT`, each of these 15 queries is executed 3 times (one per distribution mode) across 5 storage backends (S3, HTTP, GCS, Azure, LOCAL) = **up to 225 parameterized test cases**.

### 2.5 Plugin-Level Tests

Each datasource plugin has its own test suite:

| Plugin | Tests |
|--------|-------|
| `esql-datasource-s3` | S3 configuration, storage object, storage provider |
| `esql-datasource-gcs` | GCS configuration, plugin, storage object, storage provider |
| `esql-datasource-azure` | Azure configuration, plugin, storage object, storage provider |
| `esql-datasource-http` | HTTP storage object/provider, local storage provider |
| `esql-datasource-parquet` | Parquet format reader, storage object adapter |
| `esql-datasource-csv` | CSV format reader |
| `esql-datasource-ndjson` | NDJSON schema inferrer, page iterator |
| `esql-datasource-orc` | ORC format reader, storage object adapter |
| `esql-datasource-grpc` | Flight connector factory, split provider, result cursor, serialization, async tests, employee server |
| `esql-datasource-zstd` | Decompression codec |
| `esql-datasource-gzip` | Decompression codec |
| `esql-datasource-bzip2` | Decompression codec |

## 3. Test Coverage Assessment

### 3.1 What IS Tested

| Scenario | Coverage Level | Test Type |
|----------|---------------|-----------|
| **Split discovery from physical plan** | Thorough | Unit |
| **Filter hint collection and L1 partition pruning** | Thorough | Unit (Equals, >=, <, !=, IN, combined) |
| **Split count matches file count** | Thorough | Unit + randomized |
| **Sub-file splitting (CSV/NDJSON)** | Good | Unit |
| **All three distribution strategies** | Thorough | Unit + property + integration |
| **Strategy selection from pragma** | Thorough | Unit |
| **Round-robin completeness (all splits assigned)** | Thorough | Randomized property tests (1-200 splits, 1-20 nodes) |
| **Round-robin load balancing** | Thorough | Property tests (bounded imbalance) |
| **Weighted round-robin (LPT bound)** | Thorough | Property tests |
| **Split coalescing** | Thorough | Property tests + integration with distribution |
| **Determinism** | Thorough | Property tests |
| **Single split stays local** | Thorough | Multiple strategy tests |
| **Empty splits / empty nodes edge cases** | Thorough | Unit + property |
| **More nodes than splits** | Good | Property test |
| **Plan structure (Exchange insertion/removal)** | Thorough | Unit |
| **Plan break between coordinator and data node** | Thorough | Unit |
| **Split injection into ExchangeSinkExec** | Thorough | Unit (direct, through LimitExec, through AggregateExec) |
| **Async operator (sync wrapper + native async)** | Good | Unit |
| **Buffer backpressure** | Good | Unit |
| **Multi-file iteration** | Good | Unit |
| **Multi-driver parallelism via ExternalSliceQueue** | Good | Unit (concurrent, correctness) |
| **Error propagation (IO, security, runtime)** | Good | Unit |
| **Fault injection and retry** | Good | Unit + integration |
| **Non-retryable errors (403) fail immediately** | Good | Unit |
| **Serialization (ExternalSourceExec with splits)** | Good | Framework tests |
| **FileSet threading through plan nodes** | Thorough | Unit |
| **End-to-end with real queries (3-node cluster)** | Good | Integration (15 queries x 3 modes x 5 backends) |
| **S3/GCS/Azure/HTTP/LOCAL storage backends** | Good | Integration |
| **Compression codecs** | Good | Integration (fixtures generate .gz, .zst, .bz2) |
| **Stale node detection** | Basic | Unit (verifies detection, not recovery) |

### 3.2 What is NOT Tested (Gaps)

| Gap | Severity | Notes |
|-----|----------|-------|
| **Node failure during execution** | HIGH | No test simulates a data node going down mid-execution. The stale node test in `DataNodeComputeHandlerExternalErrorTests` only verifies detection, not recovery/retry. No `internalClusterTest` that kills nodes during external source execution. |
| **No internalClusterTest for external sources** | HIGH | Zero internalClusterTests dedicated to external source distribution. `CsvIT` references external sources but only for pass-through. All distributed tests are either unit tests with mocked nodes or REST integration tests. There is no test that verifies the actual `DataNodeComputeHandler` code path on real data nodes within a single JVM cluster. |
| **Circuit breaker integration** | MEDIUM | No tests verify memory limits are enforced during distributed external source execution. This is listed as a GA milestone item. |
| **Thread pool isolation** | MEDIUM | No tests verify external source work runs on a dedicated thread pool rather than the search pool. Also a GA milestone item. |
| **Large split counts at scale** | MEDIUM | Property tests go up to 200-500 splits. No test exercises 10,000+ splits to verify no performance degradation in distribution planning. |
| **Split size-aware distribution** | MEDIUM | `WeightedRoundRobinStrategy` exists and is tested, but there is no integration test that verifies the adaptive strategy picks weighted vs round-robin based on split size variance. |
| **Mixed external + Lucene queries** | MEDIUM | `testExternalDispatchConditionWithSplitsAndShards` verifies the dispatch condition but there is no integration test that runs a query combining external data and ES indices. |
| **Network partition / slow data node** | MEDIUM | Fault injection is only at the S3 HTTP level. No test simulates slow/unresponsive data nodes in the transport layer. |
| **Drain timeout** | LOW | No test verifies graceful shutdown during external source distributed execution. Listed as GA item. |
| **Virtual column injection on data nodes** | LOW | `VirtualColumnInjector` is tested indirectly via the ExternalSourceParallelismTests (partition values test), but not specifically in a distributed data-node context. |
| **Filter pushdown on data nodes (L2)** | LOW | `FilterPushdownRegistry` is mentioned as wired on data nodes in #143209 but no test exercises L2 filter translation during distributed execution. |
| **Connector-based sources (Flight/JDBC)** | LOW | Flight has its own tests (`FlightSplitProviderTests`, `FlightSplitCollectionSerializationTests`, etc.) but these connectors are not wired into the distribution infrastructure and are disabled for MVP. |
| **Adaptive strategy threshold tuning** | LOW | The threshold for when adaptive switches from local to distributed (split count, aggregation presence) is tested but the specific threshold values are not parameterized/fuzz tested. |
| **Concurrent split discovery** | LOW | No test exercises split discovery with concurrent storage listing calls. |

## 4. Test Fixtures Summary

### S3 Fixture
- `DataSourcesS3HttpFixture` (extends `S3HttpFixture`) - in-process HTTP server mimicking S3 API
- Pre-loaded with `employees.parquet`, `employees.csv`, `employees.ndjson` and their compressed variants
- Request logging for asserting access patterns
- `FaultInjectingS3HttpHandler` - wraps S3 handler with configurable HTTP 500/503 injection, path filtering, countdown

### GCS Fixture
- `GoogleCloudStorageHttpFixture` (from `test:fixtures:gcs-fixture`) - in-process HTTP server mimicking GCS API
- OAuth2 token simulation
- Blob-level storage

### Azure Fixture
- `DataSourcesAzureHttpFixture` (from `AzureFixtureUtils`) - in-process HTTP server mimicking Azure Blob Storage API
- SharedKey authentication

### Compression
- Compressed variants (.gz, .zst, .zstd, .bz2, .bz) generated on-the-fly from .csv and .ndjson fixtures in `@BeforeClass`
- Written to S3, GCS, Azure fixtures and local filesystem

### No Docker/TestContainers
- All fixtures are in-process HTTP servers (no Docker, no MinIO, no LocalStack, no Azurite)
- This keeps tests fast and deterministic but means we are testing against fixture implementations, not real cloud services

## 5. Flaky/Muted Tests

From git log:
- `ExternalSourceOperatorFactoryTests.testCreateOperatorWithMockedStorageAndFormat` (#142632) - was muted, appears fixed
- `AnalyzerTests.testResolveExternalRelationUnresolvedFileSet` (#142796) - was muted
- `AnalyzerTests.testResolveExternalRelationPassesFileSet` (#142795) - was muted
- `GenerativeIT` is muted for both single-node (#143023) and multi-node (#143023) - these are generative tests, not specific to external sources

## 6. Open Issues

From `gh issue list --label "ES|QL|DS"`:
- **#143279**: ESQL: Unified data source SPI - Meta Issue (the proposal work)
- **#143276**: ESQL: Data sources: Iceberg end-to-end
- **#143277**: ESQL: Data sources: FROM datasource::expression syntax
- **#143278**: ESQL: Data sources: SQL pushdown proof of concept
- **#143275**: ESQL: Data sources: Rename External* -> DataSource*
- **#143274**: ESQL: Data sources: Clean up old interface
- **#143273**: ESQL: Data sources: Connector assembly
- **#143272**: ESQL: Data sources: Lakehouse assembly
- **#143271**: ESQL: Data sources: Wire the base contract

None of these are specifically about test coverage gaps, but the GA roadmap (from `/tmp/v1-product-shape.md`) explicitly calls out "distributed execution tests" as a GA item.

## 7. Architecture of the Test Suite

```
Unit Tests (fast, mocked)
  |
  +-- Split SPI layer
  |     SplitDiscoveryPhaseTests, FileSplitProviderTests
  |
  +-- Distribution strategy layer
  |     AdaptiveStrategyTests, ExternalDistributionPropertyTests,
  |     ExtendedDistributionPropertyTests (weighted, coalescing)
  |
  +-- Plan structure layer
  |     ExternalDistributionTests (mapper, exchange collapse)
  |     ExternalSourceDataNodeTests (plan break, split injection)
  |
  +-- Execution layer
  |     ExternalSourceParallelismTests (multi-driver, ExternalSliceQueue)
  |     AsyncExternalSourceOperatorFactoryTests (operator lifecycle)
  |     AsyncExternalSourceBufferTests (backpressure)
  |
  +-- Error handling
  |     SplitDiscoveryPhaseErrorTests, DataNodeComputeHandlerExternalErrorTests,
  |     FaultInjectionRetryTests
  |
  +-- Serialization
        ExternalSourceExecSerializationTests, ExternalRelationTests

Integration Tests (real cluster, real HTTP fixtures)
  |
  +-- ExternalDistributedSpecIT
  |     3-node cluster, 3 distribution modes, 5 storage backends,
  |     15 query patterns = up to 225 test cases
  |
  +-- FaultInjectingS3HttpHandlerIT
        Real HTTP server fault injection
```

## 8. Recommendations for Closing Gaps

### High Priority (for GA)

1. **InternalClusterTest for distributed external execution**: Create a test that starts a multi-node cluster in-JVM and verifies that external source splits are actually dispatched to data nodes and results come back correctly through the exchange infrastructure. This would exercise the real `DataNodeComputeHandler.handleExternalSourceRequest()` code path.

2. **Node failure during execution**: Use `internalClusterTest` with `ensureStableCluster()` checks and node restart/stop to verify the system handles data node loss gracefully (either fails the query with a clear error or retries on remaining nodes).

3. **Circuit breaker integration test**: Create a test that reads a large enough external source to trigger the circuit breaker and verify the query fails cleanly without OOM.

### Medium Priority

4. **Thread pool isolation test**: Verify external source work runs on the expected thread pool and does not starve the search thread pool.

5. **Large-scale split distribution**: Add a property test with 10,000+ splits to verify distribution planning remains fast.

6. **Mixed external + index query**: Test `EXTERNAL "s3://..." | LOOKUP my_index ON key` or similar mixed queries.

### Lower Priority

7. **L2 filter pushdown on data nodes**: Create a test that verifies filters are translated and applied per-node during distributed execution.

8. **Real cloud integration tests**: Consider adding optional CI tests against MinIO/Azurite/GCS-emulator Docker containers for more realistic testing (separate test suite, not blocking CI).

## 9. Summary

The distributed execution test suite is **comprehensive at the unit level** -- particularly strong in:
- Randomized property testing of split assignment invariants
- Strategy selection and edge cases
- Plan structure and serialization
- Operator lifecycle and backpressure
- Error handling and context preservation

The **integration test** (`ExternalDistributedSpecIT`) provides good end-to-end coverage across storage backends and distribution modes with real HTTP fixtures.

The main gap is the **absence of internalClusterTests** that exercise the actual distributed code path (real transport layer, real DataNodeComputeHandler). All distribution tests either use mocked DiscoveryNodes or REST API calls. The GA milestone should prioritize closing this gap, along with circuit breaker and thread pool isolation tests.

Total test count estimate: ~180+ individual test methods across 20+ test classes, covering the full distributed execution stack from split discovery through distribution planning to data node execution.
