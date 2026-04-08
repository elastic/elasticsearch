# Deep Dive: Load & Performance Testing Infrastructure for ES|QL External Data Sources

## Executive Summary

JMH is indeed not going to be enough. JMH benchmarks in the repo are all **microbenchmarks** -- they measure individual operators (TopN, Aggregator, BlockRead, Eval) or planning steps in isolation, without network I/O, without object stores, without distributed execution. For external data sources, the performance-critical path is: network latency to object store + data transfer + decompression + deserialization + compute pipeline. JMH cannot exercise that path.

The repo already has substantial infrastructure that can be built upon, but there is a clear gap: **no load/performance test harness exists for external data sources today**. What follows is a detailed inventory of what exists and a practical blueprint for what needs to be built.

---

## 1. Rally: Elastic's Macrobenchmarking Tool

### What It Is

Rally is Elastic's official macrobenchmarking framework. It runs nightly against the latest Elasticsearch snapshot build from main. Results are published at https://elasticsearch-benchmarks.elastic.co/.

Key references:
- Repo: https://github.com/elastic/rally
- Tracks: https://github.com/elastic/rally-tracks
- TESTING.asciidoc (line ~797): "For changes that might affect the performance characteristics of Elasticsearch you should also run macrobenchmarks... Rally"

### ES|QL Support in Rally

**Rally 2.10.0 added an `esql` operation type** (PR #1791). This means Rally can send ES|QL queries to an Elasticsearch cluster and measure latency/throughput.

### Existing ES|QL Rally Track

There IS an existing `esql/` track in `elastic/rally-tracks`:
- `track.json` -- track configuration with 20 index mapping files (NYC public datasets)
- `challenges/` directory -- test scenarios (including `default.json` and `querys-searchable-snapshot.json`)
- `operations/` directory -- query definitions (`default.json`, `logs.json`)
- 20 `idx_mapping*.json` files -- structured data schemas

The existing track tests ES|QL queries **against Elasticsearch indices** (standard indexed data). It does NOT test external data sources.

### Can Rally Be Extended for External Sources?

**Yes, but with significant work.** The practical approach:

1. **Custom Rally Track**: Create a new rally track (e.g., `esql-external/`) that:
   - Defines `esql` operations with `EXTERNAL "s3://..."` queries
   - Parameterizes the S3/GCS/Azure endpoint, bucket, path, format
   - Defines challenges for different query patterns (scan-all, filter, aggregate, TopN)

2. **Infrastructure requirement**: Rally needs a real or emulated object store endpoint. Options:
   - **MinIO container** alongside the Rally benchmark run (MinIO is already used in the repo)
   - **Real S3/GCS/Azure** (for production-representative latency, but $$)
   - **S3HttpFixture** from the test infrastructure (lightweight, but single-threaded Java HTTP server -- not representative of production latency)

3. **Data generation**: Rally expects pre-generated corpora. Need a data generator that produces Parquet/NDJSON files at scale (100MB, 1GB, 10GB sizes).

4. **What Rally measures well**: Throughput (queries/sec), latency percentiles (p50/p90/p95/p99), error rates, GC pauses. This is exactly what we need.

5. **What Rally does NOT measure**: Internal pipeline metrics (time in decompression vs. deserialization vs. compute), memory pressure, network bytes transferred. For those, we need profiling or internal instrumentation.

---

## 2. Existing Test Infrastructure on Main

### 2.1 Integration Test Framework (csv-spec based)

The external source test infrastructure is well-developed. Here is the class hierarchy:

```
EsqlSpecTestCase (base for all csv-spec tests)
  └─ AbstractExternalSourceSpecTestCase (base for external source tests)
       ├─ ParquetFormatSpecIT  (x-pack/plugin/esql-datasource-parquet/qa/)
       ├─ CsvFormatSpecIT      (x-pack/plugin/esql-datasource-csv/qa/)
       ├─ NdJsonFormatSpecIT   (x-pack/plugin/esql-datasource-ndjson/qa/)
       ├─ OrcFormatSpecIT      (x-pack/plugin/esql-datasource-orc/qa/)
       ├─ CsvCompressedFormatSpecIT
       ├─ NdJsonCompressedFormatSpecIT
       ├─ IcebergSpecTestCase → IcebergSpecIT
       └─ ExternalDistributedSpecIT (multi-node, 3 distribution modes)
  FlightFormatSpecIT (gRPC/Arrow Flight, uses EmployeeFlightServer)
```

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/AbstractExternalSourceSpecTestCase.java`

Key features:
- **5 storage backends**: S3, HTTP, LOCAL, GCS, Azure -- every test is cross-producted against all backends
- **Template-based queries**: `{{employees}}` gets replaced with the correct URI per backend
- **Credential injection**: S3 endpoint/access_key/secret_key, GCS credentials/project_id/token_uri, Azure account/key -- all injected automatically
- **Compression generation**: `.gz`, `.zst`, `.zstd`, `.bz2`, `.bz` variants generated on the fly from .csv and .ndjson fixtures
- **Request logging**: `S3RequestLog` tracks every S3 operation (GET_OBJECT, HEAD_OBJECT, LIST_OBJECTS_V2, etc.)
- **Request summary**: `printRequestSummary()` shows operation counts -- useful for verifying I/O efficiency

### 2.2 Test Fixtures (In-Process Object Store Emulators)

The test fixtures are **NOT containerized** -- they are lightweight in-process HTTP servers:

| Storage | Fixture Class | How It Works |
|---------|--------------|--------------|
| **S3** | `DataSourcesS3HttpFixture` (extends `S3HttpFixture`) | In-process HTTP server implementing S3 API subset. Blobs stored in `ConcurrentHashMap`. |
| **GCS** | `GoogleCloudStorageHttpFixture` | In-process HTTP server implementing GCS JSON API. Includes fake OAuth2 token endpoint. |
| **Azure** | `DataSourcesAzureHttpFixture` (extends `AzureHttpFixture`) | In-process HTTP server implementing Azure Blob REST API with SharedKey auth. |
| **MinIO** | `MinioTestContainer` | **Docker container** running real MinIO server. Used only by `repository-s3` tests, NOT by ESQL external source tests. |

**Key files**:
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/datasources/S3FixtureUtils.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/datasources/AzureFixtureUtils.java`
- `/Users/oleglvovitch/github/root/elasticsearch/test/fixtures/minio-fixture/src/main/java/org/elasticsearch/test/fixtures/minio/MinioTestContainer.java`

**Important distinction**: The S3/GCS/Azure fixtures are zero-latency in-memory servers. They are excellent for correctness testing but **useless for performance testing** because:
- No network latency (same JVM or localhost)
- No bandwidth constraints
- No retry/backoff behavior
- No concurrent access contention

### 2.3 Test Data

Fixture data is small -- by design. Located at:
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/resources/iceberg-fixtures/` -- Iceberg table with employees data (< 1MB total)
- `standalone/employees.parquet` -- single Parquet file with 100 employee records
- `employees/metadata/` -- full Iceberg metadata (v1.metadata.json, manifest avro files)
- `multifile/` -- not yet populated (tests marked `-Ignore`)

Data generators exist:
- `OrcFixtureGenerator` (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/qa/src/orcFixtureGenerator/java/org/elasticsearch/xpack/esql/qa/orc/OrcFixtureGenerator.java`) -- reads canonical `employees.csv`, generates `.orc` file

### 2.4 Distribution Testing

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/multi-node/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/multi_node/ExternalDistributedSpecIT.java`

Tests 3-node cluster with all distribution strategies:
- `coordinator_only` -- all work on coordinator
- `round_robin` -- splits distributed round-robin to data nodes
- `adaptive` -- smart distribution based on split characteristics

Each csv-spec test is run 3x (once per mode). The invariant: all modes must produce identical results.

### 2.5 Glob Discovery Tests

- `S3GlobDiscoveryIT` (Parquet QA, ORC QA) -- tests `*.parquet` glob matching against S3 fixture
- `GcsGlobDiscoveryIT` -- same for GCS
- `GcsStorageObjectIT` -- GCS storage provider unit tests

---

## 3. Existing Benchmarks in ESQL

### 3.1 JMH Microbenchmarks (benchmarks/ directory)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/benchmarks/build.gradle`

ESQL-specific JMH benchmarks:

| Benchmark | What It Measures | File |
|-----------|-----------------|------|
| `QueryPlanningBenchmark` | Parse → Analyze → Optimize cycle for 10K-field index | `benchmarks/src/main/java/.../benchmark/_nightly/esql/QueryPlanningBenchmark.java` |
| `TopNBenchmark` | TopN operator throughput | `benchmarks/src/main/java/.../benchmark/_nightly/esql/TopNBenchmark.java` |
| `ValuesSourceReaderBenchmark` | Field value extraction from Lucene | `benchmarks/src/main/java/.../benchmark/_nightly/esql/ValuesSourceReaderBenchmark.java` |
| `AggregatorBenchmark` | Aggregation operators (count, sum, min, max, count_distinct) | `benchmarks/src/main/java/.../benchmark/compute/operator/AggregatorBenchmark.java` |
| `EvalBenchmark` | Eval operators (abs, date_trunc, equal_to, rlike) | `benchmarks/src/main/java/.../benchmark/compute/operator/EvalBenchmark.java` |
| `BlockReadBenchmark` | Block data structure read performance | `benchmarks/src/main/java/.../benchmark/compute/operator/BlockReadBenchmark.java` |
| `BlockKeepMaskBenchmark` | Block column pruning | `benchmarks/src/main/java/.../benchmark/compute/operator/BlockKeepMaskBenchmark.java` |
| `ParseIpBenchmark` | IP parsing | `benchmarks/src/main/java/.../benchmark/compute/operator/ParseIpBenchmark.java` |
| `ValuesAggregatorBenchmark` | VALUES aggregation | `benchmarks/src/main/java/.../benchmark/compute/operator/ValuesAggregatorBenchmark.java` |
| `MultivalueDedupeBenchmark` | Multivalue dedup | `benchmarks/src/main/java/.../benchmark/compute/operator/MultivalueDedupeBenchmark.java` |

### 3.2 Nightly Benchmark Script

**File**: `/Users/oleglvovitch/github/root/elasticsearch/benchmarks/run.sh`

This runs **all ESQL JMH benchmarks** and outputs JSON results:
```bash
run 'esql_agg' 'AggregatorBenchmark -pgrouping=none,longs ...'
run 'esql_block_keep_mask' 'BlockKeepMaskBenchmark ...'
run 'esql_block_read' 'BlockReadBenchmark ...'
run 'esql_eval' 'EvalBenchmark'
run 'esql_parse_ip' 'ParseIpBenchmark'
run 'esql_topn' 'TopNBenchmark'
run 'esql_values_agg' 'ValuesAggregatorBenchmark'
run 'esql_values_source_reader' 'ValuesSourceReaderBenchmark'
```

Results go to `build/benchmarks/*.json`.

### 3.3 Client Benchmark

**File**: `/Users/oleglvovitch/github/root/elasticsearch/client/benchmark/`

A standalone benchmark tool that runs HTTP REST requests against a live Elasticsearch cluster. It measures:
- Throughput (ops/sec)
- Service time percentiles (p50-p99.99)
- Latency percentiles (p50-p99.99)
- Success/error counts

This is closer to what we need for external sources, but it only supports `search` and `bulk` operations today. It would need an `esql` operation type.

### 3.4 What's Missing

None of the existing benchmarks measure:
- Parquet read throughput from S3/GCS/Azure
- NDJSON.gz decompression + parsing throughput
- Split discovery + distribution overhead
- End-to-end EXTERNAL query latency at scale
- Memory pressure under concurrent external source queries
- Impact of file count (1 file vs. 1000 files)
- Impact of file size (1MB vs. 1GB)
- Impact of compression (none vs. gzip vs. zstd)
- Impact of row-group size in Parquet
- Filter pushdown benefit (predicate hits vs. misses)

---

## 4. Generative Testing (Relevant But Different)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/generative/`

The generative test framework generates random ES|QL queries and runs them against a live cluster. It is a **fuzz testing** tool, not a performance testing tool. However, it validates that complex query compositions do not crash -- which is relevant for external sources where query rewriting may differ.

---

## 5. Other Load Testing Tools

### Gatling / k6

**No references in the repo.** Neither Gatling nor k6 are used anywhere in the Elasticsearch codebase.

---

## 6. What a Practical Test Harness Looks Like

### 6.1 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Test Driver (Rally or Custom)             │
│  - Generates queries: EXTERNAL "s3://bucket/path/*.parquet"  │
│  - Sends via /_query REST API                                │
│  - Measures latency, throughput, error rates                 │
│  - Records memory/GC metrics via /_nodes/stats               │
└───────────────────────────┬──────────────────────────────────┘
                            │ HTTP
                            ▼
┌──────────────────────────────────────────────────────────────┐
│              Elasticsearch Cluster (1-3 nodes)               │
│  - ESQL compute engine                                       │
│  - External source plugins (S3, GCS, Parquet, NDJSON, etc.) │
│  - Split discovery + distribution                            │
│  - Circuit breaker monitoring                                │
└───────────────────────────┬──────────────────────────────────┘
                            │ S3/GCS/Azure API
                            ▼
┌──────────────────────────────────────────────────────────────┐
│           Object Store (MinIO / LocalStack / Real S3)         │
│  - Pre-loaded test data                                       │
│  - Configurable latency (tc netem for MinIO)                 │
│  - Bandwidth throttling                                       │
└──────────────────────────────────────────────────────────────┘
```

### 6.2 Components Needed

#### A. Object Store

| Option | Pros | Cons | When to Use |
|--------|------|------|-------------|
| **MinIO container** | Already in repo (`MinioTestContainer`), real S3 API, fast | No network latency simulation by default | CI, local dev benchmarks |
| **MinIO + tc netem** | Real S3 API + simulated latency | Requires Docker network config | Latency-sensitive benchmarks |
| **LocalStack** | Full AWS API emulation | Heavier than MinIO | When testing AWS-specific behavior |
| **Azurite** | Real Azure Blob API | Azure-only | Azure-specific benchmarks |
| **Real S3/GCS** | Production-representative | Cost, flaky, slow | Pre-release performance validation |
| **S3HttpFixture** (existing) | Zero setup, fast | Zero latency, single-threaded | NOT for perf testing |

**Recommendation**: MinIO for CI benchmarks, real S3 for release validation. MinIO is already a first-class fixture in the repo.

#### B. Test Data Generation

Need a data generator that:
1. Produces files in all supported formats (Parquet, NDJSON, CSV, ORC)
2. Generates files at configurable sizes (1MB, 10MB, 100MB, 1GB)
3. Generates configurable file counts (1, 10, 100, 1000 files)
4. Produces compressed variants (gzip, zstd, bzip2)
5. Controls Parquet row-group sizes (for row-group filter pushdown testing)
6. Generates data with known distributions (for filter selectivity testing)
7. Uploads to object store (S3/MinIO)

The `OrcFixtureGenerator` pattern can be generalized -- it already reads canonical CSV and produces binary format files.

For realistic data volumes, use the NYC taxi dataset (same as the Rally ESQL track) or synthetic log data.

#### C. Query Execution & Measurement

Two approaches:

**Approach 1: Rally Track (Recommended for CI)**
```json
{
  "operation": "esql",
  "body": {
    "query": "EXTERNAL \"s3://perf-bucket/nyc-taxi/*.parquet\" | WHERE trip_distance > 10 | STATS avg_fare = AVG(total_amount) BY payment_type | SORT payment_type"
  }
}
```

Rally gives us: throughput, latency percentiles, error rates, GC metrics. Output is JSON, integrates with the existing benchmark dashboard at elasticsearch-benchmarks.elastic.co.

**Approach 2: Custom Java Harness (For detailed profiling)**

Extend the existing `client/benchmark` framework:
```java
public class EsqlExternalBenchmarkTask implements BenchmarkTask {
    // Send EXTERNAL queries via REST client
    // Measure per-query latency
    // Collect /_nodes/stats for memory/circuit breaker
    // Collect /_query with profile=true for internal timing
}
```

This gives us access to ES|QL's profile output, which breaks down time spent in each operator.

#### D. Metrics Collection

| Metric | Source | Tool |
|--------|--------|------|
| Query latency (p50/p90/p99) | Client-side timing | Rally or custom harness |
| Throughput (queries/sec) | Client-side counting | Rally or custom harness |
| Bytes transferred from S3 | MinIO access logs or S3RequestLog | Custom |
| S3 request count by type | S3RequestLog (already exists!) | Custom |
| Memory usage | `/_nodes/stats?metric=breaker` | REST API polling |
| GC pauses | `/_nodes/stats?metric=jvm` | REST API polling |
| Circuit breaker trips | `/_nodes/stats?metric=breaker` | REST API polling |
| Per-operator timing | `/_query` with `profile: true` | ES|QL profile output |
| Thread pool utilization | `/_nodes/stats?metric=thread_pool` | REST API polling |

### 6.3 Test Matrix

The following dimensions should be varied:

| Dimension | Values | Why It Matters |
|-----------|--------|----------------|
| Format | Parquet, NDJSON, NDJSON.gz, CSV | Different decode paths |
| File size | 1MB, 10MB, 100MB, 1GB | Memory pressure, streaming behavior |
| File count | 1, 10, 100, 1000 | Split discovery, distribution overhead |
| Compression | none, gzip, zstd | CPU vs. I/O tradeoff |
| Query type | scan-all, filter, aggregate, TopN | Different operator paths |
| Filter selectivity | 1%, 10%, 50%, 100% | Filter pushdown benefit |
| Distribution mode | coordinator_only, round_robin, adaptive | Distribution overhead |
| Cluster size | 1 node, 3 nodes | Distribution benefit |
| Column pruning | all columns, 3 columns | I/O reduction |
| Concurrency | 1, 4, 16 concurrent queries | Thread pool pressure |

### 6.4 Practical First Steps

**Phase 1: Data generator + MinIO setup** (1 day)
- Extend `OrcFixtureGenerator` pattern to produce Parquet/NDJSON at scale
- Write a Gradle task that starts MinIO, generates data, uploads to MinIO
- Output: `s3://perf-bucket/` with test data at various sizes

**Phase 2: Custom Java harness** (2-3 days)
- Add `EsqlBenchmarkTask` to `client/benchmark`
- Measure latency/throughput for parameterized EXTERNAL queries
- Collect breaker/GC/thread_pool stats between runs
- Output: console summary + JSON for tracking

**Phase 3: Rally track** (2-3 days)
- Create `esql-external/` rally track in elastic/rally-tracks
- Define challenges: scan, filter, aggregate, compressed, multi-file
- Integrate with MinIO (challenge parameter: `s3_endpoint`)
- Output: standard Rally metrics, publishable to benchmarks dashboard

**Phase 4: CI integration** (1-2 days)
- Add a nightly Gradle task that runs Phase 2 harness
- Store results as JMH-compatible JSON in `build/benchmarks/`
- Add regression detection (compare with baseline)

---

## 7. What the Repo Already Gives Us (Reusable Pieces)

| Component | Where | Reusable For |
|-----------|-------|-------------|
| MinIO Docker fixture | `test/fixtures/minio-fixture/` | Object store for benchmarks |
| S3HttpFixture + request logging | `x-pack/plugin/esql/qa/server/.../S3FixtureUtils.java` | Request counting in benchmarks |
| Template-based query transformation | `AbstractExternalSourceSpecTestCase` | Parameterizing benchmark queries |
| Cluster setup with external source plugins | `Clusters.java` in each QA module | Cluster configuration for benchmarks |
| JMH framework + nightly run script | `benchmarks/run.sh` | Running and reporting microbenchmarks |
| Client benchmark framework | `client/benchmark/` | Latency/throughput measurement |
| ProfileLogger | `x-pack/plugin/esql/qa/.../ProfileLogger.java` | Extracting per-operator profiling |
| OrcFixtureGenerator | `esql-datasource-orc/qa/.../OrcFixtureGenerator.java` | Pattern for data generation |
| External csv-spec tests | `external-basic.csv-spec`, `external-multifile.csv-spec` | Query patterns for benchmarks |
| Distribution mode testing | `ExternalDistributedSpecIT` | Distribution strategy benchmarks |
| GCS/Azure fixture infrastructure | `GoogleCloudStorageHttpFixture`, `AzureHttpFixture` | Multi-cloud benchmarks |

---

## 8. Recommendations

### For MVP Tech Preview

**Minimum viable benchmarking** (what you need before shipping):

1. **Data generator**: Script that produces 100MB and 1GB Parquet files (NYC taxi or synthetic logs) and uploads to MinIO
2. **Simple benchmark script**: Shell script that starts MinIO + ES cluster, runs 10 representative EXTERNAL queries, measures wall-clock time, prints p50/p90/p99
3. **Circuit breaker validation**: Run benchmark with ES memory constrained (e.g., `-Xmx2g`), verify no OOM, verify circuit breaker trips cleanly
4. **Concurrent query test**: Run 4-16 queries simultaneously, verify no deadlocks or excessive queueing

This is approximately the "load/perf testing harness" item from the roadmap.

### For MVP GA

**Production-grade benchmarking**:

1. **Rally track**: Full `esql-external/` track with multiple challenges
2. **Nightly runs**: Integrated into the nightly benchmark pipeline
3. **Regression detection**: Automated comparison with baseline numbers
4. **Multi-node tests**: 3-node cluster with all distribution modes
5. **Iceberg benchmarks**: Metadata resolution + Parquet read from catalog

### What NOT to Build

- **Custom load testing tool from scratch** -- Rally already exists and has ES|QL support
- **Distributed JMH benchmarks** -- JMH is for microbenchmarks, not end-to-end
- **Real cloud benchmarks in CI** -- too slow, too expensive, too flaky. Use MinIO. Run real cloud benchmarks manually before releases.

---

## 9. Key Insight: The S3RequestLog Infrastructure Is Gold

The `S3FixtureUtils.S3RequestLog` class already tracks every S3 operation with type, path, content length, and timestamp. This means the existing test infrastructure can answer questions like:
- How many GET_OBJECT requests does a single EXTERNAL query make?
- What is the total bytes transferred per query?
- How many LIST_OBJECTS_V2 calls does glob discovery make?
- Are there redundant HEAD_OBJECT calls?

For performance testing, the number of S3 requests is often more important than wall-clock time, because S3 API calls have a fixed cost ($0.0004/GET) and latency floor (~50ms for first byte). Reducing request count directly reduces cost and improves latency.

The existing infrastructure already counts this. What's missing is a benchmark harness that exercises it at scale and reports the numbers systematically.
