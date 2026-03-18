# Distributed Execution Hardening — Future Work

Follow-up work items from [elastic/esql-planning#291](https://github.com/elastic/esql-planning/issues/291).
The initial PR covers unit-level hardening (HTTP 500 retryability, drain error tests, fault injection infrastructure, buffer tests).
The categories below cover the remaining integration-level and production-level hardening.

---

## Category A: Multi-Node Integration Resilience Tests

**Goal:** Verify that transient S3 failures during actual distributed ESQL queries are recovered by the retry policy end-to-end.

**New file:** `qa/server/multi-node/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/multi_node/ExternalDistributedResilienceIT.java`

### What to implement

- Wire `FaultInjectingS3HttpHandler` into the S3 fixture used by the multi-node cluster. This requires changes to `ExternalDistributedClusters` and `AbstractExternalSourceSpecTestCase` to expose the handler so tests can toggle faults during queries.
- Test transient S3 failures (503, 500, connection reset) during actual ESQL queries with distributed splits across all three distribution modes (`coordinator_only`, `round_robin`, `adaptive`).
- Verify the retry policy recovers from transient faults end-to-end (query succeeds despite N injected faults).
- Verify persistent faults produce clear error messages to the user.

### Key infrastructure gap

The S3 fixture creates the `HttpHandler` internally in `DataSourcesS3HttpFixture`. To inject faults during a query, the fixture needs to expose a `FaultInjectingS3HttpHandler` wrapper that the test can toggle. This is the main engineering effort.

### Key files to modify

| File | Change |
|------|--------|
| `qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/AbstractExternalSourceSpecTestCase.java` | Expose the S3 handler for fault injection |
| `qa/server/multi-node/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/multi_node/ExternalDistributedClusters.java` | Wire fault-injecting handler into cluster setup |
| New: `ExternalDistributedResilienceIT.java` | Integration tests with fault injection |

### Test scenarios

1. **Transient 503 during distributed query** — inject N 503 faults via `FaultInjectingS3HttpHandler`, run a query that scans multiple files across nodes, verify query succeeds after retries.
2. **Transient 500 during distributed query** — same as above with HTTP 500 (now retryable).
3. **Connection reset during distributed query** — inject `CONNECTION_RESET` faults, verify retry recovery.
4. **Persistent faults fail the query** — inject more faults than the retry budget, verify the query fails with a clear error message.
5. **Faults on specific paths only** — use path filter to inject faults only for `.parquet` files, verify metadata requests succeed while data reads are faulted.

### Reference

- ClickHouse PR #66791: distributed query retries when servers stop during execution
- ClickHouse PR #84421: S3 mock throttler for resilience testing
- `FaultInjectingS3HttpHandler` already supports HTTP_500, HTTP_503, CONNECTION_RESET, SLOW_RESPONSE, TRUNCATED_RESPONSE

---

## Category B: Stress Testing with Many Splits

**Goal:** Verify the distributed execution framework handles 1000+ splits without split starvation, exchange deadlocks, or memory issues.

**New file:** `qa/server/multi-node/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/multi_node/ExternalDistributedStressIT.java` (or methods in `ExternalDistributedResilienceIT`)

### What to implement

- Generate synthetic datasets with 1000+ small Parquet files (each a few KB).
- Upload them to the S3 fixture before the test.
- Run ESQL queries that scan all files across the cluster with each distribution mode.
- Verify: no split starvation, no exchange deadlocks, all rows returned, reasonable memory usage.
- Also test with heterogeneous split sizes: mix 100-byte and 10 MB files.

### Key infrastructure gap

The current csv-spec test data has a handful of small files. Generating and hosting 1000+ files in the S3 fixture requires a synthetic data generator — a helper method that creates Parquet files programmatically using the existing Arrow writer infrastructure.

### Key files to modify

| File | Change |
|------|--------|
| New helper class or method | Synthetic Parquet file generator (N files, configurable row count and column count) |
| S3 fixture setup | Upload generated files to the S3 mock |
| New: `ExternalDistributedStressIT.java` | Stress tests |

### Test scenarios

1. **1000 uniform splits** — 1000 files × 100 rows each, query `SELECT * FROM EXTERNAL ...`, verify 100,000 rows returned.
2. **1000 uniform splits with aggregation** — `STATS count(*) BY gender`, verify correct aggregation across all splits.
3. **Heterogeneous splits** — mix of 100-row and 10,000-row files, verify no straggler effects (all distribution modes should complete within a reasonable time).
4. **Many splits with pipeline breaker** — `SORT salary DESC LIMIT 10` across 1000 files, verify TopN works correctly with distributed execution.
5. **Memory pressure** — run with a small buffer size pragma, verify backpressure prevents OOM.

### Reference

- ClickHouse issue #66834: flaky distributed S3Queue test due to uneven load distribution across processing nodes
- ES|QL `WeightedRoundRobinStrategy` uses LPT scheduling to avoid this, but has no stress test proving it works at scale
- `ExternalSliceQueue` uses `AtomicInteger` for lock-free split claiming — stress test validates correctness under contention

---

## Category C: Node Failure Mid-Query

**Goal:** Verify that the distributed execution framework handles data node failures gracefully during external source queries.

**Scope:** Extension to `EsqlNodeFailureIT` or `EsqlDisruptionIT` to cover external source queries.

### What to implement

- Run a distributed external source query.
- Kill a data node mid-execution (after it has started processing splits but before it finishes).
- Verify: with `allow_partial_results=true`, the coordinator collects results from surviving nodes; with `allow_partial_results=false`, the query fails with a clear error.
- Also test: coordinator failure, network partition between coordinator and data node.

### Key infrastructure gap

`EsqlNodeFailureIT` and `EsqlDisruptionIT` exist but only test ES index queries. Extending them to external sources requires the S3 fixture to be available in those test clusters, plus synthetic external data. The disruption test infrastructure (`NetworkDisruption`, `ServiceDisruptionScheme`) needs to be combined with the external source S3 fixture.

### Key files to modify

| File | Change |
|------|--------|
| `src/internalClusterTest/java/org/elasticsearch/xpack/esql/action/EsqlNodeFailureIT.java` | Add external source query variants |
| `src/internalClusterTest/java/org/elasticsearch/xpack/esql/action/EsqlDisruptionIT.java` | Add external source query variants |
| Cluster setup | Add S3 fixture and synthetic data to disruption test clusters |

### Test scenarios

1. **Data node killed mid-split processing** — start a distributed query, kill a data node after it starts processing, verify partial results or clear failure.
2. **Coordinator failure** — kill the coordinator node, verify the query fails cleanly (no orphaned tasks on data nodes).
3. **Network partition** — partition between coordinator and one data node, verify timeout and partial result behavior.
4. **Node restart during query** — restart a data node mid-query, verify the query either completes (if retries succeed) or fails cleanly.

### Reference

- ClickHouse PR #66791: dynamic replica reconnection when servers stop during distributed query execution
- `DataNodeComputeHandler.startExternalComputeOnDataNodes()` handles missing nodes with `allowPartial` flag — but this path has no test coverage for external sources
- `ExchangeSourceHandler.RemoteSinkFetcher.onSinkFailed()` handles exchange failures — needs testing with external source queries

---

## Category D: Adaptive Timeouts / Query-Timeout-Aware Retries

**Goal:** Prevent retries from exceeding the query's execution time budget, and use adaptive timeouts (short on first attempt, lenient on retry).

### What to implement

- **RetryPolicy enhancement:** Add a constructor parameter for maximum total retry duration so retries don't accumulate delays beyond the query's `max_execution_time`.
- **ExternalSourceDrainUtils enhancement:** The 5-minute `DRAIN_TIMEOUT` is hardcoded and independent of the query timeout. It should be derived from the remaining query budget.
- **Adaptive timeouts:** First attempt should use a shorter timeout, becoming more lenient on retries.

### Key files to modify

| File | Change |
|------|--------|
| `src/main/java/org/elasticsearch/xpack/esql/datasources/RetryPolicy.java` | Add `maxTotalDurationMs` parameter; check elapsed time before each retry |
| `src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceDrainUtils.java` | Accept remaining query budget instead of hardcoded `DRAIN_TIMEOUT` |
| `src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java` | Pass query timeout context to drain utils |
| Tests for all of the above | |

### Design notes

- `RetryPolicy.execute()` currently loops `maxRetries` times with exponential backoff. Add an `Instant deadline` or `long maxTotalMs` that short-circuits the loop if the total elapsed time would exceed the budget.
- `ExternalSourceDrainUtils.DRAIN_TIMEOUT` (5 minutes) should be replaced with a parameter passed from the operator factory, derived from the query's remaining execution budget.
- Adaptive timeouts: the first `Thread.sleep(delay)` in the retry loop uses a shorter delay; subsequent retries use longer delays. This is already partially implemented via exponential backoff, but the initial timeout for the HTTP request itself (not the retry delay) should also be adaptive.

### Reference

- ClickHouse PR #83957: S3 retry strategy considers query execution time — retries were accumulating 90-second delays that exceeded `max_execution_time`
- ClickHouse PR #56314: adaptive timeouts — low send/receive timeouts on first attempt, more lenient on retries
- ClickHouse PR #81849: jitter to prevent thundering herd (already implemented in ES|QL `RetryPolicy.delayMillis()`)
- DuckDB `httpfs_timeout_retry` extension: per-operation timeout and retry counts for different operation types

---

## Category E: DNS Resolution Failure Should Not Be Retried

**Goal:** Prevent wasting the entire retry budget on misconfigured bucket URLs where DNS resolution fails.

### What to implement

- In `RetryPolicy.isTransientSingleCause()`, `ConnectException` is currently always treated as retryable. However, DNS resolution failures (`UnknownHostException`) are wrapped as `ConnectException` in some JDK versions.
- Add a check: if `ConnectException` is caused by `java.net.UnknownHostException`, do not retry.
- This prevents wasting the entire retry budget (3 retries × exponential backoff) on typos in S3 bucket names.

### Key files to modify

| File | Change |
|------|--------|
| `src/main/java/org/elasticsearch/xpack/esql/datasources/RetryPolicy.java` | In `isTransientSingleCause()`, check if `ConnectException` is caused by `UnknownHostException` |
| `src/test/java/org/elasticsearch/xpack/esql/datasources/RetryPolicyTests.java` | Add `testUnknownHostIsNotRetryable`, `testConnectExceptionWithoutDnsFailureIsStillRetryable` |

### Design notes

```java
if (t instanceof ConnectException) {
    // DNS resolution failures should not be retried
    for (Throwable cause = t.getCause(); cause != null; cause = cause.getCause()) {
        if (cause instanceof java.net.UnknownHostException) {
            return false;
        }
    }
    return true;
}
```

### Reference

- ClickHouse PR #77678: "Do not retry S3 requests in case of host not found" — was causing 100 unnecessary retry attempts on typos in S3 bucket names
- In production, `UnknownHostException` is deterministic (DNS won't suddenly resolve a typo), so retrying is pure waste
