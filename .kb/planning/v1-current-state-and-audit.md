# ES|QL External Data Sources — Current State & Performance Audit

## 1. Current State (March 2026)

The external data sources framework on main is substantially implemented. Distributed execution, the plugin architecture, and the dual SPI (file-based + query-based) are all working. 13 plugins ship across four families: storage (S3, GCS, Azure, HTTP), formats (Parquet, ORC, NDJSON, CSV), compression (GZIP, ZSTD, BZIP2), and connectors (Arrow Flight). Iceberg has metadata discovery but no data reading.

**What works end-to-end today:** A user can write `EXTERNAL "s3://bucket/logs/*.ndjson"` and get distributed, parallel query execution across data nodes with Hive partition detection, column projection, and streaming decompression. This works for S3, GCS, and HTTP sources with Parquet, ORC, NDJSON, and CSV formats.

**What doesn't work:** The syntax is dev-gated (`EXTERNAL`), not available in production builds. No persistent datasource definitions or credential management. Filter pushdown exists as infrastructure but isn't wired for most formats. Iceberg is metadata-only.

**Recently merged (since initial audit):** LIMIT pushdown (#276, PR #143515) — `PushLimitToExternalSource` optimizer rule, FormatReader respects rowLimit. Stats-via-metadata (PR #143940) — `PushStatsToExternalSource` answers COUNT(*)/MIN/MAX from file metadata without scanning. Byte-based backpressure (PR #144218) — `AsyncExternalSourceBuffer` now tracks bytes, not just page count. CSV type support gaps fixed (#334). Parquet type support gaps fixed (#337). Text format parallel parsing (#341). Parquet row-group parallelism (#342). WeightedRoundRobin distribution strategy for size-aware split assignment.

For full details, see [EXTERNAL_SOURCES_OVERVIEW.md](x-pack/plugin/esql/EXTERNAL_SOURCES_OVERVIEW.md).

---

## 2. Performance & Scalability Audit

### 2.1 Buffer Management

`AsyncExternalSourceBuffer` uses a `ConcurrentLinkedQueue<Page>` with an `AtomicInteger` size counter.

- **Default buffer: 10 pages** (hardcoded in `SourceOperatorContext.Builder`). At 1000 rows/page, this buffers ~10K rows. For high-throughput sources (Parquet columnar reads, Flight bulk transfer), this creates a bottleneck where producers frequently stall waiting for buffer space.
- **Backpressure:** Non-blocking dual-lock pattern (separate notEmpty/notFull locks). Well-designed — no spinlock, no busy-wait. But the buffer is per-driver, and with small buffer sizes the producer thread spends significant time blocked.
- **~~No memory-based limit~~ FIXED (PR #144218):** Buffer now uses byte-based backpressure via `AsyncExternalSourceBuffer`. Pages are tracked by `ramBytesUsedByBlocks()` in addition to count. This addresses the wide-schema memory explosion risk.

**Risk (remaining):** Buffer size defaults still need tuning (GA-4). Drain timeout (5 minutes) still hardcoded.

### 2.2 Drain Timeout

`ExternalSourceDrainUtils` has a hardcoded **5-minute drain timeout** (`TimeValue.timeValueMinutes(5)`). If a consumer stops reading (query cancelled, downstream error), the producer blocks for up to 5 minutes before the timeout fires.

- **No cancellation hook** — buffer's `finish(true)` must be called externally; the drain loop doesn't check for task cancellation.
- **Masking slow sources** — a source that takes 4 minutes per page looks like normal operation, not a timeout.

**Risk:** Cancelled queries waste resources for up to 5 minutes. Slow sources aren't differentiated from deadlocks.

### 2.3 Distribution Strategy

`AdaptiveStrategy` makes distribution decisions based on simple heuristics:

- Single split → coordinator-only (correct)
- LIMIT-only plan → coordinator-only (correct)
- Aggregation present → distribute (correct)
- splits > nodes → distribute via **WeightedRoundRobin** when sizes available, plain round-robin otherwise

**What's been addressed since initial audit:**
- **Split size awareness now implemented.** `WeightedRoundRobin` uses Longest Processing Time (LPT) algorithm — sorts splits largest-first, greedily assigns to least-loaded node. File splits report byte-range length; coalesced splits sum children's sizes. Split coalescer bin-packs into 128MB groups.

**What's still missing:**
- **No load-aware assignment.** Distribution doesn't consider node CPU, memory, or queue depth. A node processing a heavy ES query gets the same split count as an idle node.
- **No runtime adaptation.** "Adaptive" decides *whether* to distribute, not runtime load balancing. No work-stealing or speculative execution.
- **Static coalescing targets.** 128MB group size and 32-split threshold are compile-time constants.

**Risk:** At scale with mixed workloads, node-level contention remains unaddressed.

### 2.4 Parquet Performance

The Parquet reader has been **substantially improved** since initial audit:

- **Columnar reader (#278, merged):** Replaced the row-by-row `GroupRecordConverter` with parquet-mr's `ColumnReader`/`ColumnReadStoreImpl` API. Reads column-at-a-time into block builders instead of row-by-row Group deserialization. 3-5x improvement over the original reader.
- **Row-group parallelism (#342, merged):** `RangeAwareFormatReader` SPI enables splitting Parquet files at row-group boundaries. Large files are no longer monolithic splits.
- **Stats-via-metadata (PR #143940, merged):** `PushStatsToExternalSource` optimizer rule answers COUNT(*)/MIN/MAX from Parquet footer metadata without scanning data.
- Column projection works (reduces I/O).
- Default batch size: 1000 rows.

**What's still missing:**
- No predicate pushdown into row group statistics (Substrait filter pushdown).
- Not yet at Arrow-native Parquet reader performance (10-50x gap vs DuckDB/DataFusion). The columnar reader narrows this to ~3-10x.
- Parquet-MR reader improvements (#285) and circuit breaker integration (#282) are still open.

**Risk:** Parquet performance improved significantly but still not competitive with native Arrow readers. Arrow Dataset reader remains the post-MVP path for closing the gap.

### 2.5 Flight Connector Conversion

Arrow vectors are converted to ES|QL blocks via **scalar row-by-row iteration**:

```java
for (int i = 0; i < rowCount; i++) {
    if (intVec.isNull(i)) {
        builder.appendNull();
    } else {
        builder.appendInt(intVec.get(i));
    }
}
```

Arrow vectors are already columnar arrays — in principle, they could be wrapped as ES|QL blocks with zero copy. The current approach copies every value individually.

**Risk:** For large Flight result sets, conversion overhead is significant. Improvement is straightforward (bulk copy or zero-copy wrappers) but not yet implemented.

### 2.6 Thread Pool Sharing

External source I/O runs on the `GENERIC` thread pool (shared with HTTP client, S3 SDK, all plugin I/O). Distributed exchange uses the `SEARCH` pool (shared with all search operations).

- **No isolation:** A burst of external source queries competes with regular ES operations for thread pool capacity.
- **No dedicated pool:** Unlike the `esql_worker` pool for compute, external I/O has no dedicated pool.
- **No queue depth tracking:** No metrics on how many external source operations are queued.

**Risk:** Under load, external source queries can starve regular ES operations or vice versa. No observability into contention.

### 2.7 Memory Safety: Circuit Breaker Gaps

All format readers correctly use `BlockFactory` (circuit-breaker-integrated) for final ESQL blocks. However, **all third-party library allocations bypass circuit breakers entirely**:

| Component | Untracked Allocation | Risk | Worst Case |
|-----------|---------------------|------|------------|
| **Flight `RootAllocator`** | Off-heap via `Unsafe.allocateMemory()`, default limit `Long.MAX_VALUE` | **CRITICAL** | Unbounded — OOM-kill |
| **Parquet `parquet-mr`** | Row group column pages + decompression buffers as `byte[]` | **HIGH** | 1-4 GB per row group |
| **ORC `VectorizedRowBatch`** | Column vectors + stripe buffers | **HIGH** | 64-256 MB per stripe |
| **HTTP non-Range fallback** | Full file loaded into `byte[]` when server returns 200 instead of 206 | **HIGH** | N × fileSize |
| **Flight Arrow→Block** | Arrow batch + ESQL block copy coexist during conversion | **HIGH** | 2× batch size |
| **S3 async response** | `AsyncResponseTransformer.toBytes()` accumulates full response | **MEDIUM** | Hundreds of MB |
| **Split/file lists** | `GlobExpander`, `FileSplitProvider`, `SplitDiscoveryPhase` all accumulate in unbounded lists | **MEDIUM** | ~200 bytes × file count |
| **Iceberg metadata** | `planScan()` loads all `FileScanTask` objects; metadata JSON parsed in memory | **MEDIUM** | 10M files = ~2 GB |
| **~~Buffer backpressure~~** | ~~`AsyncExternalSourceBuffer` limits by page count (10), not byte size~~ **FIXED** (PR #144218) — now byte-based | ~~MEDIUM~~ | ~~Wide schemas → GB in 10 pages~~ |

**Bottom line:** The ESQL block pipeline is properly circuit-breaker-controlled. Everything upstream of block building — library I/O buffers, Arrow allocators, intermediate data structures — is not. For Tech Preview this is a known limitation. For GA, the critical items (Flight allocator, Parquet/ORC pre-read buffers, HTTP fallback) must be addressed.

### 2.8 No Load/Performance Testing

There are **zero benchmarks** for external data sources. No JMH benchmarks, no rally tracks, no load test harnesses.

- Happy-path integration tests exist per plugin (S3/GCS with testcontainers).
- Distributed execution tests now exist (~180 test methods across 20+ classes: unit tests for split discovery, distribution strategy, fault injection; multi-node integration tests with csv-spec query patterns × distribution modes × storage backends).
- No chaos/failure tests (no `internalClusterTest` for full distributed path).
- No schema drift tests.

**Risk:** Performance regressions can't be detected. Scalability limits are unknown. Testing coverage for distributed execution has improved significantly but no benchmarks exist.
