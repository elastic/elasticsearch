# Deep Dive: Split Size-Aware Distribution for ES|QL External Data Sources

**Date**: 2026-03-03
**Branch**: `esql/connector-spi-v3` (based on main at `970b4789c35`)
**Scope**: Distribution strategy layer in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/`

---

## 1. Distribution Strategy Architecture

### 1.1 Interface Hierarchy

The distribution framework is built around a single interface and a clean context/plan model:

```
ExternalDistributionStrategy (interface)
  planDistribution(ExternalDistributionContext) -> ExternalDistributionPlan

ExternalDistributionContext (record)
  - plan: PhysicalPlan
  - splits: List<ExternalSplit>
  - availableNodes: DiscoveryNodes
  - pragmas: QueryPragmas

ExternalDistributionPlan (record)
  - nodeAssignments: Map<String, List<ExternalSplit>>
  - distributed: boolean
  - LOCAL = new ExternalDistributionPlan(Map.of(), false)
```

### 1.2 All Strategy Implementations

There are exactly **four** strategy implementations:

| Strategy | File | Selection | Size-Aware? |
|----------|------|-----------|-------------|
| **AdaptiveStrategy** | `AdaptiveStrategy.java` | Default (`"adaptive"` pragma) | Yes - delegates to Weighted when size available |
| **RoundRobinStrategy** | `RoundRobinStrategy.java` | `"round_robin"` pragma | No - ignores sizes entirely |
| **WeightedRoundRobinStrategy** | `WeightedRoundRobinStrategy.java` | `"weighted_round_robin"` pragma | Yes - LPT algorithm using `estimatedSizeInBytes()` |
| **CoordinatorOnlyStrategy** | `CoordinatorOnlyStrategy.java` | `"coordinator_only"` pragma | N/A - never distributes |

Strategy resolution from `ComputeService.resolveExternalDistributionStrategy()`:
```java
return switch (pragmas.externalDistribution()) {
    case "", "adaptive" -> new AdaptiveStrategy();
    case "coordinator_only" -> CoordinatorOnlyStrategy.INSTANCE;
    case "round_robin" -> new RoundRobinStrategy();
    case "weighted_round_robin" -> new WeightedRoundRobinStrategy();
    default -> { yield new AdaptiveStrategy(); }
};
```

All strategies (except `CoordinatorOnly`) use `NodeEligibilityStrategy` to filter eligible nodes. Default is `DATA_NODES_ONLY` (nodes where `canContainData()` is true).

---

## 2. Split Size Awareness: Detailed Analysis

### 2.1 ExternalSplit Size Contract

**File**: `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSplit.java`

```java
public interface ExternalSplit extends NamedWriteable {
    String sourceType();
    default long estimatedSizeInBytes() {
        return -1;  // -1 means "unknown"
    }
}
```

The contract is: return a positive value when the split's byte size is known, return `-1` when unknown.

### 2.2 Split Implementations and Their Size Reporting

| Split Type | File | Reports Size? | How? |
|------------|------|---------------|------|
| **FileSplit** | `FileSplit.java` | **Yes** | Returns `length` field (byte range length from file listing) |
| **CoalescedSplit** | `CoalescedSplit.java` | **Yes** (if all children do) | Sum of children's `estimatedSizeInBytes()`; returns -1 if any child is -1 |
| **FlightSplit** | `FlightSplit.java` (esql-datasource-grpc) | **No** | Always returns `-1`. Carries `estimatedRows` but not byte size. |

Key observations:
- **File-based sources (S3, GCS, Azure, HTTP)**: All produce `FileSplit` with accurate byte sizes from storage listing.
- **Connector-based sources (Flight)**: `FlightSplit` returns `-1` for `estimatedSizeInBytes()`, falling back to round-robin. Flight *does* have `estimatedRows` (from `FlightInfo.getRecords() / endpoints.size()`), but this is **not used** by the distribution strategies.

### 2.3 How Size Information Flows Through the Pipeline

1. **Split Discovery**: `SplitDiscoveryPhase.resolveExternalSplits()` walks the physical plan, finds `ExternalSourceExec` nodes, and calls each factory's `SplitProvider.discoverSplits()`.
   - `FileSplitProvider` creates `FileSplit` with `offset=0, length=fileLength` per file (or byte-range chunks for splittable formats).
   - `FlightSplitProvider` creates `FlightSplit` with `estimatedRows` but `estimatedSizeInBytes()=-1`.

2. **Split Coalescing**: `ComputeService.coalesceSplits()` calls `SplitCoalescer.coalesce()` if split count > 32 (the `COALESCING_THRESHOLD`).
   - When all splits have size info: **greedy bin-packing** into 128 MB groups (best-fit decreasing).
   - When any split lacks size info: **count-based grouping** into 8 groups.
   - `CoalescedSplit` accurately sums children's sizes, so size info is preserved through coalescing.

3. **Distribution Decision**: `ComputeService.applyExternalDistributionStrategy()` collects all splits from the plan and passes them to the chosen strategy.

4. **AdaptiveStrategy Decision Tree** (the default path):
   ```
   splits.size() <= 1?                    -> LOCAL
   isLimitOnly(plan)?                     -> LOCAL (LIMIT without AGG/TOPN)
   no eligible nodes?                     -> LOCAL
   hasAggregation OR manySplits?
     all splits have size > 0?            -> WeightedRoundRobin (LPT)
     otherwise                            -> plain RoundRobin
   else                                   -> LOCAL
   ```
   where `manySplits = splits.size() > nodes.size()`

5. **Dispatch**: `DataNodeComputeHandler.startExternalComputeOnDataNodes()` sends each node's assigned splits as a `DataNodeRequest`, with graceful degradation (partial results) when nodes fail.

### 2.4 WeightedRoundRobinStrategy: The LPT Algorithm

**File**: `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/WeightedRoundRobinStrategy.java`

The algorithm is **Longest Processing Time (LPT)** scheduling, a classic multiprocessor scheduling heuristic:

```java
static ExternalDistributionPlan assignByWeight(List<ExternalSplit> splits, List<DiscoveryNode> nodes) {
    // 1. Sort splits by size, largest first
    sorted.sort(Comparator.comparingLong(ExternalSplit::estimatedSizeInBytes).reversed());

    // 2. Greedily assign each split to the least-loaded node
    for (ExternalSplit split : sorted) {
        int minIdx = /* index of node with lowest accumulated load */;
        assignments.get(nodes.get(minIdx).getId()).add(split);
        nodeLoads[minIdx] += split.estimatedSizeInBytes();
    }
}
```

**LPT Guarantee**: Maximum node load <= idealLoad + largestSplit, where idealLoad = totalSize / nodeCount. This is verified by the `ExtendedDistributionPropertyTests.testWeightedLoadBalancing()` test.

**Fallback**: When any split has `estimatedSizeInBytes() <= 0`, both `WeightedRoundRobinStrategy` and `AdaptiveStrategy` fall back to `RoundRobinStrategy.assignRoundRobin()` (simple modulo assignment).

---

## 3. Recent Changes: Timeline

All of this is **very new** -- the entire distribution framework was built in a burst from Feb 25 to Mar 3, 2026:

| Date | PR | What |
|------|-----|------|
| Feb 25 | #143005 | Split SPI (`ExternalSplit`, `SplitProvider`, `FileSplit`), partition detection, filter hints |
| Feb 27 | #143114 | Split discovery phase, wiring into physical plan |
| Feb 27 | #143194 | **Distribution strategies**: `AdaptiveStrategy`, `RoundRobinStrategy`, `CoordinatorOnlyStrategy`, `ExternalDistributionStrategy` interface, `external_distribution` pragma |
| Feb 28 | #143154 | Local parallelism and `ExternalSliceQueue` |
| Mar 1 | #143209 | Data node execution for external sources |
| Mar 2 | #143331 | Bridge Connector SPI to `ExternalSplit` |
| Mar 2 | #143335 | **Split coalescing**: `SplitCoalescer`, `CoalescedSplit`, greedy bin-packing |
| Mar 2 | #143349 | **Weighted distribution**: `WeightedRoundRobinStrategy` (LPT), sub-file splitting for CSV/NDJSON, `RangeStorageObject`, graceful degradation, Arrow Flight parallel execution |
| Mar 3 | #143420 | **Extended tests**: property tests for weighted RR + coalescing integration, fault injection for S3 |

The size-aware distribution (`WeightedRoundRobinStrategy`) and the coalescing (`SplitCoalescer`) were both added in the same sprint (Mar 2). The `AdaptiveStrategy` was updated in the same PR (#143349) to delegate to weighted when size info is available.

---

## 4. What's Working

### Fully Implemented and Tested:

1. **Size-aware distribution for file-based sources**: `FileSplit.estimatedSizeInBytes()` returns file length, which feeds into `WeightedRoundRobinStrategy` via `AdaptiveStrategy`. This works for S3, GCS, Azure, HTTP -- all four storage backends.

2. **LPT algorithm**: Classic Longest Processing Time scheduling with proven O(n log n) implementation. Property tests verify the load balancing bound (max_load <= ideal_load + largest_split) and completeness.

3. **Coalescing preserves size info**: `CoalescedSplit` sums children sizes, so the distribution strategy sees accurate aggregate sizes even after grouping thousands of small files.

4. **Coalescing + distribution integration**: Property tests (`ExtendedDistributionPropertyTests.testCoalescedSplitsDistributeCorrectly`) verify that the full pipeline (many files -> coalesce -> weighted distribute -> expand on data nodes) is correct.

5. **Sub-file splitting for row formats**: `FileSplitProvider` splits CSV/NDJSON/TSV/JSONL/TXT files into byte-range chunks when file length exceeds `targetSplitSizeBytes`, creating more splits with accurate size for finer-grained distribution.

6. **Graceful fallback**: When size info is unavailable (any split returns -1), both strategies automatically fall back to plain round-robin. No crashes, no incorrect assumptions.

7. **Strategy selection via pragma**: Users can force `weighted_round_robin`, `round_robin`, `coordinator_only`, or use `adaptive` (default) through the `external_distribution` query pragma.

8. **Node failure resilience**: `DataNodeComputeHandler.startExternalComputeOnDataNodes()` handles node disappearance and connection failures with partial-results fallback.

---

## 5. What's Missing / Gaps

### 5.1 Flight/Connector Splits Have No Size Info

**Gap**: `FlightSplit.estimatedSizeInBytes()` returns `-1`, so Flight queries always fall back to plain round-robin even when the Flight server reports record counts.

**Root cause**: `FlightSplit` carries `estimatedRows` from `FlightInfo.getRecords()`, but:
- The distribution strategies only look at `estimatedSizeInBytes()`, not row counts.
- Converting rows to bytes requires knowing the average row width, which isn't available at split-discovery time.

**Impact**: Medium. Flight queries with multiple endpoints get distributed but not load-balanced. For most Flight servers, endpoints represent roughly equal partitions, so round-robin is adequate. But for heterogeneous partitions (some endpoints serve more data than others), this is suboptimal.

**Possible fix**: Either (a) add `estimatedRows()` to the distribution strategy decision (rows as proxy for cost), (b) have `FlightSplit` estimate byte size from `FlightInfo.getBytes()` if the server reports it, or (c) add an `estimatedCost()` abstraction that generalizes over both bytes and rows.

### 5.2 No Runtime Adaptation / Work Stealing

**Gap**: The "adaptive" in `AdaptiveStrategy` refers to **whether** to distribute (based on plan shape and split count), not to runtime load adaptation. All distribution decisions are made **once**, before execution starts. There is no:
- Work stealing between nodes
- Runtime rebalancing based on observed split completion times
- Dynamic reassignment of remaining splits from slow nodes to fast ones

**Impact**: High for heterogeneous workloads. If one node is slow (e.g., cloud hot-spotting, network congestion to a specific storage region), all its splits wait. The system relies on the upfront LPT assignment being "good enough."

**Comparison**: Systems like Spark and Trino have speculative execution for straggler mitigation. The current ES|QL model is closer to MapReduce v1 (static assignment).

### 5.3 No Node Capacity/Load Awareness

**Gap**: Distribution strategies assume all nodes are equally capable. There's no consideration of:
- Current node load (CPU, memory, existing queries)
- Node hardware heterogeneity (some nodes may have more CPU/RAM)
- Network proximity to storage (e.g., same-region S3 access vs cross-region)
- Thread pool saturation

The `NodeEligibilityStrategy` only filters by role (data nodes vs all nodes), not by capacity.

**Impact**: Medium. In homogeneous clusters (typical for Elastic Cloud), this is a non-issue. In mixed-hardware clusters, a 2-CPU node gets the same share as a 32-CPU node.

### 5.4 No Parquet Row-Group Level Splitting

**Gap**: `FileSplitProvider` can split row-based formats (CSV, NDJSON) into byte ranges, but **not** columnar formats (Parquet, ORC). For Parquet, each file is one split regardless of size.

**Why this matters**: A single 10 GB Parquet file creates one split, while ten 1 GB files create ten splits. The LPT algorithm correctly places the 10 GB file on its own node, but it can't subdivide it further.

**What's needed**: Parquet row-group metadata reading to discover row groups and create per-row-group splits. This is listed as a separate MVP GA item in the roadmap.

### 5.5 Coalescing Target Size is Static

**Gap**: `SplitCoalescer.DEFAULT_TARGET_GROUP_SIZE_BYTES = 128 MB` is a compile-time constant. It doesn't adapt to:
- Available memory on data nodes
- Number of available nodes (could create more groups for more nodes)
- Query characteristics (aggregation-heavy queries might benefit from different group sizes)

**Impact**: Low-to-medium. 128 MB is a reasonable default for most workloads, but extreme cases (nodes with 2 GB heap processing 10,000 tiny files) might benefit from tunability.

### 5.6 No Cost-Based Strategy for Mixed Source Queries

**Gap**: If a query reads from both external sources and ES indices, the distribution only considers external splits. There's no unified cost model that balances external I/O against shard reads.

**Impact**: Low for MVP. Cross-source queries are rare and `FROM index | JOIN EXTERNAL "s3://..."` is not yet supported.

---

## 6. Summary: Status Assessment

| Aspect | Status | Notes |
|--------|--------|-------|
| File-based size-aware distribution | **Done** | LPT algorithm, all 4 storage backends |
| Split coalescing (many small files) | **Done** | Greedy bin-packing by size |
| Sub-file splitting (CSV/NDJSON) | **Done** | Byte-range splits for row formats |
| Strategy selection pragma | **Done** | adaptive/round_robin/weighted_round_robin/coordinator_only |
| Graceful degradation | **Done** | Partial results on node failure |
| Property/invariant tests | **Done** | Load balancing bounds, completeness, determinism |
| Fault injection tests | **Done** | S3 fault injection, retry policy |
| Connector (Flight) size awareness | **Gap** | Returns -1, falls back to round-robin |
| Parquet row-group splitting | **Gap** | Files are monolithic splits |
| Runtime adaptation / work stealing | **Gap** | Static upfront assignment only |
| Node capacity awareness | **Gap** | All nodes treated equally |
| Configurable coalescing targets | **Gap** | Static 128 MB / 8-group defaults |

**Overall Assessment**: Split size-aware distribution is **substantially implemented** for file-based sources. The core algorithm (LPT), the coalescing layer, the sub-file splitting, and the adaptive strategy selection are all in place, tested, and wired end-to-end. The main remaining gaps are (1) connector sources lacking byte-size estimates, (2) Parquet row-group granularity, and (3) runtime dynamic rebalancing -- all of which are post-MVP or GA items in the roadmap.

---

## 7. Key Source Files

| File | Path |
|------|------|
| ExternalDistributionStrategy | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionStrategy.java` |
| AdaptiveStrategy | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/AdaptiveStrategy.java` |
| WeightedRoundRobinStrategy | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/WeightedRoundRobinStrategy.java` |
| RoundRobinStrategy | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/RoundRobinStrategy.java` |
| CoordinatorOnlyStrategy | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/CoordinatorOnlyStrategy.java` |
| ExternalDistributionContext | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionContext.java` |
| ExternalDistributionPlan | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionPlan.java` |
| NodeEligibilityStrategy | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/NodeEligibilityStrategy.java` |
| ExternalSplit (SPI) | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSplit.java` |
| FileSplit | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplit.java` |
| CoalescedSplit | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/CoalescedSplit.java` |
| SplitCoalescer | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/SplitCoalescer.java` |
| FlightSplit | `/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightSplit.java` |
| FileSplitProvider | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplitProvider.java` |
| FlightSplitProvider | `/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightSplitProvider.java` |
| SplitDiscoveryPhase | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/SplitDiscoveryPhase.java` |
| ExternalSliceQueue | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSliceQueue.java` |
| RangeStorageObject | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/RangeStorageObject.java` |
| ComputeService (wiring) | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java` |
| DataNodeComputeHandler (dispatch) | `/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/DataNodeComputeHandler.java` |
| ExtendedDistributionPropertyTests | `/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExtendedDistributionPropertyTests.java` |
| ExternalDistributionPropertyTests | `/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionPropertyTests.java` |
| WeightedRoundRobinStrategyTests | `/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/WeightedRoundRobinStrategyTests.java` |
| AdaptiveStrategyTests | `/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/AdaptiveStrategyTests.java` |
