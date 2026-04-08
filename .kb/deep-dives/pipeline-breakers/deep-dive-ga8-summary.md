# GA-8: Distributed Aggregation for External Sources — Deep Dive Summary

## Problem Statement

Pipeline breakers (STATS, LIMIT, TopN) execute as **single-phase, coordinator-only** operations for external sources, while ES indices get **two-phase distributed execution**. This means all data from external sources streams through one node, regardless of how many data nodes are available.

**PR with failing tests**: https://github.com/elastic/elasticsearch/pull/143596

## Root Cause

A single condition in `Mapper.addExchangeForFragment()` (Mapper.java:269-278) controls distribution for ALL pipeline breakers:

```java
private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
    if (child instanceof FragmentExec) {       // <-- THE GATE
        child = new FragmentExec(logical);
        child = new ExchangeExec(child.source(), child);
    }
    return child;
}
```

- ES indices: `EsRelation` → `FragmentExec` → passes the check → gets `ExchangeExec` → two-phase
- External sources: `ExternalRelation` → `ExternalSourceExec` → fails the check → no exchange → single-phase

This method is called by every pipeline breaker in `Mapper.mapUnary()`: Aggregate (line 121), Limit (line 141), TopN (line 146), MetricsInfo (line 161), TsInfo (line 173).

## Impact Per Pipeline Breaker

### STATS (Aggregate)

| Aspect | ES Index | External Source |
|--------|----------|-----------------|
| Mode | INITIAL on data nodes → FINAL on coordinator | SINGLE on coordinator |
| Parallelism | N data nodes process shards in parallel | Single thread, single node |
| Memory | Each node holds partial state only | Coordinator holds ALL intermediate state |
| Partial emit | Yes (INITIAL has `outputPartial=true`) | No (SINGLE has `outputPartial=false`) |

**ES plan** (fully localized):
```
LimitExec[1000]
  AggregateExec[FINAL]         ← coordinator: merge partials
    ExchangeExec
      AggregateExec[INITIAL]   ← data node: partial aggregation
        EsStatsQueryExec       ← Lucene count pushdown
```

**External plan**:
```
LimitExec[1000]
  AggregateExec[SINGLE]        ← coordinator: full aggregation
    ExternalSourceExec
```

### LIMIT

**ES plan**:
```
LimitExec[10]                  ← coordinator: final limit
  ExchangeExec
    LimitExec[10]              ← data node: early limit (reduces data transfer)
      EsQueryExec[limit=10]
```

**External plan**:
```
LimitExec[10]                  ← coordinator only
  ExternalSourceExec           ← reads ALL rows, then limits
```

### TopN (SORT + LIMIT)

**ES plan**:
```
TopNExec[emp_no DESC, 10]      ← coordinator: merge-sort top-10
  ExchangeExec
    TopNExec[emp_no DESC, 10]  ← data node: local top-10
      EsQueryExec[sort=[emp_no DESC], limit=10]  ← Lucene sort+limit pushdown
```

**External plan**:
```
TopNExec[emp_no DESC, 10]      ← coordinator only
  ExternalSourceExec           ← reads ALL rows
```

## Operational Implications Per Pipeline Breaker

### STATS: OOM Risk from Unbounded Accumulation

**The core danger**: `HashAggregationOperator.shouldEmitPartialResultsPeriodically()` (line 314-326) has this gate:

```java
protected boolean shouldEmitPartialResultsPeriodically() {
    if (aggregatorMode.isOutputPartial() == false) {
        return false;  // SINGLE mode: NEVER emits partials
    }
    // INITIAL mode: emits when unique groups exceed threshold
    return rowsAddedInCurrentBatch * partialEmitUniquenessThreshold <= numKeys;
}
```

In **INITIAL mode** (ES indices), when the number of unique group keys grows large, the operator periodically flushes its `BlockHash` and aggregator state, reinitializes them (lines 267-276), and sends partial results downstream. This bounds memory usage per operator — the coordinator's FINAL aggregate merges these partial results incrementally.

In **SINGLE mode** (external sources), `isOutputPartial()` returns false, so `shouldEmitPartialResultsPeriodically()` always returns false at line 316. **All group keys and their accumulator state are held in memory until `finish()` is called.** For high-cardinality groupings (e.g., `STATS count(*) BY user_id` on a 100M-row Parquet dataset), this means:

- The coordinator's `BlockHash` grows unboundedly — one entry per unique group key
- Each `GroupingAggregator` holds state for every group
- No partial flush, no incremental merge
- Circuit breaker (`driverContext.breaker()`) will eventually trip, but the error is unrecoverable at that point

**Concrete scenario**: `EXTERNAL "s3://logs/*.parquet" | STATS avg_duration = AVG(duration) BY session_id` where session_id has 50M unique values. In INITIAL mode, data nodes would periodically flush partial AVG state (sum, count per group). In SINGLE mode, the coordinator must hold all 50M groups in memory simultaneously.

**Non-grouping aggregations** (e.g., `STATS count(*)`) are safe — `AggregationOperator` (non-grouping) has fixed-size state regardless of mode. The OOM risk is specific to `HashAggregationOperator` with GROUP BY.

### LIMIT: Full Data Transfer Before Truncation

**How LIMIT works**: `LimitOperator` (lines 68-86) has perfect early termination — `needsInput()` returns false once `limiter.remaining() <= 0`, and excess pages are released immediately (line 79). The operator itself is memory-safe.

**The problem is upstream**: Without an exchange, there is no data-node-side `LimitExec` to cap data transfer early. The `ExternalSourceExec` reads and transmits ALL rows to the coordinator pipeline. The coordinator's `LimitOperator` then discards everything beyond the limit.

**Concrete scenario**: `EXTERNAL "s3://logs/*.parquet" | LIMIT 10` on a 1TB dataset. With distribution (ES index path), each data node reads at most 10 rows and sends them. Without distribution, the coordinator reads all data from S3, materializes it into pages, and discards all but 10 rows. The compute is wasted, but the real cost is:

- **Network I/O**: All data transferred from S3 to coordinator (1TB)
- **CPU**: All data deserialized and formatted into pages
- **Coordinator memory**: Pages are processed one at a time (bounded), but throughput is terrible

No OOM risk from the operator itself, but orders-of-magnitude slower than necessary.

### TopN (SORT + LIMIT): Full Materialization on Coordinator

**How TopN works**: `TopNOperator` uses a fixed-size min-heap (lines 582-654). Memory is pre-allocated via circuit breaker for exactly `topCount` rows (line 590-593). Each incoming row is compared against the heap; only qualifying rows are kept. Individual rows use `BreakingBytesRefBuilder` which checks the circuit breaker on every growth (line 74 of `BreakingBytesRefBuilder`).

**The operator is memory-safe** — bounded by `topCount` rows. But the same upstream problem as LIMIT applies: without distribution, ALL data must flow through the coordinator to be evaluated against the heap.

**Additional cost vs LIMIT**: TopN must actually compare every row (heap insertion is O(log N) per row), not just count positions. For wide rows with complex sort keys, this is CPU-intensive.

**Concrete scenario**: `EXTERNAL "s3://logs/*.parquet" | SORT timestamp DESC | LIMIT 100` on a 1TB dataset with 1B rows. With distribution, each data node sorts and retains only 100 rows locally, then sends 100 rows to the coordinator for a final merge-sort. Without distribution, the coordinator receives all 1B rows and runs heap insertion on each one — single-threaded, on one node.

**No OOM risk** (heap is bounded), but performance is O(N) on the coordinator where N is the total row count across all files.

### Summary Table: Risk by Operation

| Operation | OOM Risk (SINGLE) | Performance Impact | Root Cause |
|-----------|-------------------|-------------------|------------|
| `STATS ... BY high_cardinality` | **HIGH** — unbounded accumulation, no partial emit | Catastrophic — single-node, single-thread, all state in memory | `shouldEmitPartialResultsPeriodically()` disabled |
| `STATS` (no GROUP BY) | Low — fixed-size state | Bad — single-node processing of all data | No parallelism |
| `LIMIT N` | None — bounded operator | Very bad — all data read before truncation | No pushdown to data nodes |
| `SORT ... LIMIT N` | None — fixed heap | Very bad — all rows compared on coordinator | No pushdown to data nodes |

## How Two-Phase Aggregation Works (Operator Level)

The `AggregatorMode` enum controls runtime behavior through two boolean flags:

| Mode | inputPartial | outputPartial | Description |
|------|-------------|---------------|-------------|
| INITIAL | false | true | Raw input → intermediate state |
| INTERMEDIATE | true | true | Intermediate → intermediate (node-level merge) |
| FINAL | true | false | Intermediate → final result |
| SINGLE | false | false | Raw input → final result (no distribution) |

The mode flows through: `Mapper` → `AggregateExec.getMode()` → `AbstractPhysicalOperationProviders.groupingPhysicalOperation()` → `AggregatorFunctionSupplier.aggregatorFactory(mode, channels)` → `Aggregator(function, mode)` → runtime dispatch via `mode.isInputPartial()` / `mode.isOutputPartial()`.

Key consequence for SINGLE mode: `HashAggregationOperator.shouldEmitPartialResultsPeriodically()` returns false when `isOutputPartial()` is false. This means SINGLE mode cannot flush intermediate state to prevent memory blowup with many groups.

## Existing Distribution Infrastructure (Ready but Unreachable)

The infrastructure for distributing external source work across data nodes is fully implemented:

- **`AdaptiveStrategy`** decides to distribute when `splits > 1` and `hasAggregation || manySplits`
- **`DataNodeComputeHandler.handleExternalSourceRequest()`** runs external splits on data nodes
- **`startExternalComputeOnDataNodes()`** dispatches `DataNodeRequest` with external splits
- **`collapseExternalSourceExchanges()`** strips exchanges for the non-distributed path

But all of this requires an `ExchangeExec` in the plan — which the Mapper never creates for external sources.

**The gap in `ComputeService.executePlan()`** (line 532-551):
```java
if (distributionResult.isDistributed() && hasConcreteIndices == false) {
    executeExternalDistribution(..., (ExchangeSinkExec) dataNodePlan, ...);
}
```
Since `PlannerUtils.breakPlanBetweenCoordinatorAndDataNode()` returns null dataNodePlan when there's no ExchangeExec, this path is unreachable for normal external source queries.

## Test Coverage Analysis

### Well-Tested
1. **ES index two-phase**: `PhysicalPlanOptimizerTests` — extensive plan structure tests verify INITIAL below Exchange, FINAL above
2. **Compute operators**: `ForkingOperatorTestCase.testInitialFinal()` — proves INITIAL→FINAL produces correct results
3. **External source correctness**: CSV spec tests verify COUNT, AVG, MIN, MAX, grouped aggregations produce correct results
4. **External distribution mechanics**: Split assignment, plan breaking, exchange collapsing all tested

### NOT Tested (Gaps)
1. **No test verifies INITIAL/FINAL for external sources when distributed** — the only mode assertion (`ExternalDistributionTests:57`) confirms SINGLE
2. **Hand-constructed INITIAL-mode plans in distribution tests are unreachable** — the Mapper produces SINGLE, so those tests exercise a code path the system can't enter
3. **No plan-level test** takes external STATS through full optimizer and checks plan shape (like `PhysicalPlanOptimizerTests` does for ES)
4. **Integration tests can't distinguish** SINGLE vs INITIAL+FINAL — both produce correct results, but SINGLE doesn't scale

## Possible Solutions

### Option A: Extend `addExchangeForFragment` (Simplest)
Change the condition from `child instanceof FragmentExec` to `child instanceof FragmentExec || child instanceof ExternalSourceExec`.

**Problem**: This alone doesn't give two-phase aggregation on data nodes. `FragmentExec` carries the pipeline breaker logical plan to data nodes where `LocalMapper` creates `AggregateExec(INITIAL)`. `ExternalSourceExec` doesn't carry logical plan nodes — data nodes would just get `ExternalSourceExec` with no partial aggregation.

### Option B: Create `ExternalFragmentExec`
A new node type that triggers exchange insertion AND carries enough information for data-node local planning.

### Option C: Post-Mapper optimizer pass
Insert exchanges in a new optimizer pass between the Mapper and `executePlan()`. This could inspect the plan structure and add exchanges where appropriate.

### Option D: Mode conversion in distribution path
When `AdaptiveStrategy` decides to distribute, convert `AggregateExec(SINGLE)` to `AggregateExec(INITIAL)` + `AggregateExec(FINAL)` with an exchange between them. This keeps the Mapper simple and moves the distribution decision closer to execution time.

## Key Files

| Component | File | Key Lines |
|-----------|------|-----------|
| The gate | `planner/mapper/Mapper.java` | 269-278 (`addExchangeForFragment`) |
| Pipeline breaker handling | `planner/mapper/Mapper.java` | 115-173 (`mapUnary`) |
| External leaf mapping | `planner/mapper/MapperUtils.java` | 68-85 (`mapLeaf`) |
| Data-node local mapping | `planner/mapper/LocalMapper.java` | 85-88 (always INITIAL) |
| Coordinator/data-node split | `planner/PlannerUtils.java` | 116-128 (`breakPlanBetweenCoordinatorAndDataNode`) |
| Distribution execution | `plugin/ComputeService.java` | 443-551 (`executePlan`) |
| External data-node handling | `plugin/DataNodeComputeHandler.java` | 792-879 (`handleExternalSourceRequest`) |
| Mode-to-operator binding | `planner/AbstractPhysicalOperationProviders.java` | 64-197 (`groupingPhysicalOperation`) |
| AggregatorMode enum | `compute/.../aggregation/AggregatorMode.java` | 36-73 |
| Failing tests (PR) | `plugin/ExternalPipelineBreakerComparisonTests.java` | Full file |

## Estimate

From the planning doc:
- **Human**: 2-3 weeks (requires deep understanding of Mapper, LocalMapper, exchange semantics)
- **H+AI**: 1-1.5 weeks

The core change is small (extending the exchange condition), but the ripple effects need careful handling: ensuring partial aggregation runs on data nodes, node-level reduction (INTERMEDIATE mode), interaction with `collapseExternalSourceExchanges`, and correctness across all aggregation functions.
