# Deep Dive: Pipeline Breaker Distribution for External Data Sources

**Date:** 2026-03-04 | **Branch:** `esql/connector-spi-v3` (latest main)

---

## Human-Readable Summary

### The core mechanism: how distribution decisions are made

The query planner decides where to split work between coordinator and data nodes by looking for a specific marker in the plan tree. This marker — a "fragment" — is only created for Elasticsearch indices. It represents a chunk of the logical plan that should be sent to a data node for local execution.

When the planner encounters a **pipeline breaker** (an operation that must consume all input before producing output — aggregation, limit, sort+limit, time-series info), it checks whether the data below it is marked as a fragment. If yes, it inserts an **exchange boundary** — a point where data flows from data nodes to the coordinator. The pipeline breaker is then split: one copy runs on data nodes (partial/initial work), another runs on the coordinator (merge/final work).

External sources produce a different plan node — one the planner does not recognize as a fragment. So no exchange is inserted, no split occurs, and every pipeline breaker runs entirely on the coordinator. This is not specific to aggregation: **every pipeline breaker** is affected.

### The five pipeline breakers and their impact

There are five pipeline breakers in the system. All five use the same gate. All five are blocked for external sources.

#### 1. STATS (Aggregation) — Two-phase split blocked

**On ES indices:** `FROM logs | STATS count(*) BY region` runs in two phases. Each data node reads its local shards and produces partial results — intermediate state containing partial counts per region. The coordinator merges these into a single final result. If a data node has multiple shards, there's a third step: an intermediate per-node merge before sending to the coordinator. Raw data never leaves the data nodes — only compact intermediate summaries.

**On external sources:** `FROM s3:"logs/*.parquet" | STATS count(*) BY region` runs in one phase, entirely on the coordinator. All raw data from every Parquet file streams through a single aggregation pipeline on one node. There is no distribution, no parallelism, no partial aggregation.

**Impact:**
- **No parallelism.** A single thread on the coordinator processes all data. On ES indices, N data nodes work in parallel.
- **All raw data transits the coordinator.** Every row flows through the coordinator, even though only compact group summaries are needed. For a query over 1 billion rows with 50 region groups, the coordinator processes 1 billion rows instead of receiving 50 × N partial summaries.
- **No memory relief.** The aggregation operator can periodically flush partial results to limit memory usage — but this optimization only activates in the partial-output modes used by the two-phase path. Single-phase mode never gets it.

#### 2. LIMIT — Data-node limit blocked

**On ES indices:** `FROM logs | LIMIT 100` inserts a limit on each data node. Each node stops reading after 100 rows and sends them to the coordinator, which applies a final limit across all nodes. This is extremely fast — each node reads at most 100 rows from its local shards.

**On external sources:** The limit runs only on the coordinator. All data from the external source streams to the coordinator, which discards everything after the first 100 rows. For a 10GB dataset across 100 files, the coordinator reads all 10GB to return 100 rows.

**Impact:**
- **The most common workflow — peek at data — is unusably slow.** `| LIMIT 100` should be near-instant but instead scans the full dataset.
- Note: the optimizer-level `PushLimitToSource` rule (item #9 in our planning doc) independently pushes limit into the source operator. That's a separate mechanism — it stops the source reader early. This pipeline-breaker issue is about distributing the limit across nodes.

#### 3. TopN (SORT + LIMIT) — Data-node sort+limit blocked

**On ES indices:** `FROM logs | SORT @timestamp DESC | LIMIT 100` runs TopN on each data node. Each node sorts its local data, keeps only the top 100 rows, and sends them to the coordinator. The coordinator merges the pre-sorted results from all nodes. Additionally, the coordinator knows the data arrives pre-sorted, enabling a more efficient merge (sorted input optimization).

**On external sources:** Sort and limit both run on the coordinator. All data streams to the coordinator, which sorts the entire dataset and then keeps 100 rows. No per-node sorting, no per-node limiting, no sorted-input optimization.

**Impact:**
- **Worst-case network transfer.** Every row from every file crosses the network to the coordinator, even though only 100 rows are needed.
- **No sorted-input optimization.** The coordinator must do a full sort instead of a merge of pre-sorted streams.

#### 4. MetricsInfo — Two-phase metadata extraction blocked

**On ES indices:** The `METRICS` command extracts metric metadata (field names, types, time-series dimensions) from shards. Each data node extracts metadata locally (INITIAL mode), sends partial results to the coordinator, which merges them (FINAL mode).

**On external sources:** Not applicable today (METRICS/TS commands require TIME_SERIES index mode, which external sources don't support). However, if future external sources support time-series semantics, this would be blocked.

**Impact:** None currently. Mentioned for completeness.

#### 5. TsInfo — Two-phase time-series info blocked

**On ES indices:** Similar to MetricsInfo — extracts time-series granularity information per shard, merges on coordinator.

**On external sources:** Same as MetricsInfo — not applicable today.

**Impact:** None currently. Mentioned for completeness.

### The root cause in detail

The planner walks the plan top-down and maps logical nodes to physical nodes. When it encounters an ES index at a leaf, it wraps it in a fragment marker. Streaming operators above it (WHERE, EVAL, RENAME, etc.) grow the fragment — they get included in the envelope that will be sent to data nodes.

When a pipeline breaker is reached, the planner checks: "is my child a fragment?" If yes, it wraps the entire sub-plan (including the pipeline breaker) in the fragment, inserts an exchange boundary, and creates the distributed split. If no, it creates a coordinator-only physical operator.

External sources produce a physical execution node directly — no fragment marker. Streaming operators above them produce physical pipeline operators directly. When a pipeline breaker is reached, the child is a physical operator, not a fragment. The gate fails. Coordinator-only.

### Why the distribution infrastructure is unreachable

The system has a complete distribution pipeline for external sources: split discovery finds file-level splits, an adaptive strategy decides whether to distribute, a data-node handler receives and processes splits. But this infrastructure requires an exchange boundary in the plan to trigger distribution. Since the planner never inserts one for external sources, the execution path always falls to coordinator-only, regardless of how many splits exist or what the strategy decides.

The integration tests run external source queries with three different distribution strategies on a 3-node cluster. All three strategies produce identical results — because distribution never actually happens. The tests verify result correctness, not plan structure, so this silent coordinator-only execution isn't detected.

### Proposed fix

The fix has three parts:

**Part 1: Teach the planner to treat external sources as distributable.**

The planner's gate checks for a fragment marker — a wrapper around a logical plan that gets sent to data nodes for local replanning. There are two approaches:

*Option A — Extend the fragment mechanism:* When the planner encounters an external source at a leaf, wrap it in a fragment marker, just like ES indices. The data-node local planner already knows how to handle external sources (it delegates to the same factory method). This means streaming operators above the external source would naturally grow the fragment, and pipeline breakers would trigger the exchange insertion through the existing mechanism.

This is the cleanest approach because it reuses all existing machinery. The main question is whether the data-node local planner correctly handles the full range of external source plan shapes when they arrive via the fragment mechanism. The local planner already has a handler for external sources, but it's currently only reachable through manually-constructed test plans.

*Option B — Direct physical split:* Instead of going through the fragment mechanism, add a second gate in the pipeline breaker handler that checks for external sources directly and inserts the two-phase structure explicitly. This avoids touching the fragment mechanism but duplicates logic.

**Recommendation:** Option A. It's less code, reuses proven machinery, and automatically extends to all current and future pipeline breakers.

**Part 2: Add per-node reduction for external sources.**

On the ES index path, when a data node has multiple shards, partial results from each shard are merged within the node before sending to the coordinator. This is the INTERMEDIATE aggregation mode — it prevents flooding the coordinator with per-shard partial results.

The external source data-node handler has no equivalent. When a node is assigned 50 splits, it would produce 50 sets of partial results without merging them. The fix: add a per-node reduction step that mirrors what the ES index path does. The reduction plan generation already handles all pipeline breaker types — INTERMEDIATE aggregation, TopN merge, limit merge — so it's a matter of wiring it in.

**Part 3: Verify exchange collapse for the non-distributed path.**

When the strategy decides NOT to distribute (single node, few splits), the system must strip the exchange boundary and run everything on the coordinator as before. This mechanism already exists — there's a method that removes exchanges wrapping external sources. It needs to be verified that it works correctly for all pipeline breaker types, not just the simple pass-through case.

### Estimate reassessment

The planning document estimates 4d–1.5w for "Two-phase STATS." Given that the scope is actually all pipeline breakers (not just STATS), the estimate should be understood as covering:

- Planner change to treat external sources as fragments (Part 1) — the core fix
- Per-node reduction for external sources (Part 2) — follows existing patterns closely
- Exchange collapse verification (Part 3) — largely existing code, needs testing
- Tests for each pipeline breaker type on external sources

The estimate is **still reasonable** because:
- All compute operators are ready (INITIAL/FINAL/INTERMEDIATE modes fully tested)
- The distribution infrastructure is ready (split assignment, data-node handler, strategy selection)
- The data-node local planner already handles external sources
- The per-node reduction logic already handles all pipeline breaker types
- The fix for one pipeline breaker naturally fixes all of them (same gate)

The work is concentrated in the planner (one method) and the data-node handler (adding the reduction step). The rest is testing and verification.

---

## Detailed Evidence

### 1. The gate that controls distribution for all pipeline breakers

The planner walks the logical plan top-down and maps it to a physical plan. When it encounters any pipeline breaker, it calls the same method to check whether the child plan is a distributable fragment.

**The gate method** (`Mapper.java:269-278`):
```
addExchangeForFragment(LogicalPlan logical, PhysicalPlan child):
    if (child is a FragmentExec) → wrap in new FragmentExec(logical) + ExchangeExec → distributed
    else → return child unchanged → coordinator-only
```

**Called by every pipeline breaker:**

| Pipeline breaker | Mapper.java line | What happens when exchange IS inserted | What happens when it's NOT |
|-----------------|-----------------|---------------------------------------|---------------------------|
| **Aggregate** (STATS) | 121 | INITIAL on data nodes, FINAL on coordinator | SINGLE mode on coordinator |
| **Limit** | 141 | Limit on both data nodes and coordinator | Limit only on coordinator |
| **TopN** (SORT+LIMIT) | 146 | TopN on data nodes, TopN merge on coordinator (with sorted-input optimization) | TopN only on coordinator |
| **MetricsInfo** | 161 | INITIAL on data nodes, FINAL on coordinator | FINAL only on coordinator |
| **TsInfo** | 173 | INITIAL on data nodes, FINAL on coordinator | FINAL only on coordinator |

This is the **sole condition** for distribution. Nothing else is checked — not the number of splits, not the data size, not any configuration flag. Fragment or not.

**How ES indices pass the gate** (`Mapper.java:86-87`):
When the planner encounters an ES index relation at a leaf, it wraps it in a fragment marker. Streaming operators (WHERE, EVAL, etc.) grow the fragment as they stack on top (`Mapper.java:98-109`). When any pipeline breaker is reached, the fragment is detected, an exchange boundary is inserted, and the distributed split is created.

**How external sources fail the gate** (`MapperUtils.java:75-77`):
When the planner encounters an external relation, it calls a factory method that produces a physical execution node directly — no fragment marker. Streaming operators above it produce physical pipeline operators directly without growing a fragment. When any pipeline breaker is reached, the child is a physical operator, not a fragment. The gate fails. Coordinator-only.

**The data-node local planner handles all five breakers** (`LocalMapper.java:85-110`):
When a fragment arrives on a data node, it's replanned locally. Each pipeline breaker gets a data-node-appropriate mode:
- Aggregate → `AggregatorMode.INITIAL` (line 87)
- Limit → `LimitExec` with the original limit value (line 91)
- TopN → `TopNExec` with the original sort + limit (line 95)
- MetricsInfo → `MetricsInfoExec.Mode.INITIAL` (line 104)
- TsInfo → `TsInfoExec.Mode.INITIAL` (line 109)

**Per-node reduction handles all five breakers** (`PlannerUtils.java:141-182`):
When multiple shards on the same node produce partial results, a per-node reduction step merges them:
- Aggregate → `AggregatorMode.INTERMEDIATE` (line 159) — merges partial aggregation state
- TopN → `TopNReduction` (line 158) — merges sorted streams
- MetricsInfo → `MetricsInfoExec.Mode.INTERMEDIATE` (line 167) — merges metadata
- TsInfo → `TsInfoExec.Mode.INTERMEDIATE` (line 177) — merges TS info
- Limit and all others → generic `ReducedPlan` (line 180)

### 2. What the planner produces for each pipeline breaker

#### STATS (Aggregation)

**ES index:** `FROM logs | STATS count(*) BY region`
```
Coordinator:                    Data node (per node):
  AggregateExec [FINAL]           ExchangeSinkExec
    ExchangeSourceExec              AggregateExec [INITIAL]
                                      EsSourceExec
```

**External source:** `FROM s3:"logs/*.parquet" | STATS count(*) BY region`
```
Coordinator only:
  AggregateExec [SINGLE]
    ExternalSourceExec
```

SINGLE mode: raw data in, final results out. No intermediate state, no distribution.

**Evidence** (`ExternalDistributionTests.java:46-62`): A test explicitly confirms the planner produces SINGLE mode:
```
assertEquals(AggregatorMode.SINGLE, aggExec.getMode());
assertTrue("Expected ExternalSourceExec child", aggExec.child() instanceof ExternalSourceExec);
```

#### LIMIT

**ES index:** `FROM logs | LIMIT 100`
```
Coordinator:                    Data node (per node):
  LimitExec [100]                 ExchangeSinkExec
    ExchangeSourceExec              LimitExec [100]
                                      EsSourceExec
```
Each data node stops at 100 rows. Coordinator applies final limit across all nodes.

**External source:** `FROM s3:"logs/*.parquet" | LIMIT 100`
```
Coordinator only:
  LimitExec [100]
    ExternalSourceExec
```
All data streams to coordinator. Limit applied only after full read.

#### TopN (SORT + LIMIT)

**ES index:** `FROM logs | SORT @timestamp DESC | LIMIT 100`
```
Coordinator:                    Data node (per node):
  TopNExec [100, sorted-input]    ExchangeSinkExec
    ExchangeSourceExec              TopNExec [100]
                                      EsSourceExec
```
Each data node sorts locally and keeps top 100. Coordinator merges pre-sorted streams (sorted-input optimization, `Mapper.java:149-152`).

**External source:** `FROM s3:"logs/*.parquet" | SORT @timestamp DESC | LIMIT 100`
```
Coordinator only:
  TopNExec [100]
    ExternalSourceExec
```
All data streams to coordinator. Full sort on coordinator. No sorted-input optimization.

#### MetricsInfo / TsInfo

Not applicable to external sources today (require TIME_SERIES index mode). Included for completeness — if future external sources support time-series, these would also be blocked by the same gate (`Mapper.java:161, 173`).

### 3. The aggregation mode system

The compute engine supports four aggregation modes (`AggregatorMode.java:36-56`):

| Mode | Input | Output | Used when |
|------|-------|--------|-----------|
| **INITIAL** | raw rows | intermediate state | Data node: partial aggregation |
| **INTERMEDIATE** | intermediate state | intermediate state | Per-node merge of multiple shards |
| **FINAL** | intermediate state | final values | Coordinator: merge all partials |
| **SINGLE** | raw rows | final values | No distribution: everything in one pass |

The mode controls two runtime decisions in the aggregation operator:
- **Input dispatch** (`Aggregator.java:36-45`): INITIAL/SINGLE call `addRawInput()`. INTERMEDIATE/FINAL call `addIntermediateInput()`.
- **Output dispatch** (`Aggregator.java:47-53`): INITIAL/INTERMEDIATE call `evaluateIntermediate()` (emit partial state). FINAL/SINGLE call `evaluateFinal()` (emit final scalar).

Additionally, the periodic partial-emit optimization (`HashAggregationOperator.java:314-326`) — which flushes accumulated state to prevent memory blowup — only fires when the output is partial (INITIAL/INTERMEDIATE). SINGLE mode never gets this benefit.

### 4. The distribution infrastructure exists but is unreachable

The complete distribution pipeline for external sources is implemented:

- **Split discovery** (`SplitDiscoveryPhase.java:39-122`): Walks the plan, finds external source nodes, calls each source's split provider to discover file-level splits.
- **Strategy selection** (`ComputeService.java:245-256`): Chooses between coordinator-only, round-robin, adaptive, and weighted-round-robin strategies.
- **Adaptive strategy** (`AdaptiveStrategy.java:42-77`): Decides to distribute when there are multiple splits and the query has aggregation or many splits.
- **Data node handler** (`DataNodeComputeHandler.java:792-879`): Receives splits, injects them into the plan, and runs local compute.
- **Plan breaking** (`PlannerUtils.java:116-128`): Splits the plan at exchange boundaries into coordinator and data-node halves.

**The disconnection** (`ComputeService.java:457-530`):
1. Split discovery runs and populates splits on the external source node
2. The adaptive strategy decides to distribute
3. `breakPlanBetweenCoordinatorAndDataNode` looks for exchange boundaries — finds none (the planner never inserted one)
4. Data node plan is null
5. Execution falls to the coordinator-only path (line 490), ignoring the strategy's decision
6. The distributed path (line 532) is never reached

**Evidence**: The integration tests (`ExternalDistributedSpecIT`) run every external source query with three different distribution strategies on a 3-node cluster. All three strategies produce identical results — because distribution never actually happens. The tests verify result correctness, not plan structure, so this silent coordinator-only execution isn't detected.

### 5. Test coverage gap

**Well-tested:**
- ES index two-phase plan structure: 15+ tests in `PhysicalPlanOptimizerTests` verify INITIAL below exchange, FINAL above
- Compute engine INITIAL→FINAL correctness: `ForkingOperatorTestCase` proves partial→final produces same results as single-pass
- External source STATS result correctness: CSV spec tests verify COUNT, AVG, MIN, MAX, grouped aggregations produce correct answers

**Not tested:**
- No test verifies the planner produces INITIAL/FINAL for external sources when distributed
- Distribution tests (`ExternalSourceDataNodeTests`, `ExternalDistributionPropertyTests`) manually construct plans with INITIAL mode — testing a code path the system can't currently reach through normal query flow
- The integration tests can't distinguish SINGLE (all on coordinator) from INITIAL+FINAL (distributed) because both produce correct results — the difference is only performance

### 6. The fix: what specifically needs to change

**Planner (Mapper):** The aggregation handler (`Mapper.java:115-138`) needs to recognize external sources as distributable. This is more than changing the `instanceof FragmentExec` check, because the current mechanism relies on wrapping the *logical* plan in a fragment envelope and replanning it on data nodes. External sources bypass this logical-to-physical replanning.

Two viable approaches:

**Approach A — Extend the fragment envelope:** When the planner encounters an external relation, wrap it in a fragment marker just like ES indices. This requires the data-node local planner (`LocalMapper`) to handle external relations — currently it delegates them back to `MapperUtils.mapLeaf()` which produces coordinator-only nodes.

**Approach B — Direct physical split:** Instead of going through the fragment mechanism, directly insert the two-phase structure in the planner when an aggregation sits above an external source. Create `AggregateExec(FINAL) → ExchangeExec → AggregateExec(INITIAL) → ExternalSourceExec` explicitly, without relying on the fragment envelope and local replanning.

**Data node handler:** The external source handler (`DataNodeComputeHandler.java:792-879`) currently has no per-node reduction step. The ES shard handler has one (`PlannerUtils.java:159`) — it inserts an INTERMEDIATE aggregation that merges results from multiple shards on the same node before sending to the coordinator. The external source handler would need a similar step to merge results from multiple splits assigned to the same node.

**Exchange collapse:** The non-distributed path already has logic to strip exchange boundaries around external sources (`ComputeService.java:299-306`). This ensures that when the strategy decides NOT to distribute, the exchange is removed and the plan runs on the coordinator as before.

### 7. Key file locations

| Component | File | Lines |
|-----------|------|-------|
| **The gate** (shared by all breakers) | `planner/mapper/Mapper.java` | 269-278 |
| Aggregate breaker handling | `planner/mapper/Mapper.java` | 115-138 |
| Limit breaker handling | `planner/mapper/Mapper.java` | 140-143 |
| TopN breaker handling | `planner/mapper/Mapper.java` | 145-156 |
| MetricsInfo breaker handling | `planner/mapper/Mapper.java` | 160-169 |
| TsInfo breaker handling | `planner/mapper/Mapper.java` | 172-175 |
| Fragment creation (ES indices) | `planner/mapper/Mapper.java` | 86-87 |
| Fragment growth (streaming ops) | `planner/mapper/Mapper.java` | 98-109 |
| External source leaf mapping | `planner/mapper/MapperUtils.java` | 68-84 |
| Data-node local planner (all breakers) | `planner/mapper/LocalMapper.java` | 85-110 |
| Per-node reduction (all breakers) | `planner/PlannerUtils.java` | 141-182 |
| Aggregation mode enum | `compute/aggregation/AggregatorMode.java` | 36-56 |
| Mode dispatch (non-grouping) | `compute/aggregation/Aggregator.java` | 36-53 |
| Mode dispatch (grouping) | `compute/aggregation/GroupingAggregator.java` | 37-74 |
| Periodic partial emit (INITIAL only) | `compute/operator/HashAggregationOperator.java` | 314-326 |
| Plan-to-operator mode flow | `planner/AbstractPhysicalOperationProviders.java` | 64-197 |
| TopN sorted-input optimization | `planner/mapper/Mapper.java` | 149-152 |
| Split discovery | `datasources/SplitDiscoveryPhase.java` | 39-122 |
| Distribution strategy dispatch | `plugin/ComputeService.java` | 245-284 |
| Execution path selection (3 paths) | `plugin/ComputeService.java` | 457-530 |
| Data node external handler | `plugin/DataNodeComputeHandler.java` | 792-879 |
| Plan breaking at exchanges | `planner/PlannerUtils.java` | 116-128 |
| Exchange collapse for external | `plugin/ComputeService.java` | 299-306 |
| Test: SINGLE mode confirmed | `plugin/ExternalDistributionTests.java` | 46-62 |
| Test: hand-built INITIAL plans | `plugin/ExternalSourceDataNodeTests.java` | 60, 398 |
| Test: compute engine two-phase | `compute/operator/ForkingOperatorTestCase.java` | 72-130 |

---

## Verdict on Planning Document Claims

| Claim | Verdict | Evidence |
|-------|---------|----------|
| "STATS on external sources runs single-phase on the coordinator" | **Verified True** | `ExternalDistributionTests.java:57` — SINGLE mode confirmed. `Mapper.java:269-278` — gate only passes FragmentExec. |
| "All raw rows from data nodes flow through one node" | **Verified True (stronger)** | Distribution is completely unreachable, not just inefficient. `ComputeService.java:490` — null dataNodePlan causes early return to coordinator-only path. |
| "The two-phase aggregation operators already exist and work for ES indices" | **Verified True** | `ForkingOperatorTestCase.java:72-130` — INITIAL→FINAL tested. `PhysicalPlanOptimizerTests.java:1066` — plan structure verified. |
| "The gap is planner wiring" | **Verified True** | `Mapper.java:269-278` — single `instanceof FragmentExec` check is the gate. |
| "Fix: widen the condition so the planner inserts the INITIAL/FINAL split" | **Partially True** | The condition needs widening, but it's not a simple one-line change. The fragment mechanism relies on logical-plan envelopes and data-node replanning. External sources need either (a) integration into the fragment mechanism or (b) a new physical-level split. Additionally, the data-node handler needs a per-node reduction step. |
| "Estimate: 4d–1.5w" | **Reasonable** | Operators are ready. Distribution infrastructure is ready. Work is concentrated in planner + data-node handler. Complexity is moderate — the fragment mechanism is non-trivial but well-precedented. Fixing the gate once fixes all pipeline breakers simultaneously. |

### Critical finding: scope is much wider than stated

The planning document frames this as a STATS-only issue. In fact, the same gate blocks distribution for **all pipeline breakers** on external sources:

| Pipeline breaker | Mapper line | Impact on external sources |
|-----------------|-------------|---------------------------|
| **STATS** | 121 | Single-phase aggregation on coordinator — no partial aggregation on data nodes |
| **LIMIT** | 141 | All data streams to coordinator before limit is applied |
| **TopN** (SORT+LIMIT) | 146 | Full sort on coordinator — no per-node sorting, no sorted-input merge optimization |
| **MetricsInfo** | 161 | Not applicable today (requires TIME_SERIES mode) |
| **TsInfo** | 173 | Not applicable today (requires TIME_SERIES mode) |

All use the same `addExchangeForFragment` method with the same `instanceof FragmentExec` check. **Fixing the gate for one pipeline breaker naturally fixes all of them** — the change is in the shared gate method and the fragment mechanism, not in each breaker individually.

This means item #21 in the planning document is actually more valuable than described. It's not "two-phase STATS" — it's "enable distributed execution for all pipeline breakers on external sources." The estimate remains reasonable because the fix is the same regardless of how many breakers benefit.
