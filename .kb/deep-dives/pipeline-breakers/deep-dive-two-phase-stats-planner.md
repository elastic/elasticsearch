# Deep Dive: Two-Phase STATS/Aggregation Split in ES|QL Planner

**Date**: 2026-03-04
**Branch**: `esql/connector-spi-v3` (main @ 970b4789c35)

---

## 1. Where the Two-Phase Split Decision Is Made

### Primary Location: `Mapper.mapUnary()` (Lines 115-138)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

The entire two-phase aggregation split logic lives in `Mapper.mapUnary()` at lines 115-138. Here's the exact flow:

```java
// Line 115
if (unary instanceof Aggregate aggregate) {
    List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);

    // Line 121: CRITICAL — attempt to split at fragment boundary
    mappedChild = addExchangeForFragment(aggregate, mappedChild);

    // Line 124: If exchange was added → two-phase (distributed)
    if (mappedChild instanceof ExchangeExec exchange) {
        mappedChild = new ExchangeExec(mappedChild.source(), intermediate, true, exchange.child());
    }
    // Line 128: If NO exchange was added → single-phase (coordinator only)
    // ... UNLESS there are NonEvaluatableGroupingFunctions
    else if (aggregate.groupings()
        .stream()
        .noneMatch(group -> group.anyMatch(expr -> expr instanceof GroupingFunction.NonEvaluatableGroupingFunction))) {
            return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.SINGLE, intermediate);
        } else {
            mappedChild = MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
        }

    // Line 137: ALWAYS creates a FINAL agg at the coordinator
    return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.FINAL, intermediate);
}
```

### The Decision Tree

1. **`addExchangeForFragment()` is called (line 121)**: This is the gate. If the child is a `FragmentExec`, it gets wrapped in an exchange. If not, nothing happens.

2. **If exchange was added** (child is `ExchangeExec`, line 124):
   - The exchange output is rewritten to use intermediate attributes
   - A FINAL aggregation is created on the coordinator (line 137)
   - Result: **INITIAL** (inside FragmentExec, planned later by LocalMapper) → Exchange → **FINAL** (coordinator)

3. **If NO exchange was added** (child is NOT ExchangeExec):
   - Check if any grouping uses `NonEvaluatableGroupingFunction` (e.g., `CATEGORIZE`)
   - **If no NonEvaluatable functions**: Create `AggregatorMode.SINGLE` agg (line 131) and return immediately — no FINAL
   - **If NonEvaluatable functions present**: Create `INITIAL` agg (line 133) then `FINAL` agg (line 137) — two-phase on coordinator only

---

## 2. The Condition That Limits Distribution to ES Indices

### The Gate: `addExchangeForFragment()` (Lines 269-278)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

```java
// Line 269
private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
    // Line 273: THE CHECK — only works if child is FragmentExec
    if (child instanceof FragmentExec) {
        child = new FragmentExec(logical);
        child = new ExchangeExec(child.source(), child);
    }
    return child;
}
```

**This is the exact condition**: `child instanceof FragmentExec`. Nothing else.

### How FragmentExec Gets Created

`FragmentExec` is created only in two places in `Mapper`:

1. **`mapLeaf()` line 87**: When the leaf is an `EsRelation`
   ```java
   if (leaf instanceof EsRelation esRelation) {
       return new FragmentExec(esRelation);  // Creates FragmentExec
   }
   ```

2. **`mapUnary()` lines 98-109**: When the child is already a `FragmentExec` and the current node is a streaming (non-breaking) operator, it wraps the whole logical subtree into a new `FragmentExec`:
   ```java
   if (mappedChild instanceof FragmentExec) {
       if (unary instanceof PipelineBreaker == false
           || (unary instanceof Limit limit && limit.local())
           || (unary instanceof TopN topN && topN.local())) {
           return new FragmentExec(unary);  // Grows the fragment
       }
   }
   ```

### What ExternalRelation Produces Instead

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java` (lines 75-77)

```java
if (p instanceof ExternalRelation external) {
    return external.toPhysicalExec();  // Returns ExternalSourceExec, NOT FragmentExec
}
```

`ExternalRelation.toPhysicalExec()` returns an `ExternalSourceExec` (line 115 of ExternalRelation.java), which is NOT a `FragmentExec`.

### The Consequence

When `Mapper.mapUnary()` processes `STATS ... | EXTERNAL "s3://..."`:

1. `mapInner()` recurses to the leaf → `ExternalRelation` → `ExternalSourceExec` (not `FragmentExec`)
2. In `mapUnary()`, streaming operators (WHERE, EVAL, etc.) do NOT wrap into FragmentExec because `mappedChild instanceof FragmentExec` is false — they just call `MapperUtils.mapUnary()` and produce physical pipeline operators directly
3. When we reach the `Aggregate`:
   - `mappedChild` is something like `FilterExec(ExternalSourceExec(...))` — NOT a `FragmentExec`
   - `addExchangeForFragment()` does nothing (child is not FragmentExec)
   - No exchange is created
   - Falls to the `else if` branch (line 128)
   - If no `NonEvaluatableGroupingFunction` → **SINGLE** mode
   - Otherwise → **INITIAL + FINAL** (but both on coordinator, no exchange between them)

---

## 3. How External Source Fragments Flow Through the Mapper

### Step-by-Step: `FROM s3://... | WHERE x > 5 | STATS count(*) BY y`

**Logical plan** (bottom-up):
```
Aggregate[count(*), y]
  Filter[x > 5]
    ExternalRelation[s3://bucket/path]
```

**Mapper walk** (recursive top-down, physical plan built bottom-up):

1. **`mapInner(Aggregate)`** → it's a UnaryPlan → `mapUnary(Aggregate)`
2. Inside `mapUnary`, first: `mappedChild = mapInner(Filter)` → it's a UnaryPlan → `mapUnary(Filter)`
3. Inside `mapUnary(Filter)`, first: `mappedChild = mapInner(ExternalRelation)` → it's a LeafPlan → `mapLeaf(ExternalRelation)`
4. `mapLeaf` sees it's NOT an `EsRelation`, calls `MapperUtils.mapLeaf()` → returns `ExternalSourceExec`
5. Back in `mapUnary(Filter)`:
   - `mappedChild = ExternalSourceExec` → NOT a `FragmentExec` → skip the fragment-growing block
   - Filter is not a PipelineBreaker → reaches `MapperUtils.mapUnary()` → returns `FilterExec(ExternalSourceExec)`
6. Back in `mapUnary(Aggregate)`:
   - `mappedChild = FilterExec(ExternalSourceExec)` → NOT a `FragmentExec`
   - Line 121: `addExchangeForFragment(aggregate, mappedChild)` → child is not `FragmentExec` → returns unchanged
   - Line 124: `mappedChild instanceof ExchangeExec` → **false**
   - Line 128: Check for `NonEvaluatableGroupingFunction` in groupings → if none found:
   - Line 131: **Returns `AggregateExec(FilterExec(ExternalSourceExec), SINGLE)`**

**Result physical plan** (for normal grouping):
```
AggregateExec[SINGLE, count(*), y]
  FilterExec[x > 5]
    ExternalSourceExec[s3://bucket/path]
```

All on coordinator, single-phase, no distribution.

### What Happens with CATEGORIZE

If the query is `FROM s3://... | STATS count(*) BY CATEGORIZE(msg)`:

1. Same flow until line 128
2. `aggregate.groupings()` contains `CATEGORIZE(msg)` which is a `NonEvaluatableGroupingFunction`
3. Falls to `else` branch (line 132-133):
   - `mappedChild = AggregateExec(ExternalSourceExec, INITIAL)`
4. Line 137: `return AggregateExec(mappedChild, FINAL)`

**Result**:
```
AggregateExec[FINAL, count(*), CATEGORIZE(msg)]
  AggregateExec[INITIAL, count(*), CATEGORIZE(msg)]
    ExternalSourceExec[s3://bucket/path]
```

Two-phase, but both on coordinator, no exchange/distribution between them.

---

## 4. Where INITIAL vs FINAL Mode Is Set on Aggregation Operators

### 4a. The `MapperUtils.aggExec()` Factory Method

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java` (lines 201-224)

```java
static AggregateExec aggExec(Aggregate aggregate, PhysicalPlan child, AggregatorMode aggMode, List<Attribute> intermediateAttributes) {
    if (aggregate instanceof TimeSeriesAggregate ts) {
        return new TimeSeriesAggregateExec(/* ..., aggMode, ... */);
    } else {
        return new AggregateExec(/* ..., aggMode, ... */);
    }
}
```

This is the factory that creates `AggregateExec` with the specified mode. Called from:

| Caller | Mode | Line |
|--------|------|------|
| `Mapper.mapUnary()` | `SINGLE` | Line 131 |
| `Mapper.mapUnary()` | `INITIAL` | Line 133 |
| `Mapper.mapUnary()` | `FINAL` | Line 137 |
| `LocalMapper.mapUnary()` | `INITIAL` | Line 87 |

### 4b. The `Mapper.mapUnary()` Call Sites (Coordinator Mapper)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

1. **`AggregatorMode.SINGLE`** — Line 131: When no exchange was added AND no NonEvaluatableGroupingFunction in groupings
2. **`AggregatorMode.INITIAL`** — Line 133: When no exchange was added BUT NonEvaluatableGroupingFunction present (coordinator-only two-phase)
3. **`AggregatorMode.FINAL`** — Line 137: Always the outer/return aggregation (both distributed and coordinator-only two-phase cases)

### 4c. The `LocalMapper.mapUnary()` Call Site (Data Node Mapper)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/LocalMapper.java` (lines 85-87)

```java
if (unary instanceof Aggregate aggregate) {
    List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);
    return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
}
```

The `LocalMapper` ALWAYS sets `INITIAL` mode. This is because `LocalMapper` runs on data nodes where the `FragmentExec` logical plan is locally replanned into a physical plan. The data node always does the initial (partial) aggregation; the coordinator does the final reduction.

### 4d. Node-Level Reduction: `INTERMEDIATE` Mode

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java` (line 159)

```java
case AggregateExec aggExec -> getPhysicalPlanReduction(estimatedRowSize, aggExec.withMode(AggregatorMode.INTERMEDIATE));
```

When node-level reduction is enabled (reducing data from multiple shards on the same node before sending to coordinator), the `INITIAL` agg is replaced with `INTERMEDIATE` mode for the reduction step.

### 4e. The `AggregatorMode` Enum

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/AggregatorMode.java`

| Mode | Input | Output | Description |
|------|-------|--------|-------------|
| `INITIAL` | raw | partial | Maps raw inputs to intermediate outputs. Runs on data nodes. |
| `INTERMEDIATE` | partial | partial | Maps intermediate inputs to intermediate outputs. Node reduction. |
| `FINAL` | partial | final | Maps intermediate inputs to final outputs. Runs on coordinator. |
| `SINGLE` | raw | final | Maps raw inputs to final outputs. Coordinator-only, no distribution. |

Valid sequences:
- `SINGLE` (no distribution)
- `INITIAL` → `INTERMEDIATE`* → `FINAL` (distributed)

### 4f. Physical Operator Creation from AggregateExec

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/AbstractPhysicalOperationProviders.java` (line 64)

The `groupingPhysicalOperation()` method reads `aggregateExec.getMode()` (line 72) and passes it through to the operator factories:

- **Non-grouping**: `AggregationOperator.AggregationOperatorFactory(aggregatorFactories, aggregatorMode)` (line 95)
- **Grouping**: `HashAggregationOperatorFactory(groupSpecs, aggregatorMode, ...)` (line 182)

The mode flows straight from the plan node to the compute operator.

---

## 5. Summary: Why External Sources Get Single-Phase STATS

The chain of causation is:

1. `ExternalRelation` maps to `ExternalSourceExec` (not `FragmentExec`)
2. Streaming operators above it produce physical pipeline operators directly (not growing a fragment)
3. When `Aggregate` is reached, `addExchangeForFragment()` checks `child instanceof FragmentExec` → **false**
4. No exchange is inserted → no distribution boundary
5. Aggregation defaults to `SINGLE` mode (or `INITIAL`+`FINAL` without exchange for CATEGORIZE)
6. Everything runs on coordinator in one pass

### What Would Need to Change for Distributed Aggregation on External Sources

To enable two-phase distributed aggregation for external sources, the planner would need to recognize external source plans as distributable — either by:

**Option A: Recognize ExternalSourceExec as a distribution boundary** — Modify `addExchangeForFragment()` (or add a parallel method) to also check for `ExternalSourceExec` (or a more general interface like `DistributableExec`) and insert an exchange when found.

**Option B: Wrap ExternalSourceExec in something FragmentExec-like** — Create a wrapper that makes external sources look like fragments to the existing planner. This is more invasive.

**Option C: Create the two-phase split post-mapping** — After the Mapper produces the SINGLE-mode physical plan, a separate pass could detect `AggregateExec[SINGLE]` over `ExternalSourceExec` and rewrite it to `AggregateExec[FINAL] → Exchange → AggregateExec[INITIAL]`.

The current external distribution infrastructure (`SplitDiscoveryPhase`, `AdaptiveStrategy`, `DataNodeComputeHandler.startExternalComputeOnDataNodes`) already handles distributing `ExternalSourceExec` data across nodes — but it distributes the **full plan** (which includes the `SINGLE` agg). Each data node computes the complete aggregation on its subset of splits and sends final results. The coordinator then just concatenates them — **which produces incorrect results** for aggregations (duplicate groups, wrong counts).

The fix specifically requires the coordinator mapper to split the aggregation so data nodes send INITIAL (partial) intermediate state, and the coordinator does the FINAL merge.

---

## 6. Key File Index

| File | What's There | Key Lines |
|------|-------------|-----------|
| `.../planner/mapper/Mapper.java` | Two-phase split decision, `addExchangeForFragment()` gate | 115-138, 269-278 |
| `.../planner/mapper/LocalMapper.java` | Data node mapping (always INITIAL) | 85-87 |
| `.../planner/mapper/MapperUtils.java` | `aggExec()` factory, `mapLeaf()` for ExternalRelation | 75-77, 201-224 |
| `.../plan/logical/ExternalRelation.java` | `toPhysicalExec()` → `ExternalSourceExec` | 115-127 |
| `.../plan/physical/ExternalSourceExec.java` | Physical plan node for external sources (NOT FragmentExec) | 48 |
| `.../plan/physical/FragmentExec.java` | Wrapper for logical plan fragments sent to data nodes | 24 |
| `.../plan/physical/AggregateExec.java` | Physical agg node, carries `AggregatorMode` | 42, 143 |
| `.../compute/aggregation/AggregatorMode.java` | INITIAL/INTERMEDIATE/FINAL/SINGLE enum | 36-56 |
| `.../planner/AbstractPhysicalOperationProviders.java` | Creates compute operators from AggregateExec + mode | 64-197 |
| `.../planner/PlannerUtils.java` | `reductionPlan()` — node-level INTERMEDIATE mode | 141-182 |
| `.../plan/logical/PipelineBreaker.java` | Marker interface — Aggregate implements it | 17 |
| `.../plan/logical/Aggregate.java` | Implements PipelineBreaker, ExecutesOn.Coordinator | 47-53 |
| `.../expression/function/grouping/GroupingFunction.java` | NonEvaluatableGroupingFunction (CATEGORIZE) | 49-53 |
| `.../plugin/ComputeService.java` | `executePlan()` dispatch, external distribution | 443-558, 726-796 |
| `.../plugin/DataNodeComputeHandler.java` | `startExternalComputeOnDataNodes()`, split injection | 247-400, 792-879 |
| `.../datasources/SplitDiscoveryPhase.java` | Discovers splits for ExternalSourceExec | 35-123 |
