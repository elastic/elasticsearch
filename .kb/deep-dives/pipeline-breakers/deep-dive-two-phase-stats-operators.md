# Deep Dive: Two-Phase STATS Aggregation at the Operator Level

## 1. AggregatorMode Enum

**File**: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/AggregatorMode.java`

Four modes exist, each defined by two boolean flags (`inputPartial`, `outputPartial`):

| Mode           | inputPartial | outputPartial | Description                              |
|----------------|-------------|---------------|------------------------------------------|
| `INITIAL`      | false       | true          | Maps raw inputs to intermediate outputs  |
| `INTERMEDIATE` | true        | true          | Maps intermediate inputs to intermediate outputs |
| `FINAL`        | true        | false         | Maps intermediate inputs to final outputs |
| `SINGLE`       | false       | false         | Maps raw inputs to final outputs (single-pass) |

**Valid sequences** (from Javadoc, line 29-34):
- `SINGLE` alone
- `INITIAL` -> `INTERMEDIATE`* -> `FINAL`

The `INTERMEDIATE` mode is used for **node-level reduction** — when multiple threads on a data node each produce INITIAL results, they merge into a single INTERMEDIATE result per node before sending to the coordinator.

---

## 2. How Operators Use Modes

### 2a. Non-Grouping: `AggregationOperator`

**File**: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/AggregationOperator.java`

This operator handles aggregations without GROUP BY (e.g., `STATS count(*)`). It delegates all mode logic to `Aggregator`.

**File**: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/Aggregator.java`

The `Aggregator` class wraps an `AggregatorFunction` + `AggregatorMode` and uses the mode to switch behavior:

**`processPage()` (line 36-45)** — input dispatch:
```java
if (mode.isInputPartial()) {
    // INTERMEDIATE or FINAL mode: input is pre-aggregated intermediate state
    aggregatorFunction.addIntermediateInput(page);
} else {
    // INITIAL or SINGLE mode: input is raw data
    aggregatorFunction.addRawInput(page, mask);
}
```

**`evaluate()` (line 47-53)** — output dispatch:
```java
if (mode.isOutputPartial()) {
    // INITIAL or INTERMEDIATE mode: emit intermediate state
    aggregatorFunction.evaluateIntermediate(blocks, offset, driverContext);
} else {
    // FINAL or SINGLE mode: emit final scalar result
    aggregatorFunction.evaluateFinal(blocks, offset, driverContext);
}
```

**`evaluateBlockCount()` (line 32-34)** — how many blocks each agg produces:
- `outputPartial` true: `aggregatorFunction.intermediateBlockCount()` (multiple blocks for partial state)
- `outputPartial` false: `1` (single scalar result)

### 2b. Grouping: `HashAggregationOperator`

**File**: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/HashAggregationOperator.java`

This operator handles `STATS ... BY group`. It uses `BlockHash` for group-key management and `GroupingAggregator` for per-group accumulation.

**File**: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/GroupingAggregator.java`

`GroupingAggregator` uses mode identically to `Aggregator`:

**`prepareProcessPage()` (line 37-61)** — input dispatch:
- `isInputPartial()` true (INTERMEDIATE/FINAL): calls `aggregatorFunction.addIntermediateInput(positionOffset, groupIds, page)`
- `isInputPartial()` false (INITIAL/SINGLE): calls `aggregatorFunction.prepareProcessRawInputPage(seenGroupIds, page)`

**`evaluate()` (line 68-74)** — output dispatch:
- `isOutputPartial()` true (INITIAL/INTERMEDIATE): calls `evaluateIntermediate(blocks, offset, selected)`
- `isOutputPartial()` false (FINAL/SINGLE): calls `evaluateFinal(blocks, offset, selected, evaluationContext)`

**Early partial emit** (HashAggregationOperator line 314-326):
The `shouldEmitPartialResultsPeriodically()` method only fires when `aggregatorMode.isOutputPartial()` is true (INITIAL or INTERMEDIATE). This is the "streaming partial emit" that prevents memory blowup when there are many groups — it periodically flushes and resets.

### 2c. AggregatorFunction Interface

**File**: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/AggregatorFunction.java`

Defines the four core operations that every aggregation must implement:
- `addRawInput(Page, BooleanVector mask)` — consume raw data
- `addIntermediateInput(Page)` — consume pre-aggregated state
- `evaluateIntermediate(Block[], int, DriverContext)` — emit intermediate state
- `evaluateFinal(Block[], int, DriverContext)` — emit final scalar

The intermediate state is opaque per-function. For example, `SumDoubleAggregatorFunction` intermediate state includes `(value, delta, seen)` (3 blocks); `CountAggregatorFunction` intermediate state is `(count, seen)` (2 blocks).

### Summary: INITIAL vs FINAL Execution

| Aspect              | INITIAL                           | FINAL                              |
|---------------------|-----------------------------------|------------------------------------|
| Input               | Raw data pages                    | Intermediate state pages           |
| Processing method   | `addRawInput()`                   | `addIntermediateInput()`           |
| Output              | Intermediate state                | Final scalar values                |
| Output block count  | Multiple (intermediate state)     | 1 per aggregation                  |
| Partial emit        | Yes (when outputPartial=true)     | No                                 |
| Where it runs       | Data node threads (parallel)      | Coordinator (single)               |

---

## 3. How Mode Flows from Physical Plan to Operator Factory

### 3a. Mapper: Logical -> Physical (mode assignment)

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

In `mapUnary()` (line 115-138), when the logical plan is `Aggregate`:

1. `addExchangeForFragment()` is called on the child (line 121). If the child is a `FragmentExec` (ES index source), this wraps it in an `ExchangeExec`, signaling distributed execution.

2. **If an exchange was added** (child became `ExchangeExec`):
   - The exchange output is set to intermediate attributes (line 125)
   - The FINAL `AggregateExec` wraps it (line 137): `AggregateExec(FINAL) -> ExchangeExec -> FragmentExec`
   - Inside the FragmentExec, the `LocalMapper` (line 87) creates: `AggregateExec(INITIAL) -> EsSourceExec`

3. **If no exchange** (child is NOT FragmentExec — e.g., external source, local data):
   - If no non-evaluatable grouping functions: `AggregateExec(SINGLE)` (line 131)
   - Otherwise: `AggregateExec(INITIAL) -> child`, then `AggregateExec(FINAL)` wraps it (line 133-137)

### 3b. LocalMapper: Fragment -> Physical on data nodes

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/LocalMapper.java`

On data nodes, `mapUnary()` (line 85-88) always creates `AggregatorMode.INITIAL`:
```java
if (unary instanceof Aggregate aggregate) {
    List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);
    return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
}
```

### 3c. PlannerUtils.reductionPlan: Node-level reduction

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java` (line 159)

When computing the "reduction plan" for node-level merge, the `AggregateExec` mode is changed to `INTERMEDIATE`:
```java
case AggregateExec aggExec -> getPhysicalPlanReduction(estimatedRowSize, aggExec.withMode(AggregatorMode.INTERMEDIATE));
```

This creates the middle step: INITIAL (per-thread) -> INTERMEDIATE (per-node merge) -> FINAL (coordinator).

### 3d. AbstractPhysicalOperationProviders: Physical plan -> Operator factory

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/AbstractPhysicalOperationProviders.java`

In `groupingPhysicalOperation()` (line 64-197):

The mode from `aggregateExec.getMode()` (line 72) flows to:

1. **Non-grouping path** (line 77-96):
   - `aggregatesToFactory()` receives the mode (line 84-92)
   - Creates `Aggregator.Factory` via `supplier.aggregatorFactory(s.mode, s.channels)` (line 90)
   - Wraps in `AggregationOperator.AggregationOperatorFactory(aggregatorFactories, aggregatorMode)` (line 95)

2. **Grouping path** (line 97-191):
   - `aggregatesToFactory()` receives the mode (line 162-170)
   - Creates `GroupingAggregator.Factory` via `supplier.groupingAggregatorFactory(s.mode, s.channels)` (line 168)
   - Wraps in `HashAggregationOperatorFactory(groups, aggregatorMode, aggregatorFactories, ...)` (line 182-190)

The mode also controls layout computation (line 104, 135, 149) and filter application (line 318: filters only apply when `mode.isInputPartial() == false`).

### 3e. AggregatorFunctionSupplier: Factory creation

**File**: `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/AggregatorFunctionSupplier.java`

The `aggregatorFactory()` method (line 27-39) creates an `Aggregator.Factory` that closes over both the mode and input channels:
```java
default Aggregator.Factory aggregatorFactory(AggregatorMode mode, List<Integer> channels) {
    return new Aggregator.Factory() {
        @Override
        public Aggregator apply(DriverContext driverContext) {
            return new Aggregator(aggregator(driverContext, channels), mode);
        }
    };
}
```

Similarly `groupingAggregatorFactory()` (line 41-53) for grouped aggregations.

The mode is baked into the factory and then into each `Aggregator`/`GroupingAggregator` instance at runtime.

### Complete data flow:

```
Mapper.mapUnary(Aggregate)
  -> sets AggregatorMode on AggregateExec physical plan node
    -> LocalExecutionPlanner.planAggregation(AggregateExec)
      -> AbstractPhysicalOperationProviders.groupingPhysicalOperation(aggregateExec)
        -> aggregateExec.getMode() extracts the mode
        -> aggregatesToFactory() passes mode to each AggregatorFunctionSupplier
          -> supplier.aggregatorFactory(mode, channels) / supplier.groupingAggregatorFactory(mode, channels)
            -> Aggregator(function, mode) / GroupingAggregator(function, mode)
              -> mode.isInputPartial() / mode.isOutputPartial() control runtime behavior
```

---

## 4. External Sources Get SINGLE Mode (Coordinator-Only Aggregation)

### Proof

**ExternalRelation** implements `ExecutesOn.Coordinator` (line 46 in `ExternalRelation.java`):
```java
public class ExternalRelation extends LeafPlan implements ExecutesOn.Coordinator {
```

**Mapper.mapLeaf()** (line 86-93 in `Mapper.java`):
When the Mapper encounters an `ExternalRelation`, it calls `MapperUtils.mapLeaf()` which calls `external.toPhysicalExec()`, returning an `ExternalSourceExec` (NOT a `FragmentExec`).

**Mapper.mapUnary()** critical path (lines 98-137):
1. `mappedChild = mapInner(unary.child())` — for external sources, this returns `ExternalSourceExec` (not `FragmentExec`)
2. The `if (mappedChild instanceof FragmentExec)` check (line 98) is FALSE — so the fragment path is skipped
3. Execution reaches the Aggregate handler (line 115)
4. `addExchangeForFragment(aggregate, mappedChild)` is called (line 121), but since `mappedChild` is NOT a `FragmentExec`, this method returns `mappedChild` unchanged (line 269-278):
   ```java
   private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
       if (child instanceof FragmentExec) {  // FALSE for external sources
           child = new FragmentExec(logical);
           child = new ExchangeExec(child.source(), child);
       }
       return child;  // returned unchanged
   }
   ```
5. Since `mappedChild` is NOT an `ExchangeExec` (line 124 is FALSE), and assuming no non-evaluatable grouping functions (line 128-130), the code creates:
   ```java
   return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.SINGLE, intermediate);
   ```

**Result**: For external sources, the physical plan is:
```
AggregateExec(mode=SINGLE, child=ExternalSourceExec)
```

All data flows through a single `AggregationOperator(SINGLE)` or `HashAggregationOperator(SINGLE)` on the **coordinator node**. There is:
- No INITIAL phase on data nodes
- No INTERMEDIATE node-level reduction
- No FINAL phase
- No exchange/distribution

### The exception: NonEvaluatableGroupingFunction

If the groupings contain `NonEvaluatableGroupingFunction` (currently only `Categorize`), the external source path falls through to lines 133-137:
```java
mappedChild = MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
// then falls through to:
return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.FINAL, intermediate);
```
This creates `AggregateExec(FINAL) -> AggregateExec(INITIAL) -> ExternalSourceExec` — still all on the coordinator, but in two phases locally. This is needed because CATEGORIZE cannot run in SINGLE mode.

### Performance implication

For `FROM external | STATS avg(price) BY category`:
- **ES index**: `AggregateExec(FINAL)` on coordinator <- `Exchange` <- `AggregateExec(INITIAL)` on N data nodes (parallel)
- **External source**: `AggregateExec(SINGLE)` on coordinator <- `ExternalSourceExec` on coordinator (serial)

All rows from the external source stream through a single operator pipeline on the coordinator. For large datasets this means:
1. No parallelism — single thread processes all data
2. No node-level reduction — all intermediate state must fit in coordinator memory
3. No early partial emit benefit — SINGLE mode has `isOutputPartial()=false`, so `shouldEmitPartialResultsPeriodically()` always returns false

### What distributed aggregation for external sources would require

To get two-phase aggregation for external sources (GA-8 in the roadmap):
1. External sources need to produce `FragmentExec` (or equivalent) so the Mapper wraps them in an `ExchangeExec`
2. OR: A new mapping path that explicitly creates INITIAL/FINAL split for distributed external source execution
3. The `SplitProvider` mechanism already exists for file-based sources — the splits would need to map to parallel INITIAL aggregation tasks
4. The `DataNodeComputeHandler` already supports running `AggregateExec(INITIAL)` on data nodes — the gap is getting external source operators dispatched there

### Key file paths summary

| Component | File | Key lines |
|-----------|------|-----------|
| AggregatorMode enum | `compute/src/main/java/.../aggregation/AggregatorMode.java` | 36-73 |
| Aggregator (non-grouping, mode dispatch) | `compute/src/main/java/.../aggregation/Aggregator.java` | 36-53 |
| GroupingAggregator (grouping, mode dispatch) | `compute/src/main/java/.../aggregation/GroupingAggregator.java` | 37-74 |
| AggregatorFunction interface | `compute/src/main/java/.../aggregation/AggregatorFunction.java` | 20-52 |
| AggregatorFunctionSupplier (factory+mode binding) | `compute/src/main/java/.../aggregation/AggregatorFunctionSupplier.java` | 27-53 |
| AggregationOperator (non-grouping) | `compute/src/main/java/.../operator/AggregationOperator.java` | 41-352 |
| HashAggregationOperator (grouping) | `compute/src/main/java/.../operator/HashAggregationOperator.java` | 41-595 |
| Mapper (mode assignment, exchange decision) | `esql/src/main/java/.../planner/mapper/Mapper.java` | 115-138, 269-278 |
| LocalMapper (data node INITIAL) | `esql/src/main/java/.../planner/mapper/LocalMapper.java` | 85-88 |
| MapperUtils (aggExec factory, external leaf) | `esql/src/main/java/.../planner/mapper/MapperUtils.java` | 68-84, 201-224 |
| PlannerUtils (INTERMEDIATE reduction) | `esql/src/main/java/.../planner/PlannerUtils.java` | 141-182, 159 |
| AbstractPhysicalOperationProviders (plan->operator) | `esql/src/main/java/.../planner/AbstractPhysicalOperationProviders.java` | 64-197, 275-336 |
| AggregateExec (physical plan node) | `esql/src/main/java/.../plan/physical/AggregateExec.java` | 27-213 |
| ExternalRelation (ExecutesOn.Coordinator) | `esql/src/main/java/.../plan/logical/ExternalRelation.java` | 46 |
| Aggregate (ExecutesOn.Coordinator, PipelineBreaker) | `esql/src/main/java/.../plan/logical/Aggregate.java` | 47-53 |
