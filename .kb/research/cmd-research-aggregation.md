# Aggregation Commands with External Data Sources in ES|QL

## 1. STATS Command: Aggregation Mode for External Sources

### How ExternalRelation maps to physical plan

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

`ExternalRelation` (logical) maps to `ExternalSourceExec` (physical) via `MapperUtils.mapLeaf()` at **line 92**. This bypasses `FragmentExec` entirely -- `EsRelation` produces `FragmentExec` (line 87), but `ExternalRelation` produces `ExternalSourceExec` directly (via `MapperUtils.mapLeaf()` line 75-77 in MapperUtils.java).

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java`

```java
// Line 73-77
// External data sources (Iceberg, Parquet, etc.)
// These are executed on the coordinator only, bypassing FragmentExec/ExchangeExec dispatch
if (p instanceof ExternalRelation external) {
    return external.toPhysicalExec();
}
```

### How AggregateExec mode is determined

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`, **lines 115-138**

The `mapUnary` method handles `Aggregate` as a pipeline breaker (line 115). The critical flow for external sources:

1. The child of `Aggregate` is already mapped to physical plan. For external sources, this is `ExternalSourceExec` (not `FragmentExec`).
2. `addExchangeForFragment` is called at **line 121** -- but since the child is NOT a `FragmentExec`, **it returns the child unchanged** (line 273: `if (child instanceof FragmentExec)` is false for `ExternalSourceExec`).
3. Since `addExchangeForFragment` does NOT add an exchange, `mappedChild` is still `ExternalSourceExec` (not `ExchangeExec`).
4. The code enters the `else if` branch at **line 128-134**:
   - If no groupings use `NonEvaluatableGroupingFunction` (i.e., `CATEGORIZE`), the aggregation gets **`AggregatorMode.SINGLE`** (line 131).
   - If `CATEGORIZE` is used, it falls through to INITIAL mode at line 133, then FINAL at line 137.

**Result: External sources get SINGLE-pass aggregation by default**, which is correct since all data flows through one coordinator pipeline. The two-phase split (INITIAL on data nodes + FINAL on coordinator) only happens when `FragmentExec` is present (i.e., for ES indices).

### addExchangeForFragment behavior

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`, **lines 269-278**

```java
private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
    // in case of fragment, preserve the streaming operator (order-by, limit or topN) for local replanning
    // no need to do it for an aggregate since it gets split
    // and clone it as a physical node along with the exchange
    if (child instanceof FragmentExec) {
        child = new FragmentExec(logical);
        child = new ExchangeExec(child.source(), child);
    }
    return child;
}
```

This method **only acts on FragmentExec children**. For `ExternalSourceExec`, it's a no-op. This means external sources never get an `ExchangeExec` injected by the mapper for aggregation.

### Exception: Distributed External Sources

When external sources have splits and the distribution strategy decides to distribute (ComputeService line 532), the plan DOES get an `ExchangeExec`. In this path:
- `breakPlanBetweenCoordinatorAndDataNode` (PlannerUtils line 116-128) splits at the `ExchangeExec` boundary.
- The coordinator gets the FINAL aggregation, data nodes get INITIAL.
- But this exchange is **not added by the mapper** -- it's added by the distribution strategy when the plan already has exchanges from the mapper (for the external source exchange that wraps `ExternalSourceExec`).

Looking more carefully at the distributed path in ComputeService:
- **Line 457-458**: `discoverSplits()` finds splits for external sources, then `applyExternalDistributionStrategy()` decides whether to distribute.
- **Line 299-306**: `collapseExternalSourceExchanges()` removes exchanges wrapping `ExternalSourceExec` when NOT distributing:
```java
static PhysicalPlan collapseExternalSourceExchanges(PhysicalPlan plan) {
    return plan.transformUp(ExchangeExec.class, exchange -> {
        if (exchange.child() instanceof ExternalSourceExec) {
            return exchange.child();
        }
        return exchange;
    });
}
```

This confirms: for non-distributed external sources, exchanges are collapsed, and the aggregation runs in SINGLE mode entirely on the coordinator.

### ComputeService execution path for external sources

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`

**Lines 490-529**: When `dataNodePlan == null` (no ExchangeExec in the plan, which happens for coordinator-only external sources after exchange collapse), the plan executes entirely on the coordinator:
- Creates a `ComputeContext` with `EmptyIndexedByShardId.instance()` (no shards)
- Calls `runCompute()` which creates local drivers

**Lines 531-551**: When distributed and no ES indices, calls `executeExternalDistribution()` which:
- Sets up exchange source/sink between coordinator and data nodes
- Coordinator runs the FINAL plan (above the exchange)
- Data nodes run the INITIAL plan (below the exchange, with assigned splits)

**Lines 914-936**: The `runCompute` method detects external sources at line 914:
```java
boolean hasExternalSource = plan.anyMatch(p -> p instanceof ExternalSourceExec);
```
When true, it uses the external-source variant of `PlannerUtils.localPlan()` (line 916-926) which passes a `FilterPushdownRegistry` instead of `SearchExecutionContext` list.


## 2. INLINESTATS Command

### Plan Structure

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/InlineStats.java`

`InlineStats` is a `SurrogateLogicalPlan` -- it replaces itself with an `InlineJoin` during analysis (line 138-143):

```java
@Override
public LogicalPlan surrogate() {
    // left join between the main relation and the local, lookup relation
    Source source = source();
    LogicalPlan left = aggregate.child();
    return new InlineJoin(source, left, InlineJoin.stubSource(aggregate, left), joinConfig());
}
```

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/join/InlineJoin.java`

`InlineJoin` extends `Join` and uses `LEFT` join semantics. The right side is the aggregation (with its source replaced by a `StubRelation`).

### Compatibility with External Sources

INLINESTATS should work with external sources because:

1. The `InlineStats` node transforms into an `InlineJoin` during the surrogate substitution phase (analysis).
2. The `InlineJoin` is a `Join` which is handled by `Mapper.mapBinary()` (Mapper.java line 183-233).
3. For external sources, the left child will be `ExternalSourceExec` (not `FragmentExec`).
4. At line 200-202: `if (left instanceof FragmentExec) { return new FragmentExec(bp); }` -- this won't match for external sources.
5. The right child (the aggregation subquery) gets its own separate execution -- the `stubSource` is replaced during pre-analysis.

**However**, looking at Mapper.java lines 198-231, there's a potential issue:
- When the left child is NOT `FragmentExec`, the code maps the right child separately.
- If the right child is `LocalSourceExec` (pre-computed local data), it creates a `HashJoinExec` (line 207-215).
- If the right child is `FragmentExec` with index_mode=LOOKUP, it creates a `LookupJoinExec` (line 217-230).
- Otherwise, it falls through to `MapperUtils.unsupported(bp)` at line 232.

For INLINESTATS with an external source:
- The right side (aggregate) will have been pre-computed and replaced with a `LocalRelation` via `InlineJoin.inlineData()`.
- So the right child maps to `LocalSourceExec`, which matches the `HashJoinExec` path at line 207.
- **This should work correctly with external sources.**

### Verification restriction

The `postAnalysisPlanVerification()` at lines 170-200 only blocks INLINESTATS for time-series (`TS` command) without a prior STATS. It does NOT block INLINESTATS for external sources.


## 3. Aggregate Function Dependencies on Lucene/ES

### Summary: No direct Lucene dependencies in aggregate functions

All aggregate functions implement `ToAggregator.supplier()` which returns pure compute `AggregatorFunctionSupplier` implementations. None of the function suppliers reference `IndexSearcher`, `ShardContext`, or any Lucene API.

### Per-function analysis

| Function | Class | File | Lucene Dependency |
|----------|-------|------|-------------------|
| `count()` | `Count` | `.../aggregate/Count.java:146-151` | None. Returns `CountAggregatorFunction.supplier()` or `DenseVectorCountAggregatorFunction.supplier()` |
| `sum()` | `Sum extends NumericAggregate` | `.../aggregate/Sum.java` | None. Dispatches by type to `SumLong/Int/Double/LossySumDouble` suppliers |
| `avg()` | `Avg extends NumericAggregate` | `.../aggregate/Avg.java` | None. Same pattern as Sum |
| `min()` | `Min extends NumericAggregate` | `.../aggregate/Min.java` | None. Same pattern |
| `max()` | `Max extends NumericAggregate` | `.../aggregate/Max.java` | None. Same pattern |
| `values()` | `Values` | `.../aggregate/Values.java:172-179` | None. Type dispatch to supplier map |
| `top()` | `Top` | `.../aggregate/Top.java:365` | None. Type dispatch to supplier map |
| `percentile()` | `Percentile extends NumericAggregate` | `.../aggregate/Percentile.java` | None. Type dispatch |
| `median_absolute_deviation()` | `MedianAbsoluteDeviation extends NumericAggregate` | `.../aggregate/MedianAbsoluteDeviation.java` | None. Type dispatch |
| `st_centroid_agg()` | `SpatialCentroid` | `.../aggregate/SpatialCentroid.java:112-131` | **Indirect via FieldExtractPreference**. Uses `DOC_VALUES` vs `SourceValues` variants. Default is `NONE` (SourceValues), which is pure compute. The `DOC_VALUES` variant is only set by `SpatialDocValuesExtraction` rule, which only runs inside `FragmentExec` (data node local optimization). External sources always use `SourceValues` variant. |
| `st_extent_agg()` | `SpatialExtent` | `.../aggregate/SpatialExtent.java:111-134` | Same as `SpatialCentroid` -- default uses `SourceValues`, which is pure compute |

### Where Lucene *is* involved (but not in the aggregation function itself)

1. **Filter on aggregate** (e.g., `COUNT(*) WHERE condition`): The `aggregatesToFactory` method in `AbstractPhysicalOperationProviders.java` line 318-326 creates a `FilteredAggregatorFunctionSupplier` using `EvalMapper.toEvaluator()` with `context.shardContexts()`. For external sources, `shardContexts` is `EmptyIndexedByShardId.instance()` which is fine since ESQL expression filters (WHERE on aggregates) don't actually need Lucene -- they operate on compute engine blocks.

2. **SpatialDocValuesExtraction**: This local physical optimizer rule (`.../rules/physical/local/SpatialDocValuesExtraction.java`) changes `fieldExtractPreference` to `DOC_VALUES` for spatial aggregation fields when the data comes from Lucene indices with doc values. This rule only matches `AggregateExec` nodes above `FieldExtractExec` nodes above `EsQueryExec` -- it will never match external sources since they don't produce `EsQueryExec`.

3. **Categorize grouping function**: When `STATS ... BY CATEGORIZE(field)` is used, the mapper forces two-phase aggregation (INITIAL + FINAL) at Mapper.java lines 128-134, even without an exchange. This uses `AnalysisRegistry` (from ML's text categorization) via the `HashAggregationOperatorFactory` at `AbstractPhysicalOperationProviders.java` line 189. The `AnalysisRegistry` is not Lucene-specific per se, but it's a heavyweight dependency. **CATEGORIZE should still work with external sources** since the aggregation operators themselves don't require Lucene.


## 4. PushStatsToSource

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushStatsToSource.java`

### Does it handle external sources?

**No.** The rule explicitly matches only `AggregateExec` whose child is `EsQueryExec`:

```java
// Line 49
if (aggregateExec.child() instanceof EsQueryExec queryExec) {
```

For external sources, the child would be `ExternalSourceExec`, not `EsQueryExec`, so the rule is a no-op.

### What stats can be pushed?

Only `COUNT` (ungrouped) -- the rule creates an `EsStatsQueryExec` that uses Lucene's `docFreq` for fast counting. This is inherently Lucene-specific and cannot apply to external sources.

Specifically:
- Only works when `aggregate.groupings().isEmpty()` (no GROUP BY)
- Only works for exactly 1 aggregate function that is a `Count`
- For `COUNT(*)` (foldable field): uses `doc_count` with wildcard
- For `COUNT(field)`: uses `exists` query, only when field is single-valued
- Creates `EsStatsQueryExec` which directly uses Lucene index statistics

**Impact on external sources**: None. External sources always compute COUNT via the standard `CountAggregatorFunction` which processes rows one by one. This is slower for large datasets compared to Lucene's index statistics shortcut, but functionally correct.


## 5. PushTopNToSource

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushTopNToSource.java`

### Does it match ExternalSourceExec?

**No.** The rule explicitly matches only `TopNExec` whose child is `EsQueryExec`:

```java
// Line 144-148 (evaluatePushable method)
if (child instanceof EsQueryExec queryExec
    && queryExec.canPushSorts()
    && canPushDownOrders(topNExec.order(), lucenePushdownPredicates)
    && canPushLimit(topNExec, plannerSettings)
    && tooManyKeywordSortFields(topNExec.order(), maxKeywordSortFields) == false) {
```

And the compound case at line 152-153:
```java
if (child instanceof EvalExec evalExec
    && evalExec.child() instanceof EsQueryExec queryExec
```

Both cases require `EsQueryExec` as the source. `ExternalSourceExec` is never matched.

### What can be pushed?

- Sort on indexed field attributes (pushed as `EsQueryExec.FieldSort`)
- Sort on `_score` metadata (pushed as `EsQueryExec.ScoreSort`)
- Sort on `ST_DISTANCE` expressions (pushed as `EsQueryExec.GeoDistanceSort`)
- Subject to limit size constraint (`luceneTopNLimit` planner setting)
- Subject to max keyword sort fields constraint

All of these are Lucene-specific pushdowns (field sorts use Lucene's `IndexSearcher.search` with sort, geo distance uses Lucene's `LatLonDocValuesField.newDistanceSort`).

**Impact on external sources**: External sources always use the compute engine's `TopNOperator` for sorting and limiting, which processes rows in memory. This is functionally correct but doesn't benefit from Lucene's indexed sort optimization. For external sources, sort pushdown would need to be implemented via the source-specific SPI (e.g., Parquet row group ordering, or a connector-provided sort).


## Summary

| Aspect | Behavior with External Sources | Notes |
|--------|-------------------------------|-------|
| **STATS aggregation mode** | **SINGLE** (one-pass) | Correct for coordinator-only execution. Two-phase (INITIAL+FINAL) only when distributed. |
| **STATS BY CATEGORIZE** | **Two-phase** (INITIAL+FINAL) even without exchange | Special case in mapper lines 128-134. Works correctly. |
| **INLINESTATS** | **Works** via InlineJoin -> HashJoinExec | Right side pre-computed as LocalRelation, then hash-joined. |
| **Aggregate functions** | **All work** (no Lucene deps) | Pure compute operators. Spatial aggs use SourceValues variant (not DocValues). |
| **PushStatsToSource** | **Skipped** (not applicable) | Only matches EsQueryExec child. External sources compute COUNT via standard path. |
| **PushTopNToSource** | **Skipped** (not applicable) | Only matches EsQueryExec child. External sources use compute engine TopNOperator. |
| **Filter on aggregate** | **Works** | EvalMapper with EmptyIndexedByShardId handles expression-based filters fine. |
| **Distributed aggregation** | **Works** | When splits are distributed, exchange splits aggregation into INITIAL (data nodes) + FINAL (coordinator). |
