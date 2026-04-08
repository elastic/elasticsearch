# Processing Commands & Pushdown Optimizations for External Data Sources

Deep-dive research into how standard processing commands and pushdown optimizations work
with external data sources (`ExternalRelation` logical / `ExternalSourceExec` physical).

---

## 1. MapperUtils.java -- Plan Mapping for External Sources

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java`

### mapLeaf() -- Lines 68-85

Handles leaf plan nodes. External sources are handled at **line 75-77**:

```java
if (p instanceof ExternalRelation external) {
    return external.toPhysicalExec();   // creates ExternalSourceExec directly
}
```

This calls `ExternalRelation.toPhysicalExec()` (line 115 of ExternalRelation.java) which creates an
`ExternalSourceExec` with all source metadata, config, and attributes. Key difference from ES sources:
no `FragmentExec` wrapping, no data node dispatch.

### mapUnary() -- Lines 87-191

Maps every unary (single-child) logical plan node to its physical equivalent.
These are **all source-agnostic** -- they accept any `PhysicalPlan child`, so they work identically
whether the child is an ES source or an external source.

| Line | Logical Node        | Physical Node          | Command    | External Source OK? |
|------|---------------------|------------------------|------------|---------------------|
| 88   | `Filter`            | `FilterExec`           | WHERE      | YES                 |
| 92   | `Project`           | `ProjectExec`          | KEEP/DROP  | YES                 |
| 96   | `Eval`              | `EvalExec`             | EVAL       | YES                 |
| 100  | `Dissect`           | `DissectExec`          | DISSECT    | YES                 |
| 104  | `Grok`              | `GrokExec`             | GROK       | YES                 |
| 108  | `Rerank`            | `RerankExec`           | RERANK     | YES                 |
| 119  | `Completion`        | `CompletionExec`       | COMPLETION | YES                 |
| 130  | `Enrich`            | `EnrichExec`           | ENRICH     | YES                 |
| 144  | `MMR`               | `MMRExec`              | MMR        | YES                 |
| 148  | `MvExpand`          | `MvExpandExec`         | MV_EXPAND  | YES                 |
| 152  | `ChangePoint`       | `ChangePointExec`      | CHANGE_POINT | YES               |
| 163  | `FuseScoreEval`     | `FuseScoreEvalExec`    | (internal) | YES                 |
| 167  | `Sample`            | `SampleExec`           | SAMPLE     | YES                 |
| 171  | `Subquery`          | (passthrough to child) | (internal) | YES                 |
| 176  | `UriParts`          | `UriPartsExec`         | URI_PARTS  | YES                 |
| 186  | `RegisteredDomain`  | `RegisteredDomainExec` | REG_DOMAIN | YES                 |

**All 16 unary handlers are source-agnostic.** They receive the physical child (whether `ExternalSourceExec`
or anything else) and simply wrap it with the corresponding physical operator.

### Additional handlers in Mapper.java (not MapperUtils)

Pipeline breakers are handled in `Mapper.mapUnary()` (lines 95-181):

| Line | Logical Node        | Physical Node         | Command    | External Source OK? |
|------|---------------------|-----------------------|------------|---------------------|
| 115  | `Aggregate`         | `AggregateExec`       | STATS      | YES (see note)      |
| 140  | `Limit`             | `LimitExec`           | LIMIT      | YES                 |
| 145  | `TopN`              | `TopNExec`            | SORT+LIMIT | YES                 |
| 160  | `MetricsInfo`       | `MetricsInfoExec`     | (internal) | NO (ES-specific)    |
| 172  | `TsInfo`            | `TsInfoExec`          | (internal) | NO (ES-specific)    |

---

## 2. Mapper.java -- External vs ES Source Handling

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

### mapLeaf() -- Lines 85-93

```java
private PhysicalPlan mapLeaf(LeafPlan leaf) {
    if (leaf instanceof EsRelation esRelation) {
        return new FragmentExec(esRelation);   // ES: wrapped in FragmentExec for data node dispatch
    }
    return MapperUtils.mapLeaf(leaf);          // External: ExternalRelation -> ExternalSourceExec directly
}
```

**Key difference**:
- `EsRelation` -> `FragmentExec` (sent to data nodes via `ExchangeExec`)
- `ExternalRelation` -> `ExternalSourceExec` (runs on coordinator only)

### mapUnary() -- Lines 95-181

The critical branching logic is at **lines 98-109**:

```java
if (mappedChild instanceof FragmentExec) {
    // ES sources: streaming operators get folded INTO the fragment
    if (unary instanceof PipelineBreaker == false
        || (unary instanceof Limit limit && limit.local())
        || (unary instanceof TopN topN && topN.local())) {
        return new FragmentExec(unary);  // Absorb into fragment for data node execution
    }
}
```

For external sources, `mappedChild` is `ExternalSourceExec` (NOT `FragmentExec`), so this branch
is **never taken**. Instead, the code falls through to the pipeline breaker handlers or to
`MapperUtils.mapUnary()` at line 180.

**Consequence for pipeline breakers (Aggregate, Limit, TopN)**:

For ES sources with `FragmentExec`:
- `Aggregate` (line 115): Creates `FragmentExec` + `ExchangeExec` + INITIAL/FINAL split
- `Limit` (line 140): Creates `FragmentExec` + `ExchangeExec` + `LimitExec`
- `TopN` (line 145): Creates `FragmentExec` + `ExchangeExec` + `TopNExec`

For external sources (no `FragmentExec`):
- `Aggregate` (lines 128-131): Since child is NOT `ExchangeExec`, creates **SINGLE mode** aggregate
  (single-pass, no INITIAL/FINAL split)
- `Limit` (line 142): Creates `LimitExec` directly (no exchange)
- `TopN` (line 147): Creates `TopNExec` directly (no exchange)

This is the correct behavior for coordinator-only execution -- no need for distributed aggregation.

### addExchangeForFragment() -- Lines 269-278

This method only adds `ExchangeExec` when the child is `FragmentExec`. For external sources,
it returns the child unchanged.

---

## 3. Filter Pushdown (PushFiltersToSource.java)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java`

### External source path -- Lines 54-56

```java
} else if (filterExec.child() instanceof ExternalSourceExec externalExec) {
    plan = planFilterExecForExternalSource(filterExec, externalExec, ctx.filterPushdownRegistry());
}
```

This is a dedicated path for external sources, separate from the ES/Lucene path.

### planFilterExecForExternalSource() -- Lines 241-286

The method:
1. Looks up `FilterPushdownSupport` from the `FilterPushdownRegistry` by source type (line 247)
2. If no support registered, returns the filter unchanged (line 250)
3. Splits filter by AND (line 254)
4. Calls `pushdownSupport.pushFilters(filters)` (line 257) -- delegates to source-specific SPI
5. If pushdown succeeds, creates `ExternalSourceExec.withPushedFilter(combinedFilter)` (line 273)
6. If remainder exists, wraps in `FilterExec` (line 277); otherwise prunes `FilterExec` entirely (line 279)

### What filter types can be pushed?

The answer depends on the `FilterPushdownSupport` implementation registered for each source type.
The SPI interface (`FilterPushdownSupport.java`, lines 34-128) defines:
- `pushFilters(List<Expression>)` -- bulk push
- `canPush(Expression)` -- individual check returning `YES`/`NO`/`RECHECK`

The **framework** does not restrict which expression types can be pushed -- it delegates entirely
to the SPI. Each data source plugin (Parquet, Iceberg, etc.) decides what it can handle.

### Three paths in PushFiltersToSource.rule():

| Line | Child type          | Path                                   |
|------|---------------------|----------------------------------------|
| 50   | `EsQueryExec`       | Lucene pushdown (translatable check)   |
| 52   | `EvalExec` -> `EsQueryExec` | Lucene pushdown through eval aliases |
| 54   | `ExternalSourceExec`| SPI-based pushdown via FilterPushdownRegistry |

### Important detail: pushed filter is opaque

The pushed filter stored in `ExternalSourceExec.pushedFilter()` (line 63 of ExternalSourceExec.java)
is `Object` type. It is never serialized because external sources execute on coordinator only
(`ExecutesOn.Coordinator`). The filter is created during local physical optimization and consumed
by the operator factory in the same JVM.

---

## 4. Limit Pushdown (PushLimitToSource.java)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushLimitToSource.java`

### Lines 16-28

```java
public class PushLimitToSource extends PhysicalOptimizerRules.OptimizerRule<LimitExec> {
    @Override
    protected PhysicalPlan rule(LimitExec limitExec) {
        PhysicalPlan plan = limitExec;
        PhysicalPlan child = limitExec.child();
        if (child instanceof EsQueryExec queryExec) {
            plan = queryExec.withLimit(limitExec.limit());
        } else if (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
            plan = exchangeExec.replaceChild(queryExec.withLimit(limitExec.limit()));
        }
        return plan;
    }
}
```

**Does NOT handle ExternalSourceExec.** The rule only recognizes `EsQueryExec` (direct or through
`ExchangeExec`). For external sources, the `LimitExec` remains in the plan as a compute-layer operator.

**This is a gap**: LIMIT is not pushed to external sources. The external source reads all data
and `LimitOperator` truncates it in the compute pipeline. For large external datasets, this means
reading far more data than necessary.

---

## 5. Column Pruning / Projection Pushdown

### Logical: PruneColumns (operates on logical plan)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/PruneColumns.java`

Lines 80-92 show the switch statement for node types that get pruned:
- `Aggregate`, `InlineJoin`, `Eval`, `Project`, `EsRelation`, `Fork`, `RegexExtract`

**ExternalRelation is NOT in this switch.** It falls to the `default -> p` case (line 91), meaning
column pruning is NOT applied to `ExternalRelation` at the logical level.

However, `EsRelation` is only pruned for `LOOKUP` index mode (lines 197-210). For normal
ES sources, pruning at the logical level also does nothing -- the real column minimization
happens at the physical level via `InsertFieldExtraction`.

### Physical: ProjectAwayColumns (global optimizer)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/ProjectAwayColumns.java`

Lines 73-115: This rule adds explicit `Project` nodes inside `FragmentExec` to minimize data sent
from data nodes to coordinator. It only activates when it encounters `ExchangeExec` -> `FragmentExec`.

**Does NOT affect external sources** -- there is no `FragmentExec`/`ExchangeExec` for external sources.
Since external sources run on coordinator only, there is no cross-node data transfer to optimize.

### Physical: InsertFieldExtraction (local optimizer)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/InsertFieldExtraction.java`

Lines 48-49: Skips `LeafExec` nodes. Lines 60-61: Only processes children that export `_doc` attribute
(specific to `EsQueryExec`).

**Does NOT affect external sources** -- `ExternalSourceExec` has no `_doc` attribute. External source
operators produce all their columns directly without a `FieldExtractExec` intermediary.

### How KEEP, DROP, RENAME affect external sources:

1. **RENAME** (analyzed at line 1540 of Analyzer.java): Resolved into `Project` during analysis.
   Never reaches the mapper as `Rename`.

2. **KEEP** (Keep.java, line 18): Extends `Project`. Mapped by `MapperUtils.mapUnary()` line 92-93
   as `ProjectExec`. Works identically for all sources.

3. **DROP** (Drop.java, line 20): This is a `UnaryPlan` that is resolved/simplified during analysis
   and optimization into a `Project` node. The final plan has `Project` not `Drop`.

All three become `ProjectExec` nodes in the physical plan. `ProjectExec` simply selects/reorders
columns from its child's output. It is completely source-agnostic.

### Is column projection pushed down to external sources?

**No.** There is no rule that pushes column selection into `ExternalSourceExec`. The external source
reads ALL columns listed in its `attributes` list (set during metadata resolution), and `ProjectExec`
filters them downstream.

**This is a gap for efficiency**: If a query like `EXTERNAL "s3://..." | KEEP name, age` only needs
2 of 20 columns, the external source still reads all 20 columns from the file. Columnar formats
like Parquet could benefit significantly from column projection pushdown.

---

## 6. SORT Command

### How SORT works for external sources:

1. **Standalone SORT (without LIMIT)**: Not supported for ANY source. `OrderBy.postOptimizationPlanVerification()`
   (line 139-141 of OrderBy.java) rejects it with error: "Unbounded SORT not supported yet [...] please add a LIMIT"

2. **SORT + LIMIT = TopN**: The logical optimizer combines `Limit` + `OrderBy` into `TopN`
   (via `ReplaceLimitAndSortAsTopN`, line 18-25). `TopN` is then mapped to `TopNExec` by
   `Mapper.mapUnary()` at line 145-155.

3. **TopN pushdown to source**: `PushTopNToSource` (lines 65-287 of PushTopNToSource.java) only
   handles `EsQueryExec` children (lines 144, 153). **No TopN pushdown for external sources.**

4. **TopN execution for external sources**: `TopNExec` runs as a compute-layer operator. It
   materializes all rows from the source, sorts them in memory, and returns the top N.
   This works correctly but requires reading and sorting all data.

### Summary: SORT for external sources

- `SORT` alone: ERROR (same as ES sources -- general ESQL limitation)
- `SORT ... | LIMIT N`: Works via `TopNExec` in compute layer. No pushdown to source.
- No sort pushdown to external sources (no equivalent of Lucene's sorted field access).

---

## 7. LIMIT Command -- Backpressure Behavior

### Direct LIMIT (without SORT):

1. `Limit` logical node mapped to `LimitExec` by `Mapper.mapUnary()` line 140-142.
2. `LimitExec` is planned into `LimitOperator` in the compute layer.

### Backpressure mechanism:

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/LimitOperator.java`

The `LimitOperator` implements backpressure through the `Operator` interface:

- `needsInput()` (line 68-70): Returns `false` when `limiter.remaining() <= 0` -- stops requesting
  pages from the source operator.
- `isFinished()` (line 94-96): Returns `true` when limit is exhausted -- signals the Driver to stop.
- `addInput()` (line 73-86): Truncates the last page if it exceeds the remaining limit.

The `Driver` operates the operator chain sequentially:
- When `LimitOperator.isFinished()` returns true, the Driver finishes the entire pipeline.
- This means the source operator's `isFinished()` will be called, triggering cleanup.

**For external sources**: The source operator (created by the SPI's `operatorFactory()`) will
receive no more `needsInput()` calls once the limit is hit. However, the external source may
have already read ahead / buffered data. The level of "early termination" depends on the
source operator implementation:
- **Parquet**: Reads in row groups. May read one extra row group after limit.
- **NDJSON**: Streaming, so backpressure works well -- stops reading after limit.
- **CSV**: Similar to NDJSON, streaming.

LIMIT works via compute-layer backpressure for external sources. It does not push the limit
value to the source operator (no equivalent of `EsQueryExec.withLimit()`).

---

## 8. RENAME, KEEP, DROP -- Pure Plan Transformations

All three are resolved during analysis into `Project` nodes and work identically for all sources.

### RENAME
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Rename.java`
- Resolved during analysis (Analyzer.java line 1538-1540) into a `Project` with renamed aliases.
- Never reaches the mapper as a `Rename` node.
- **Works for external sources**: YES (becomes `ProjectExec`, source-agnostic)

### KEEP
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Keep.java`
- Extends `Project` (line 18)
- Mapped by `MapperUtils.mapUnary()` line 92-93 as `ProjectExec`
- **Works for external sources**: YES

### DROP
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Drop.java`
- Resolved during analysis into a `Project` (removals become retained-columns projection)
- **Works for external sources**: YES

---

## Summary: External Source Support Matrix

### Fully Working (source-agnostic compute operators):

| Command      | Mapping Location (MapperUtils.java)    | Notes                                       |
|--------------|----------------------------------------|---------------------------------------------|
| WHERE        | Line 88 (Filter -> FilterExec)         | Compute-layer evaluation + SPI filter pushdown |
| EVAL         | Line 96 (Eval -> EvalExec)             | Pure computation, no source dependency      |
| KEEP         | Line 92 (Project -> ProjectExec)       | Resolved from `Keep extends Project`        |
| DROP         | Line 92 (resolved to Project)          | Resolved during analysis                    |
| RENAME       | Line 92 (resolved to Project)          | Resolved during analysis to Project         |
| DISSECT      | Line 100 (Dissect -> DissectExec)      | String extraction, source-agnostic          |
| GROK         | Line 104 (Grok -> GrokExec)            | Pattern extraction, source-agnostic         |
| MV_EXPAND    | Line 148 (MvExpand -> MvExpandExec)    | Multi-value expansion, source-agnostic      |
| ENRICH       | Line 130 (Enrich -> EnrichExec)        | Joins against enrich index, source-agnostic |
| STATS        | Mapper.java line 115 (Aggregate)       | SINGLE mode for coordinator-only sources    |
| LIMIT        | Mapper.java line 140 (Limit->LimitExec)| Backpressure via LimitOperator              |
| SORT+LIMIT   | Mapper.java line 145 (TopN->TopNExec)  | In-memory sort, no Lucene pushdown          |
| CHANGE_POINT | Line 152 (ChangePoint->ChangePointExec)| Time-series analysis, source-agnostic       |
| SAMPLE       | Line 167 (Sample -> SampleExec)        | Probabilistic sampling, source-agnostic     |
| RERANK       | Line 108 (Rerank -> RerankExec)        | ML inference, source-agnostic               |
| COMPLETION   | Line 119 (Completion->CompletionExec)  | LLM completion, source-agnostic             |
| URI_PARTS    | Line 176 (UriParts -> UriPartsExec)    | URL parsing, source-agnostic                |
| REG_DOMAIN   | Line 186 (RegDomain -> RegDomainExec)  | Domain parsing, source-agnostic             |

### Pushdown Optimizations:

| Optimization           | ES Sources                | External Sources              | Gap?  |
|------------------------|---------------------------|-------------------------------|-------|
| Filter pushdown        | Lucene QueryBuilder       | SPI FilterPushdownSupport     | No    |
| Limit pushdown         | EsQueryExec.withLimit()   | NOT implemented               | YES   |
| TopN/Sort pushdown     | Lucene sorted fields      | NOT implemented               | YES   |
| Column projection      | InsertFieldExtraction     | NOT implemented               | YES   |
| Sample pushdown        | RandomSamplingQueryBuilder| NOT implemented               | YES   |
| Stats pushdown         | CountStarOptimization     | NOT implemented               | Minor |
| ProjectAwayColumns     | FragmentExec optimization | N/A (coordinator only)        | No    |

### Key Gaps Identified:

1. **LIMIT pushdown**: External sources read all data; `LimitOperator` truncates in compute layer.
   For `EXTERNAL "s3://huge.parquet" | LIMIT 10`, this reads the entire file.

2. **Column projection pushdown**: External sources read all columns even when only a few are needed.
   Parquet and ORC formats support efficient column projection.

3. **TopN/Sort pushdown**: External sources sort entirely in memory. No opportunity for
   source-level sorted access.

4. **Sample pushdown**: `PushSampleToSource` only handles `EsQueryExec`. External sources
   read all data and sample in compute layer.

5. **PruneColumns logical rule**: Does not prune `ExternalRelation` attributes. The full schema
   persists through the logical plan even when downstream only needs a subset.
