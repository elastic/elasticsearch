# ES|QL Special/Less-Common Commands with External Data Sources

Investigation date: 2026-03-03
Branch: `esql/connector-spi-v3` (main)

## Summary Table

| Command | Status | Failure Mode | Notes |
|---------|--------|-------------|-------|
| TS | BLOCKED | Hard check: `EsRelation` + `IndexMode.TIME_SERIES` required | Structurally impossible with external sources |
| METRICS_INFO | BLOCKED | Hard check: `EsRelation` + `IndexMode.TIME_SERIES` required | Same gate as TS |
| TS_INFO | BLOCKED | Hard check: `EsRelation` + `IndexMode.TIME_SERIES` required | Same gate as TS |
| SAMPLE | WORKS | - | Pure page-level random filter, no source coupling |
| CHANGE_POINT | WORKS | - | Coordinator-only surrogate plan, operates on pages |
| FORK | WORKS | - | Coordinator-only, maps sub-plans independently |
| RERANK | WORKS | - | Uses Inference API, source-agnostic |
| COMPLETION | WORKS | - | Uses Inference API, source-agnostic |
| MV_EXPAND | WORKS | - | Pure page-level operator, no source coupling |
| DISSECT | WORKS | - | Pure string pattern extraction on pages |
| GROK | WORKS | - | Pure regex extraction on pages |
| WHERE | WORKS | - | FilterOperator evaluates expressions on pages |
| EVAL | WORKS | - | EvalOperator evaluates expressions on pages |

---

## 1. TS Command (Time Series)

**Status: BLOCKED -- structurally incompatible with external sources**

### How it works

The `TS` command is parsed as `visitTimeSeriesCommand` which calls `visitRelation(source, SourceCommand.TS, ...)`, creating an `UnresolvedRelation` with `SourceCommand.TS`.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java:776-778`
```java
public LogicalPlan visitTimeSeriesCommand(EsqlBaseParser.TimeSeriesCommandContext ctx) {
    return visitRelation(source(ctx), SourceCommand.TS, ctx.indexPatternAndMetadataFields());
}
```

When a STATS command follows a TS source, the parser creates a `TimeSeriesAggregate` instead of a regular `Aggregate`:

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java:513-524`
```java
boolean hasTimeSeries = input.anyMatch(p -> p instanceof UnresolvedRelation ur && ur.indexMode() == IndexMode.TIME_SERIES);
// ...
if (hasAggregate == false && hasPromqlCommand == false && hasTimeSeries && hasInfoCommand == false) {
    return new TimeSeriesAggregate(source(ctx), input, stats.groupings, stats.aggregates, null, new UnresolvedTimestamp(source(ctx)));
}
```

### Why it fails with external sources

**Gate 1 -- Parser level:** The TS command creates an `UnresolvedRelation` that resolves to `EsRelation` (never `ExternalRelation`). External sources use `EXTERNAL "uri"` syntax via `visitExternalCommand`, which creates `UnresolvedExternalRelation` -> `ExternalRelation`. These are completely separate code paths.

**Gate 2 -- Optimizer level:** The `TranslateTimeSeriesAggregate` rule explicitly searches for `EsRelation` to find the `_tsid` attribute:

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/TranslateTimeSeriesAggregate.java:167-170`
```java
aggregate.forEachDown(EsRelation.class, r -> {
    for (Attribute attr : r.output()) {
        if (attr.name().equals(MetadataAttribute.TSID_FIELD)) {
            tsid.set(attr);
```

And transforms the `EsRelation` to add `_tsid` and change index mode:

**File:** `TranslateTimeSeriesAggregate.java:300-306`
```java
LogicalPlan newChild = aggregate.child().transformUp(EsRelation.class, r -> {
    IndexMode indexMode = requiredTimeSeriesSource.get() ? r.indexMode() : IndexMode.STANDARD;
    if (r.output().contains(tsid.get()) == false) {
        return r.withIndexMode(indexMode).withAttributes(CollectionUtils.combine(r.output(), tsid.get()));
    } else {
        return r.withIndexMode(indexMode);
    }
});
```

`ExternalRelation` has no `indexMode()`, no `_tsid`, no `withIndexMode()` -- this would silently produce no result (the `forEachDown(EsRelation.class, ...)` would find nothing).

**Conclusion:** TS is fundamentally an Elasticsearch time-series index concept. Supporting it for external sources would require either: (a) external sources declaring themselves as time-series with `_tsid` metadata, or (b) a completely different time-series aggregation path. Neither is practical for the MVP.

---

## 2. METRICS_INFO Command

**Status: BLOCKED -- requires `EsRelation` with `IndexMode.TIME_SERIES`**

### How it works

`MetricsInfo` is a plan node that returns metadata about metric fields (name, data_stream, unit, metric_type, field_type, dimension_fields).

### Why it fails

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/MetricsInfo.java:126-129`
```java
public void postAnalysisVerification(Failures failures) {
    boolean hasTsSource = child().anyMatch(p -> p instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES);
    if (hasTsSource == false) {
        failures.add(fail(this, "METRICS_INFO can only be used with TS source command"));
    }
```

Hard `instanceof EsRelation` check. `ExternalRelation` will never match.

**Failure mode:** Verification error: "METRICS_INFO can only be used with TS source command"

---

## 3. TS_INFO Command

**Status: BLOCKED -- requires `EsRelation` with `IndexMode.TIME_SERIES`**

### How it works

`TsInfo` extends `MetricsInfo` with an additional `dimensions` column.

### Why it fails

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/TsInfo.java:126-129`
```java
public void postAnalysisVerification(Failures failures) {
    boolean hasTsSource = child().anyMatch(p -> p instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES);
    if (hasTsSource == false) {
        failures.add(fail(this, "TS_INFO can only be used with TS source command"));
    }
```

Identical `instanceof EsRelation` gate.

**Failure mode:** Verification error: "TS_INFO can only be used with TS source command"

---

## 4. SAMPLE Command

**Status: WORKS with external sources**

### How it works

`Sample` is a simple UnaryPlan that holds a probability expression. It maps to `SampleExec` (physical), which creates a `SampleOperator.Factory`.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Sample.java:21`
```java
public class Sample extends UnaryPlan implements TelemetryAware {
```

No `PostAnalysisVerificationAware`, no `EsRelation` checks, no `IndexMode` checks.

**Execution path:**

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/LocalExecutionPlanner.java:1404-1408`
```java
private PhysicalOperation planSample(SampleExec rsx, LocalExecutionPlannerContext context) {
    PhysicalOperation source = plan(rsx.child(), context);
    var probability = (double) Foldables.valueOf(context.foldCtx(), rsx.probability());
    return source.with(new SampleOperator.Factory(probability), source.layout);
}
```

Pure page-level operator: takes a probability, randomly keeps/drops rows from each page. No Lucene or ES dependencies whatsoever.

**Conclusion:** Works perfectly with external sources. The `SampleOperator` just filters rows from the upstream source based on random probability.

---

## 5. CHANGE_POINT Command

**Status: WORKS with external sources**

### How it works

`ChangePoint` implements `SurrogateLogicalPlan` and `ExecutesOn.Coordinator`. It uses ML's `ChangePointDetector` to detect change points in a numeric time series.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ChangePoint.java:46-53`
```java
public class ChangePoint extends UnaryPlan
    implements SurrogateLogicalPlan, PostAnalysisVerificationAware, SortPreserving, LicenseAware, ExecutesOn.Coordinator {
```

### Surrogate plan

**File:** `ChangePoint.java:143-154`
```java
public LogicalPlan surrogate() {
    OrderBy orderBy = new OrderBy(source(), child(), List.of(order()));
    Limit limit = new Limit(source(), new Literal(Source.EMPTY, ChangePointOperator.INPUT_VALUE_COUNT_LIMIT + 1, DataType.INTEGER), orderBy);
    ChangePoint changePoint = new ChangePoint(source(), limit, value, key, targetType, targetPvalue);
    return new Limit(source(), new Literal(Source.EMPTY, ChangePointOperator.INPUT_VALUE_COUNT_LIMIT, DataType.INTEGER), changePoint);
}
```

The surrogate expands to: `child -> SORT key -> LIMIT 1001 -> CHANGE_POINT -> LIMIT 1000`.

### Verification

**File:** `ChangePoint.java:157-167`
```java
public void postAnalysisVerification(Failures failures) {
    Order order = order();
    if (DataType.isSortable(order.dataType()) == false) {
        failures.add(fail(this, "change point key [" + key.name() + "] must be sortable"));
    }
    if (value.dataType().isNumeric() == false) {
        failures.add(fail(this, "change point value [" + value.name() + "] must be numeric"));
    }
}
```

Only checks data types (sortable key, numeric value). No `EsRelation` checks.

### Execution

**File:** `LocalExecutionPlanner.java:1398-1401`
```java
private PhysicalOperation planChangePoint(ChangePointExec changePoint, LocalExecutionPlannerContext context) {
    PhysicalOperation source = plan(changePoint.child(), context);
    Layout layout = source.layout.builder().append(changePoint.targetType()).append(changePoint.targetPvalue()).build();
    return source.with(new ChangePointOperator.Factory(layout.get(changePoint.value().id()).channel(), changePoint.source()), layout);
}
```

Pure page-level operator. The `ChangePointOperator` calls into ML's `ChangePointDetector`, which is a pure in-memory computation.

**Dependency:** Requires ML plugin (license check: `MachineLearning.CHANGE_POINT_AGG_FEATURE.check(state)`). This is a plugin/license dependency, not a source-type dependency.

**Conclusion:** Works with external sources. Data flows: external source pages -> sort -> limit -> change point detection.

---

## 6. FORK Command

**Status: WORKS with external sources**

### How it works

`Fork` is an n-ary plan that runs multiple sub-plans (branches) and merges their results. It implements `ExecutesOn.Coordinator`.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Fork.java:39`
```java
public class Fork extends LogicalPlan implements PostAnalysisPlanVerificationAware, TelemetryAware, ExecutesOn.Coordinator {
```

### Physical mapping

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java:247-251`
```java
private PhysicalPlan mapFork(Fork fork) {
    if (fork instanceof UnionAll unionAll) {
        return mapUnionAll(unionAll);
    }
    return new MergeExec(fork.source(), fork.children().stream().map(this::mapInner).toList(), fork.output());
}
```

Each branch is independently mapped via `mapInner`. The branches can contain any valid plan, including ones with `ExternalRelation`.

### Verification

The `postAnalysisPlanVerification` only checks:
1. No nested FORKs
2. Column data type consistency across branches

No source-type checks.

**Conclusion:** Works with external sources. Each FORK branch is an independent sub-plan that can include external sources.

---

## 7. RERANK Command

**Status: WORKS with external sources**

### How it works

`Rerank` extends `InferencePlan` and uses the Inference API to re-rank results. It implements `ExecutesOn.Coordinator`.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/inference/Rerank.java:42`
```java
public class Rerank extends InferencePlan<Rerank> implements PostAnalysisVerificationAware, TelemetryAware {
```

### Execution

**File:** `LocalExecutionPlanner.java:718-748`
```java
private PhysicalOperation planRerank(RerankExec rerank, LocalExecutionPlannerContext context) {
    PhysicalOperation source = plan(rerank.child(), context);
    // ... evaluates rerank fields, creates RerankOperator with inference service
    return source.with(new RerankOperator.Factory(inferenceService, inferenceId, queryText, ...));
}
```

The `RerankOperator` calls an external inference service (via ES Inference API). The input comes from Pages -- it evaluates `rerankFields` expressions on the page data, sends text to the inference endpoint, and adds a score column.

### Verification

Only checks:
- `queryText` must be a string constant
- `rerankFields` must be string types

No source-type checks.

**Dependency:** Requires Inference API (`inferenceService`). This is an ES service dependency, not a source-type dependency. The inference service is available regardless of where the data came from.

**Conclusion:** Works with external sources. Reads pages from upstream, sends text to inference endpoint, adds score.

---

## 8. COMPLETION Command

**Status: WORKS with external sources**

### How it works

`Completion` extends `InferencePlan` and uses the Inference API for LLM completions. It implements `ExecutesOn.Coordinator`.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/inference/Completion.java:39`
```java
public class Completion extends InferencePlan<Completion> implements TelemetryAware, PostAnalysisVerificationAware {
```

### Verification

Only checks:
- `prompt` must be a string type

No source-type checks.

**Conclusion:** Works with external sources. Same pattern as RERANK -- takes page data, calls inference API, adds output column.

---

## 9. MV_EXPAND

**Status: WORKS with external sources**

### How it works

`MvExpand` is a UnaryPlan that expands multi-valued fields into separate rows.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/MvExpand.java:26`
```java
public class MvExpand extends UnaryPlan implements TelemetryAware, SortAgnostic {
```

### Execution

**File:** `LocalExecutionPlanner.java:1389-1395`
```java
Layout.Builder layout = source.layout.builder();
layout.replace(mvExpandExec.target().id(), mvExpandExec.expanded().id());
return source.with(
    new MvExpandOperator.Factory(source.layout.get(mvExpandExec.target().id()).channel(), blockSize),
    layout.build()
);
```

Pure page-level operator. Takes a column channel number, expands multi-valued blocks into separate rows. No Lucene or ES dependencies.

**Note:** External sources like Parquet can produce multi-valued fields (e.g., repeated/list types). The `MvExpandOperator` will work correctly on those.

**Conclusion:** Works with external sources.

---

## 10. DISSECT

**Status: WORKS with external sources**

### How it works

`Dissect` extends `RegexExtract` and uses `DissectParser` to extract structured fields from text.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Dissect.java:29`
```java
public class Dissect extends RegexExtract implements TelemetryAware, SortPreserving {
```

### Execution

**File:** `LocalExecutionPlanner.java:637-661` (planDissect)
```java
private PhysicalOperation planDissect(DissectExec dissect, LocalExecutionPlannerContext context) {
    PhysicalOperation source = plan(dissect.child(), context);
    // Creates StringExtractOperator with DissectParser-based evaluator
}
```

Pure string extraction. `DissectParser` is from `org.elasticsearch.dissect` -- a standalone library that parses strings according to a pattern. Zero Lucene/ES dependencies.

**Conclusion:** Works with external sources. Extremely useful for parsing log lines from external files (e.g., NDJSON logs from S3).

---

## 11. GROK

**Status: WORKS with external sources**

### How it works

`Grok` extends `RegexExtract` and uses the Grok pattern library for regex-based field extraction.

**File:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Grok.java:35`
```java
public class Grok extends RegexExtract implements TelemetryAware, SortPreserving {
```

### Execution

Uses `org.elasticsearch.grok.Grok` -- a standalone regex library (no Lucene dependency). Creates a `ColumnExtractOperator` with a `GrokEvaluatorExtracter`.

Pure string regex extraction on pages.

**Conclusion:** Works with external sources. Like DISSECT, extremely valuable for parsing unstructured log data from external sources.

---

## 12. WHERE (Filter)

**Status: WORKS with external sources**

### How it works

`Filter` is mapped to `FilterExec`, which creates a `FilterOperatorFactory` wrapping an expression evaluator.

**File:** `LocalExecutionPlanner.java:1354-1360`
```java
private PhysicalOperation planFilter(FilterExec filter, LocalExecutionPlannerContext context) {
    PhysicalOperation source = plan(filter.child(), context);
    PhysicalOperation filterOperation = source.with(
        new FilterOperatorFactory(EvalMapper.toEvaluator(context.foldCtx(), filter.condition(), source.layout, context.shardContexts)),
        source.layout
    );
```

The `FilterOperatorFactory` takes a boolean expression evaluator and drops rows where the condition is false. This is a pure page-level operation.

### Scoring caveat

**File:** `LocalExecutionPlanner.java:1362-1363`
```java
if (context.shardContexts.isEmpty() == false && PlannerUtils.usesScoring(filter)) {
    // Add scorer operator to add the filter expression scores to the overall scores
```

The `ScoreOperator` is only added when `shardContexts` is non-empty (data nodes with Lucene shards). For external sources running on the coordinator, `shardContexts` is empty, so no scoring is attempted. This means:
- Regular WHERE with external sources: **works**
- WHERE with `_score` metadata on external sources: **not applicable** (external sources don't have `_score`)

### Filter pushdown

External sources also support L2 filter pushdown. The `ExternalSourceExec` has a `pushedFilter` field that carries pushed-down filter expressions to the source operator:

**File:** `LocalExecutionPlanner.java:1270`
```java
.pushedFilter(externalSource.pushedFilter())
```

**Conclusion:** Works with external sources. Simple boolean filtering on pages. No Lucene dependency for non-scoring filters.

---

## 13. EVAL

**Status: WORKS with external sources**

### How it works

`Eval` is mapped to `EvalExec`, which creates `EvalOperator.EvalOperatorFactory` instances for each expression.

**File:** `LocalExecutionPlanner.java:625-635`
```java
private PhysicalOperation planEval(EvalExec eval, LocalExecutionPlannerContext context) {
    PhysicalOperation source = plan(eval.child(), context);
    for (Alias field : eval.fields()) {
        var evaluatorSupplier = EvalMapper.toEvaluator(context.foldCtx(), field.child(), source.layout, context.shardContexts);
        Layout.Builder layout = source.layout.builder();
        layout.append(field.toAttribute());
        source = source.with(new EvalOperatorFactory(evaluatorSupplier), layout.build());
    }
    return source;
}
```

Pure expression evaluation on pages. Each `EvalOperator` reads input columns from the page, evaluates the expression, and appends the result as a new column. No Lucene or ES dependencies.

**Conclusion:** Works with external sources. All scalar functions (math, string, date, conditional, etc.) work on pages regardless of where the data came from.

---

## Architecture Pattern Summary

The commands divide into two categories:

### Source-Coupled Commands (blocked)
- **TS**, **METRICS_INFO**, **TS_INFO**: These are intrinsically tied to Elasticsearch's time-series index mode. They require:
  - `EsRelation` with `IndexMode.TIME_SERIES`
  - Special metadata attributes (`_tsid`, `@timestamp`)
  - Elasticsearch-specific metric field types

### Page-Level Operators (work)
Everything else operates on `Page` objects produced by the source:
- **SAMPLE**, **CHANGE_POINT**, **MV_EXPAND**: Pure transformations on pages
- **DISSECT**, **GROK**: String extraction on pages
- **WHERE**, **EVAL**: Expression evaluation on pages
- **FORK**: Coordinator-level branching of sub-plans
- **RERANK**, **COMPLETION**: Take page data and call inference API

The key architectural insight: once data enters the compute engine as `Page` objects (which external sources produce via `SourceOperator`), all downstream processing commands work identically regardless of data origin. The only commands that break are those that require Elasticsearch-specific index metadata or capabilities.
