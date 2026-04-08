# Deep Dive: Item 23 -- Aggregate Pushdown for Connectors

## Executive Summary

No aggregate pushdown exists for any external data source today. A query like
`EXTERNAL "flight://db/orders" | STATS count(*), sum(amount) BY region` fetches
every row from the remote source over the wire, then aggregates inside the
ES|QL compute engine. For SQL-capable sources (future JDBC, or any connector
that can evaluate `SELECT COUNT(*), SUM(amount), region FROM orders GROUP BY
region`), this is wasteful by orders of magnitude.

This document traces the complete code path from the `STATS` command through
the logical plan, physical plan, optimizer rules, and finally the compute
operators, and identifies exactly where aggregate pushdown would plug in, what
SPI changes are needed, and what the optimizer rule would look like.

---

## 1. How STATS Is Planned Today

### 1.1 Logical Plan: `Aggregate`

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Aggregate.java`

The `Aggregate` logical plan node (line 47) extends `UnaryPlan` and carries two
key fields:

```java
protected final List<Expression> groupings;          // BY clause expressions
protected final List<? extends NamedExpression> aggregates; // aggregate functions + grouping keys
```

It implements `PipelineBreaker` and `ExecutesOn.Coordinator`, meaning it always
executes on the coordinator node and breaks the streaming pipeline (data must
be fully consumed before the aggregate can emit results).

### 1.2 Logical-to-Physical Mapping: `Mapper`

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

The `Mapper.mapUnary()` method (line 115) handles `Aggregate`:

```java
if (unary instanceof Aggregate aggregate) {
    List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);
    mappedChild = addExchangeForFragment(aggregate, mappedChild);

    if (mappedChild instanceof ExchangeExec exchange) {
        // Two-phase: INITIAL on data nodes, FINAL on coordinator
        mappedChild = new ExchangeExec(mappedChild.source(), intermediate, true, exchange.child());
    } else if (/* no GroupingFunction */) {
        // Single-pass on coordinator
        return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.SINGLE, intermediate);
    } else {
        mappedChild = MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
    }
    return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.FINAL, intermediate);
}
```

**For external sources:** `ExternalRelation.toPhysicalExec()` returns an
`ExternalSourceExec` (a `LeafExec`), NOT a `FragmentExec`. So
`addExchangeForFragment` is a no-op (the child is not a `FragmentExec`), and
the mapper hits the `else if` branch producing `AggregatorMode.SINGLE`. This
means:

> For external sources, `STATS` produces a single `AggregateExec` in SINGLE
> mode sitting directly above `ExternalSourceExec`.

The resulting physical plan tree:

```
AggregateExec [mode=SINGLE]
  ExternalSourceExec [flight://db/orders]
```

### 1.3 Physical Execution: `AggregateExec` to Operators

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/AbstractPhysicalOperationProviders.java`

`groupingPhysicalOperation()` (line 64) converts `AggregateExec` into either:
- `AggregationOperator` (no groupings) -- simple accumulation
- `HashAggregationOperator` (with groupings) -- hash-based group-by

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/HashAggregationOperator.java`

The `HashAggregationOperator` (line 41) consumes `Page` objects from the source
operator, hashes grouping keys into a `BlockHash`, and accumulates
`GroupingAggregator` state. On `finish()`, it emits the aggregated output.

**This is the code path that would be eliminated by aggregate pushdown** -- if
the source can compute the aggregate natively, none of the hashing or
accumulation needs to happen inside ES|QL.

---

## 2. Current Filter Pushdown Model (The Template)

### 2.1 FilterPushdownSupport SPI

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java`

```java
public interface FilterPushdownSupport {
    PushdownResult pushFilters(List<Expression> filters);

    record PushdownResult(Object pushedFilter, List<Expression> remainder) { ... }
}
```

The pattern:
1. Optimizer provides ESQL expressions
2. SPI implementation converts what it can to a source-specific opaque object
3. Returns remainder (what couldn't be pushed)
4. Opaque object stored on `ExternalSourceExec.pushedFilter`
5. At execution time, `SourceOperatorContext.pushedFilter()` passes it to the operator factory

### 2.2 FilterPushdownRegistry

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FilterPushdownRegistry.java`

Simple `Map<String, FilterPushdownSupport>` keyed by source type (e.g. "iceberg",
"parquet"). Populated by `DataSourceModule` from plugin registrations.

### 2.3 PushFiltersToSource -- The Optimizer Rule

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java`

```java
public class PushFiltersToSource extends
    PhysicalOptimizerRules.ParameterizedOptimizerRule<FilterExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
        // ...
        if (filterExec.child() instanceof ExternalSourceExec externalExec) {
            plan = planFilterExecForExternalSource(filterExec, externalExec, ctx.filterPushdownRegistry());
        }
        return plan;
    }
}
```

The rule:
1. Matches `FilterExec` whose child is `ExternalSourceExec`
2. Looks up `FilterPushdownSupport` from registry by source type
3. Calls `pushFilters()` with the filter expressions
4. Creates new `ExternalSourceExec` with the opaque pushed filter via `withPushedFilter()`
5. If remainder exists, keeps `FilterExec` on top; otherwise removes it

### 2.4 ExternalSourceExec Fields

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`

Current fields:
```java
private final String sourcePath;
private final String sourceType;
private final List<Attribute> attributes;
private final Map<String, Object> config;
private final Map<String, Object> sourceMetadata;
private final Object pushedFilter;         // Opaque -- NOT serialized (coordinator only)
private final Integer estimatedRowSize;
private final FileSet fileSet;             // NOT serialized
private final List<ExternalSplit> splits;
```

Key design: the `pushedFilter` is an **opaque Object** that only the
source-specific operator factory interprets. Since `ExternalSourceExec`
implements `ExecutesOn.Coordinator`, it is never serialized across nodes, so
the opaque object can be any JVM object.

**There is no field for pushed aggregates today.**

---

## 3. Existing Aggregate Pushdown to Lucene (PushStatsToSource)

### 3.1 PushStatsToSource

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushStatsToSource.java`

This rule pushes simple `COUNT(*)` aggregations into Lucene:

```java
public class PushStatsToSource extends
    PhysicalOptimizerRules.ParameterizedOptimizerRule<AggregateExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext context) {
        if (aggregateExec.child() instanceof EsQueryExec queryExec) {
            // ... checks for pushable stats (currently only COUNT)
            plan = new EsStatsQueryExec(...);  // Replaces AggregateExec entirely
        }
        return plan;
    }
}
```

This rule:
1. Matches `AggregateExec` whose child is `EsQueryExec`
2. Checks if the aggregate is a simple COUNT (no groupings, single agg)
3. Replaces the entire `AggregateExec` + `EsQueryExec` with `EsStatsQueryExec`

**Limitation:** Only supports COUNT, no groupings, single aggregate.

### 3.2 PushCountQueryAndTagsToSource

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushCountQueryAndTagsToSource.java`

More sophisticated: pushes `COUNT(*) BY round_to_field` into Lucene via
`EsStatsQueryExec.ByStat`. Requires query and tags to be present.

### 3.3 Key Insight

Both Lucene pushdown rules **replace the AggregateExec entirely** with a
specialized leaf node (`EsStatsQueryExec`). For external sources, we would
follow a different strategy: keep the `ExternalSourceExec` as-is, but add
pushed aggregate info to it (parallel to how `pushedFilter` works), and change
the plan tree so the `AggregateExec` is either removed or replaced with a
passthrough.

---

## 4. Connector SPI and QueryRequest

### 4.1 QueryRequest Today

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/QueryRequest.java`

```java
public record QueryRequest(
    String target,
    List<String> projectedColumns,
    List<Attribute> attributes,
    Map<String, Object> config,
    int batchSize,
    BlockFactory blockFactory
) { }
```

**What's missing:** There is no aggregate information, no filter information,
no GROUP BY information. The `QueryRequest` is purely a column-projection
request today. A SQL connector receiving this can only do
`SELECT col1, col2, ... FROM target` -- it cannot push down
`WHERE`, `GROUP BY`, or aggregate functions.

### 4.2 Connector Interface

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/Connector.java`

```java
public interface Connector extends Closeable {
    ResultCursor execute(QueryRequest request, Split split);
}
```

### 4.3 OperatorFactoryRegistry -- How Connectors Get Invoked

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/OperatorFactoryRegistry.java`

At line 62-81, for `ConnectorFactory` instances:
```java
if (sf instanceof ConnectorFactory cf) {
    Connector connector = cf.open(context.config());
    // Build QueryRequest from context
    QueryRequest request = new QueryRequest(target, projectedColumns, context.attributes(),
        context.config(), context.batchSize(), null);
    return new AsyncConnectorSourceOperatorFactory(connector, request, ...);
}
```

The `QueryRequest` is built from `SourceOperatorContext`. Note that:
- `pushedFilter` from context is **not passed to QueryRequest** today
- No aggregate info exists on either `SourceOperatorContext` or `QueryRequest`

---

## 5. Proposed Design: Aggregate Pushdown for External Sources

### 5.1 Overview

The design mirrors the existing filter pushdown pattern:

1. **SPI interface** (`AggregatePushdownSupport`) for sources to declare which aggregates they can handle
2. **Opaque pushed-aggregate object** stored on `ExternalSourceExec`
3. **Optimizer rule** (`PushAggregatesToSource`) that matches `AggregateExec` over `ExternalSourceExec`
4. **SPI on QueryRequest** to carry aggregate info to connectors
5. **Output schema change**: when aggregates are pushed, `ExternalSourceExec.attributes` changes to the aggregate output schema

### 5.2 SPI: `AggregatePushdownSupport`

New interface in `datasources/spi/`:

```java
/**
 * SPI for aggregate pushdown. Sources implement this to indicate which
 * aggregate expressions they can evaluate natively.
 */
public interface AggregatePushdownSupport {

    /**
     * Attempt to push aggregates to the source.
     *
     * @param aggregates the aggregate expressions (COUNT, SUM, etc.)
     * @param groupings  the GROUP BY expressions
     * @return result with opaque pushed-aggregate object and remainder
     */
    AggregatePushdownResult pushAggregates(
        List<? extends NamedExpression> aggregates,
        List<Expression> groupings
    );

    record AggregatePushdownResult(
        Object pushedAggregate,              // opaque, source-specific
        List<? extends NamedExpression> remainder,  // aggregates that couldn't be pushed
        List<Attribute> outputAttributes     // schema of the source's aggregate output
    ) {
        public boolean isFullyPushed() {
            return remainder == null || remainder.isEmpty();
        }
    }
}
```

**Why opaque:** A SQL connector would produce a SQL string fragment like
`"SELECT COUNT(*), SUM(amount), region FROM orders GROUP BY region"`. A Flight
connector might produce a FlightSQL command. An Iceberg connector would produce
Iceberg `Expression` objects. The framework doesn't need to understand the
representation.

### 5.3 ExternalSourceExec Changes

Add a new field parallel to `pushedFilter`:

```java
private final Object pushedAggregate;  // Opaque -- NOT serialized (coordinator only)
```

Add a `withPushedAggregate(Object newAggregate, List<Attribute> newAttributes)` method
that returns a new `ExternalSourceExec` with the aggregate info and updated output
attributes (since the source now returns aggregate results, not raw rows).

### 5.4 ExternalSourceFactory Changes

Add to the base interface:

```java
public interface ExternalSourceFactory {
    // ... existing methods ...

    default AggregatePushdownSupport aggregatePushdownSupport() {
        return null;  // no aggregate pushdown by default
    }
}
```

And a corresponding `AggregatePushdownRegistry` (parallel to `FilterPushdownRegistry`):

```java
public class AggregatePushdownRegistry {
    private final Map<String, AggregatePushdownSupport> pushdownSupport;
    public AggregatePushdownSupport get(String sourceType) { ... }
}
```

### 5.5 QueryRequest Changes

Add aggregate info to `QueryRequest`:

```java
public record QueryRequest(
    String target,
    List<String> projectedColumns,
    List<Attribute> attributes,
    Map<String, Object> config,
    int batchSize,
    BlockFactory blockFactory,
    Object pushedFilter,       // NEW: opaque filter for connector use
    Object pushedAggregate     // NEW: opaque aggregate for connector use
) { }
```

The connector's `execute()` implementation inspects `pushedAggregate` and, if
non-null, generates the appropriate native query (SQL `GROUP BY`, FlightSQL
command, etc.).

### 5.6 LocalPhysicalOptimizerContext Changes

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/LocalPhysicalOptimizerContext.java`

Add `aggregatePushdownRegistry`:

```java
public record LocalPhysicalOptimizerContext(
    PlannerSettings plannerSettings,
    EsqlFlags flags,
    Configuration configuration,
    FoldContext foldCtx,
    SearchStats searchStats,
    FilterPushdownRegistry filterPushdownRegistry,
    AggregatePushdownRegistry aggregatePushdownRegistry  // NEW
) { }
```

### 5.7 Optimizer Rule: `PushAggregatesToSource`

New rule in `optimizer/rules/physical/local/`:

```java
/**
 * Pushes aggregate expressions to external data sources that support
 * native aggregation (e.g. SQL databases, FlightSQL endpoints).
 *
 * Transforms:
 *   AggregateExec [mode=SINGLE, groupings=[region], aggs=[count(*), sum(amount), region]]
 *     ExternalSourceExec [flight://db/orders]
 *
 * Into:
 *   ExternalSourceExec [flight://db/orders, pushedAggregate=...,
 *                       attributes=[count(*):long, sum(amount):double, region:keyword]]
 *
 * When only some aggregates can be pushed (partial pushdown), the rule keeps
 * AggregateExec on top for the remainder. This is more complex and may not
 * be needed in the first version.
 */
public class PushAggregatesToSource extends
    PhysicalOptimizerRules.ParameterizedOptimizerRule<AggregateExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        // Only handle SINGLE mode (coordinator-only execution for external sources)
        if (aggregateExec.getMode() != AggregatorMode.SINGLE) {
            return aggregateExec;
        }

        if (aggregateExec.child() instanceof ExternalSourceExec externalExec == false) {
            return aggregateExec;
        }

        AggregatePushdownSupport support = ctx.aggregatePushdownRegistry().get(externalExec.sourceType());
        if (support == null) {
            return aggregateExec;
        }

        AggregatePushdownResult result = support.pushAggregates(
            aggregateExec.aggregates(),
            aggregateExec.groupings()
        );

        if (result.isFullyPushed()) {
            // Replace the entire AggregateExec + ExternalSourceExec with a
            // new ExternalSourceExec that produces aggregate output directly
            return externalExec.withPushedAggregate(
                result.pushedAggregate(),
                result.outputAttributes()
            );
        }

        // Partial pushdown: not implemented in V1
        // Would require splitting the AggregateExec into pushed and non-pushed parts
        return aggregateExec;
    }
}
```

**Registration in LocalPhysicalPlanOptimizer** (line 69-106):

The rule would be added to the "Push to ES" batch alongside `PushStatsToSource`
and `PushFiltersToSource`. Since it matches `AggregateExec` (not `FilterExec`),
it won't conflict with existing rules:

```java
esSourceRules.add(new PushAggregatesToSource());  // NEW
```

### 5.8 How It Plugs Into the Pipeline

Current flow for `EXTERNAL "flight://db/orders" | STATS count(*) BY region`:

```
Parser
  -> Aggregate(groupings=[region], aggs=[count(*), region])
       ExternalRelation("flight://db/orders")

Mapper
  -> AggregateExec(mode=SINGLE, groupings=[region], aggs=[count(*), region])
       ExternalSourceExec("flight://db/orders", attrs=[id, amount, region, ...])

LocalPhysicalOptimizer (today: no rule matches)
  -> AggregateExec(mode=SINGLE)
       ExternalSourceExec(...)

Execution: Source reads ALL rows -> HashAggregationOperator groups and counts
```

With aggregate pushdown:

```
LocalPhysicalOptimizer (PushAggregatesToSource matches)
  -> ExternalSourceExec("flight://db/orders",
       pushedAggregate=<SQL: SELECT COUNT(*), region FROM orders GROUP BY region>,
       attrs=[count(*):long, region:keyword])

Execution: Source sends native aggregate query, gets pre-aggregated results
           No HashAggregationOperator needed.
```

---

## 6. SQL Databases as Primary Beneficiary

### 6.1 SQL Generation from Pushed Aggregate

For a SQL connector, the `AggregatePushdownSupport` implementation would:

1. Walk the `aggregates` list, matching ESQL aggregate functions to SQL equivalents:
   - `Count(*)` -> `COUNT(*)`
   - `Count(field)` -> `COUNT(field)`
   - `Sum(field)` -> `SUM(field)`
   - `Avg(field)` -> `AVG(field)`
   - `Min(field)` -> `MIN(field)`
   - `Max(field)` -> `MAX(field)`
   - `CountDistinct(field)` -> `COUNT(DISTINCT field)` (if supported)

2. Walk the `groupings` list, mapping ESQL field references to SQL column names

3. Produce an opaque `SqlAggregate` object:

```java
record SqlAggregate(
    String selectClause,     // "COUNT(*), SUM(amount), region"
    String groupByClause,    // "region"
    List<Attribute> output   // typed output attributes
) { }
```

4. At execution time, the SQL connector's `execute()` method checks for
   `pushedAggregate instanceof SqlAggregate` and generates:
   `SELECT COUNT(*), SUM(amount), region FROM orders GROUP BY region`

### 6.2 Pushable vs Non-Pushable Functions

SQL-safe aggregate functions (ANSI SQL standard):
- COUNT, SUM, AVG, MIN, MAX -- universally supported
- COUNT DISTINCT -- widely supported

NOT pushable to SQL:
- `PERCENTILE(field, pct)` -- no SQL standard equivalent
- `MEDIAN(field)` -- no universal SQL syntax
- `VALUES(field)` -- no SQL equivalent
- `TOP(field, n)` -- SQL varies (MySQL `GROUP_CONCAT` vs PostgreSQL `array_agg`)
- `CATEGORIZE` -- ES|QL-specific grouping function
- Any aggregate with a `WHERE` filter clause (e.g. `COUNT(*) WHERE x > 5`)
- Any aggregate with a window interval

### 6.3 Aggregate Functions in ES|QL

**Directory:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/aggregate/`

Key aggregate functions (all extend `AggregateFunction`):
- `Count` (line 47) -- `COUNT(*)` or `COUNT(field)`
- `Sum` (line 51) -- `SUM(field)`, extends `NumericAggregate`
- `Avg` (line 35) -- `AVG(field)`, extends `AggregateFunction`
- `Min` -- `MIN(field)`
- `Max` -- `MAX(field)`
- `CountDistinct` -- `COUNT(DISTINCT field)`
- `Median` -- median (surrogate for percentile at 50%)
- `Percentile` -- percentile at arbitrary point
- `Values` -- collects all values
- `Top` -- top N values
- `StdDev`, `Variance`, `WeightedAvg`

Each `AggregateFunction` has:
- `field()` -- the expression being aggregated
- `filter()` -- optional WHERE filter
- `window()` -- optional time window
- `parameters()` -- extra params (e.g. percentile value)

For pushdown, we'd need to inspect these fields to determine SQL compatibility.

---

## 7. What Changes If the Source Already Computed the Aggregate

### 7.1 Plan Tree Shape

**Before pushdown:**
```
AggregateExec [mode=SINGLE, groupings=[region], aggs=[count(*), sum(amount), region]]
  ExternalSourceExec [attrs=[id, amount, region, date, ...]]
```

**After full pushdown:**
```
ExternalSourceExec [pushedAggregate=..., attrs=[count_star:long, sum_amount:double, region:keyword]]
```

The `AggregateExec` is completely removed. The `ExternalSourceExec`'s output
attributes change to match the aggregate result schema.

### 7.2 Output Attribute Matching

Critical: the output attributes of the pushed `ExternalSourceExec` must have
the same `NameId`s as the original `AggregateExec.output()`. This is because
downstream operators (Project, Eval, etc.) reference attributes by `NameId`.

The `AggregatePushdownResult.outputAttributes()` must be the exact attributes
from `AggregateExec.output()` (which comes from `Aggregate.output(aggregates)`).
The SPI implementation can use the same `Attribute` objects from the aggregate
list.

### 7.3 Partial Pushdown (Future)

If only some aggregates are pushable (e.g. `STATS count(*), percentile(x, 95) BY region`
where `percentile` is not SQL-pushable):

**Option A: Don't push at all** (simplest, V1 approach)

**Option B: Source computes pushable aggregates, ESQL computes the rest**
- Source returns: `count_star, region` (pre-aggregated)
- ES|QL still needs raw `x` values per region for `percentile`
- This requires the source to return BOTH aggregated and raw data, which is
  impractical. So partial pushdown really means: push only if ALL aggregates
  are pushable.

**Option C: Two passes** -- first pass pushes pushable aggregates, second pass
reads raw data for non-pushable ones, then join results. Very complex, not
worth it.

**Recommendation: V1 = all-or-nothing. Only push if every aggregate in the
STATS command is supported by the source.**

---

## 8. Filter + Aggregate Pushdown Interaction

When both filter and aggregate pushdown are active:

```
EXTERNAL "jdbc://..." | WHERE date > '2024-01-01' | STATS sum(amount) BY region
```

The physical plan before optimization:
```
AggregateExec [mode=SINGLE]
  FilterExec [date > '2024-01-01']
    ExternalSourceExec [...]
```

After `PushFiltersToSource` runs:
```
AggregateExec [mode=SINGLE]
  ExternalSourceExec [..., pushedFilter=<SQL: date > '2024-01-01'>]
```

After `PushAggregatesToSource` runs:
```
ExternalSourceExec [...,
  pushedFilter=<SQL: date > '2024-01-01'>,
  pushedAggregate=<SQL: SELECT SUM(amount), region ... GROUP BY region>]
```

At execution time, the SQL connector combines these into:
```sql
SELECT SUM(amount), region FROM orders WHERE date > '2024-01-01' GROUP BY region
```

**Order matters:** `PushFiltersToSource` should run before `PushAggregatesToSource`
in the optimizer batch, which is the natural order since filters sit between
the aggregate and the source in the plan tree. The filter pushdown removes the
`FilterExec`, leaving `AggregateExec` directly over `ExternalSourceExec`, which
is exactly the pattern that `PushAggregatesToSource` matches.

---

## 9. Implementation Plan

### Phase 1: Core SPI (1.5 weeks H+AI)
1. Define `AggregatePushdownSupport` interface in `datasources/spi/`
2. Define `AggregatePushdownRegistry` in `datasources/`
3. Add `pushedAggregate` field to `ExternalSourceExec`
4. Add `pushedAggregate` to `SourceOperatorContext`
5. Add `pushedFilter` and `pushedAggregate` to `QueryRequest`
6. Add `aggregatePushdownRegistry` to `LocalPhysicalOptimizerContext`

### Phase 2: Optimizer Rule (1 week H+AI)
1. Implement `PushAggregatesToSource` rule
2. Register in `LocalPhysicalPlanOptimizer.rules()`
3. Unit tests with mock `AggregatePushdownSupport`

### Phase 3: SQL Connector Implementation (2 weeks H+AI)
1. Implement `SqlAggregatePushdownSupport` for a SQL connector
2. SQL generation from ESQL aggregate expressions
3. Integration tests with a real SQL database (H2 or similar)

### Phase 4: Flight / gRPC (1 week H+AI)
1. Wire aggregate pushdown through FlightSQL protocol
2. Test with a FlightSQL server that supports aggregation

**Total estimate: ~5.5 weeks H+AI, ~2 weeks with 3 engineers**

---

## 10. Key File References

| Component | File | Key Lines |
|-----------|------|-----------|
| Aggregate (logical) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Aggregate.java` | L47, L61-62, L124-137 |
| AggregateExec (physical) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/AggregateExec.java` | L27, L34-48, L143 |
| ExternalSourceExec | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java` | L48, L58-66, L239-252 |
| ExternalRelation | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java` | L46, L115-127 |
| FilterPushdownSupport | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java` | L34, L46 |
| PushFiltersToSource | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java` | L45-58, L241-286 |
| PushStatsToSource | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushStatsToSource.java` | L44, L47-69 |
| PushCountQueryAndTags | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushCountQueryAndTagsToSource.java` | L57-89 |
| Mapper | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java` | L115-138 |
| MapperUtils (aggExec) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java` | L193-224 |
| LocalPhysicalPlanOptimizer | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/LocalPhysicalPlanOptimizer.java` | L39, L69-106 |
| LocalPhysicalOptimizerContext | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/LocalPhysicalOptimizerContext.java` | L17-37 |
| QueryRequest | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/QueryRequest.java` | L20-32 |
| Connector | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/Connector.java` | L22-29 |
| ConnectorFactory | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ConnectorFactory.java` | L17-26 |
| ExternalSourceFactory | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java` | L20-39 |
| OperatorFactoryRegistry | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/OperatorFactoryRegistry.java` | L37, L58-95 |
| AsyncConnectorSourceOp | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncConnectorSourceOperatorFactory.java` | L26, L65-110 |
| SourceOperatorContext | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java` | L41-56 |
| HashAggregationOperator | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/HashAggregationOperator.java` | L41, L136-168, L176-246 |
| AggregateFunction | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/aggregate/AggregateFunction.java` | L61, L65-68, L121-127 |
| Count | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/aggregate/Count.java` | L47 |
| FilterPushdownRegistry | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FilterPushdownRegistry.java` | L24-43 |
| EsStatsQueryExec | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/EsStatsQueryExec.java` | L32-100 |
| PhysicalOptimizerRules | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/PhysicalOptimizerRules.java` | L15-63 |

---

## 11. Risks and Open Questions

1. **Attribute NameId matching**: The pushed source must produce output with
   NameIds that match what downstream operators expect. This is the same
   challenge `PushStatsToSource` has (noted in its TODO at line 129-132).

2. **HAVING equivalent**: ES|QL doesn't have HAVING, but `| STATS count() BY r | WHERE count > 10`
   creates a FilterExec above AggregateExec. If the aggregate is pushed, this
   filter still works because it operates on the source's output. But pushing
   the filter-after-aggregate into the source too (SQL `HAVING`) is a separate
   optimization.

3. **Type compatibility**: ESQL types (LONG, DOUBLE, KEYWORD) must match the
   types returned by the remote source. SQL `COUNT(*)` returns `BIGINT` which
   maps to ES `LONG`, but edge cases exist (e.g. `SUM` on integers may
   overflow differently in SQL vs ES|QL).

4. **Correctness guarantee**: The source must produce exactly the same results
   as ES|QL's aggregation. For `COUNT` and `SUM` this is straightforward. For
   `AVG`, floating-point summation modes may differ. The `SummationMode`
   parameter on `Sum`/`Avg` (lossy vs. compensated) may not have SQL
   equivalents.

5. **Multivalue fields**: ES|QL's aggregation handles multivalued fields
   (duplicating rows for grouping). SQL databases don't have multivalue fields.
   This is a non-issue for SQL sources but relevant for Flight connectors
   backed by ES-like systems.

6. **Filtered aggregations**: `STATS count(*) WHERE x > 5 BY region` produces
   a `FilteredExpression` wrapping the `Count`. SQL can express this as
   `COUNT(*) FILTER (WHERE x > 5)` (standard SQL) or
   `COUNT(CASE WHEN x > 5 THEN 1 END)` (universal). This is pushable but
   adds complexity to V1.
