# Deep Dive: SQL Pushdown PoC -- SQL Dialect Abstraction, AST Builder, Pushdown Optimizer Rule

**Date**: 2026-03-03
**Branch**: `esql/connector-spi-v3` (analysis of code on `main`)

---

## 1. Existing Filter Pushdown Infrastructure

### 1.1 PushFiltersToSource (Physical Optimizer Rule)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java`

This is the authoritative model for any SQL pushdown rule. It handles three cases:

1. **Lucene path** (lines 50-51): `FilterExec -> EsQueryExec` -- converts ESQL expressions to Lucene QueryBuilder via `TranslationAware` + `TRANSLATOR_HANDLER.asQuery()`.
2. **Lucene with eval** (lines 52-53): `FilterExec -> EvalExec -> EsQueryExec` -- resolves alias references first, then same Lucene path.
3. **External source path** (lines 54-56): `FilterExec -> ExternalSourceExec` -- delegates to `FilterPushdownRegistry` which looks up `FilterPushdownSupport` by sourceType.

The external source path (`planFilterExecForExternalSource`, lines 241-286) is the direct template for SQL pushdown:

```java
private static PhysicalPlan planFilterExecForExternalSource(
    FilterExec filterExec,
    ExternalSourceExec externalExec,
    FilterPushdownRegistry registry
) {
    FilterPushdownSupport pushdownSupport = registry.get(externalExec.sourceType());
    if (pushdownSupport == null) return filterExec;

    List<Expression> filters = splitAnd(filterExec.condition());
    FilterPushdownSupport.PushdownResult result = pushdownSupport.pushFilters(filters);

    if (result.hasPushedFilter()) {
        ExternalSourceExec newExternalExec = externalExec.withPushedFilter(combinedFilter);
        if (result.hasRemainder()) {
            return new FilterExec(filterExec.source(), newExternalExec, Predicates.combineAnd(result.remainder()));
        } else {
            return newExternalExec;
        }
    }
    return filterExec;
}
```

**Key pattern**: Split AND conditions, pass to SPI implementation which classifies each as pushable/remainder, store opaque result in plan node.

### 1.2 FilterPushdownSupport SPI

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java`

The interface has two methods:
- `pushFilters(List<Expression> filters) -> PushdownResult` (primary method, called by optimizer)
- `canPush(Expression expr) -> Pushability` (default NO, fine-grained per-expression check)

`PushdownResult` is a record containing:
- `Object pushedFilter` -- opaque, source-specific filter (e.g., Iceberg Expression, or a SQL WHERE clause string)
- `List<Expression> remainder` -- expressions that must stay in FilterExec

The `Pushability` enum mirrors Lucene's `Translatable`: YES, NO, RECHECK.

### 1.3 FilterPushdownRegistry

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FilterPushdownRegistry.java`

Simple map of `sourceType -> FilterPushdownSupport`. Populated by `DataSourceModule` (line 216-227) from `ExternalSourceFactory.filterPushdownSupport()`.

**Important limitation**: Lazy-loaded factories (connectors, catalogs) return null from `filterPushdownSupport()` by default, so their pushdown support is not registered until the factory is actually loaded. This means SQL pushdown for JDBC connectors would need either eager registration or a different mechanism.

### 1.4 How Pushed Filter Reaches Execution

The flow is:
1. `PushFiltersToSource` stores opaque filter via `ExternalSourceExec.withPushedFilter(Object)` (line 273)
2. `LocalExecutionPlanner.planExternalSource()` (line 1270) copies it: `.pushedFilter(externalSource.pushedFilter())`
3. `SourceOperatorContext` record carries `Object pushedFilter` (line 51)
4. The source-specific `SourceOperatorFactoryProvider.create(context)` extracts and interprets it

For connectors specifically, `OperatorFactoryRegistry.factory()` (lines 62-81) creates a `QueryRequest` and passes it to `AsyncConnectorSourceOperatorFactory`. However, **the pushed filter is NOT currently included in QueryRequest** -- it goes through `SourceOperatorContext` which is used by file-based factories, not by the connector path.

**This is a critical gap**: For the connector path (line 63-81), `OperatorFactoryRegistry` creates `QueryRequest` with `target`, `projectedColumns`, `attributes`, `config`, `batchSize` -- but no filter. The `pushedFilter` from `SourceOperatorContext` is not threaded through to `Connector.execute()`.

---

## 2. ExternalSourceExec Plan Node

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`

Key fields:
- `String sourcePath` -- the URI/path to the data source
- `String sourceType` -- type identifier (e.g., "parquet", "flight", "jdbc")
- `List<Attribute> attributes` -- output schema (column projections)
- `Map<String, Object> config` -- source-specific configuration
- `Map<String, Object> sourceMetadata` -- opaque metadata from resolution
- `Object pushedFilter` -- **opaque, NOT serialized** (coordinator-only execution)
- `Integer estimatedRowSize` -- for page size calculation
- `FileSet fileSet` -- NOT serialized (coordinator-only)
- `List<ExternalSplit> splits` -- serialized for distributed execution

The `pushedFilter` field is `Object` (line 63), deliberately opaque. The Javadoc at line 41-43 explains: "The pushedFilter is an opaque Object that only the source-specific operator factory interprets. It is NOT serialized because external sources execute on coordinator only."

Mutator: `withPushedFilter(Object newFilter)` returns a new instance (line 239-252).

**For SQL pushdown**: The pushedFilter could be a `String` containing the generated SQL WHERE clause, or a structured `SqlWhereClause` object. Since it's never serialized, we have full freedom in type choice.

---

## 3. ESQL Expression AST (Types Requiring SQL Translation)

### 3.1 Expression Hierarchy

**Base**: `Expression` extends `Node<Expression>` -- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/Expression.java`

The expression types that a SQL generator must handle for WHERE clause translation:

#### Comparison Operators (in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/predicate/operator/comparison/`)

| Class | SQL Equivalent | Key Pattern |
|-------|---------------|-------------|
| `Equals` | `=` | `EsqlBinaryComparison`, left/right children, implements `Negatable` |
| `NotEquals` | `!=` / `<>` | Same base |
| `LessThan` | `<` | Same base |
| `LessThanOrEqual` | `<=` | Same base |
| `GreaterThan` | `>` | Same base |
| `GreaterThanOrEqual` | `>=` | Same base |
| `In` | `IN (...)` | Extends `EsqlScalarFunction`, has `value()` and `list()` |

All binary comparisons extend `EsqlBinaryComparison` which extends `BinaryComparison`. The pattern is always `left() op right()` where typically `left()` is a `FieldAttribute`/`NamedExpression` and `right()` is a foldable `Literal`.

#### Logical Operators (in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/predicate/logical/`)

| Class | SQL Equivalent | Key Pattern |
|-------|---------------|-------------|
| `And` | `AND` | `BinaryLogic`, `left()` + `right()` |
| `Or` | `OR` | `BinaryLogic`, `left()` + `right()` |
| `Not` | `NOT` | `UnaryScalarFunction`, `field()` is child |

#### Null Checks (in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/predicate/nulls/`)

| Class | SQL Equivalent |
|-------|---------------|
| `IsNull` | `IS NULL` |
| `IsNotNull` | `IS NOT NULL` |

#### Range (in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/predicate/`)

| Class | SQL Equivalent | Notes |
|-------|---------------|-------|
| `Range` | `BETWEEN` or `x >= lower AND x <= upper` | Has `value()`, `lower()`, `upper()`, `includeLower()`, `includeUpper()` |

#### Value Types

- **`Literal`** (`x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/Literal.java`): Foldable constant values. `value()` returns the Java value, `dataType()` returns the type.
- **`FieldAttribute`** (`x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/FieldAttribute.java`): Column reference. `name()` returns the field name.

### 3.2 Foldable Pattern

Expressions where `foldable() == true` can be constant-folded to get their Java value via `fold(FoldContext)`. Literals are always foldable. The `Foldables.literalValueOf()` utility is the canonical way to extract values.

Values come back as Java types: `BytesRef` for strings, `Integer`/`Long`/`Double` for numerics, etc. A SQL generator needs type-aware value formatting (e.g., quoting strings, formatting dates).

---

## 4. Optimizer Rule Patterns

### 4.1 Common Structure

All physical optimizer rules in the local optimizer follow one of two patterns:

**Pattern 1: Simple rule** (`OptimizerRule<SubPlan>`):
```java
public class PushLimitToSource extends PhysicalOptimizerRules.OptimizerRule<LimitExec> {
    @Override
    protected PhysicalPlan rule(LimitExec limitExec) {
        // Match pattern, transform plan node
    }
}
```

**Pattern 2: Parameterized rule** (`ParameterizedOptimizerRule<SubPlan, Context>`):
```java
public class PushFiltersToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec, LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
        // Match pattern, use context for registry lookup
    }
}
```

The `LocalPhysicalOptimizerContext` (file: `LocalPhysicalOptimizerContext.java`) is a record containing: `plannerSettings`, `flags`, `configuration`, `foldCtx`, `searchStats`, `filterPushdownRegistry`.

### 4.2 Registration

Rules are registered in `LocalPhysicalPlanOptimizer.rules()` (line 70-106). The "Push to ES" batch runs repeatedly. A SQL pushdown rule would be added to this batch.

### 4.3 How a PushdownToSql Rule Would Work

A SQL pushdown rule would follow the `PushFiltersToSource` pattern exactly, but instead of producing an opaque Iceberg expression or Lucene QueryBuilder, it would produce a SQL WHERE clause string. The rule would:

1. Match `FilterExec -> ExternalSourceExec` where sourceType is a SQL-capable connector (e.g., "jdbc")
2. Walk the ESQL expression tree, translating each node to SQL syntax
3. Classify expressions as pushable (translatable to SQL) vs remainder (must stay in compute engine)
4. Store the SQL WHERE clause in `ExternalSourceExec.pushedFilter()`
5. When the operator factory builds the query, it wraps the pushed filter into a full SELECT statement

The rule would be a `ParameterizedOptimizerRule<FilterExec, LocalPhysicalOptimizerContext>` so it can access the `FilterPushdownRegistry` for SQL dialect information.

---

## 5. ES SQL Plugin Analysis -- Can It Be Reused?

### 5.1 What the SQL Plugin Has

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/sql/src/main/java/org/elasticsearch/xpack/sql/planner/QueryTranslator.java`

The SQL plugin's `QueryTranslator` translates **SQL expressions into Elasticsearch QueryBuilder** (Lucene queries), NOT into SQL strings. It's the reverse of what we need:
- It **parses** SQL -> Expression AST
- It **translates** Expression AST -> Elasticsearch QueryBuilder (for ES execution)

The translator has a chain of `SqlExpressionTranslator` instances:
- `BinaryComparisons` -- translates `=, !=, <, <=, >, >=` to Lucene range/term queries
- `InComparisons` -- translates `IN` to terms queries
- `Ranges` -- translates `BETWEEN` to range queries
- `BinaryLogic` -- translates `AND`/`OR` to bool queries
- `Nots` -- translates `NOT` to negated queries
- `IsNullTranslator` / `IsNotNullTranslator` -- translates null checks to exists queries
- `Likes` -- translates `LIKE` to regex queries
- `Scalars` -- translates scalar functions to scripted queries

### 5.2 Can It Be Reused?

**No, the SQL plugin cannot be directly reused for SQL generation.** The reasons:

1. **Wrong direction**: The SQL plugin translates SQL -> ES queries. We need ESQL expressions -> SQL strings. It's the inverse operation.

2. **Different expression trees**: The SQL plugin uses `org.elasticsearch.xpack.ql.expression.*` (the old QL library). ESQL uses `org.elasticsearch.xpack.esql.core.expression.*` and `org.elasticsearch.xpack.esql.expression.*`. These are different class hierarchies (ESQL forked from QL).

3. **No SQL generation code exists anywhere in the codebase.** The SQL plugin never needs to generate SQL -- it receives SQL from clients and converts it to ES operations.

4. **The QueryTranslator pattern IS useful as an architectural reference.** The chain-of-translators pattern (each translator handles one expression type) is a good model for a SQL generator. But the implementation must be written from scratch.

### 5.3 What IS Reusable

The **IcebergPushdownFilters** class is the best reference for a SQL generator:

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`

This class converts ESQL expressions to Iceberg expressions using the exact same pattern needed for SQL generation. The `convert()` method (lines 55-138) handles:
- `EsqlBinaryComparison` (Equals, NotEquals, LT, LTE, GT, GTE)
- `In` expressions
- `IsNull` / `IsNotNull`
- `Range` (BETWEEN)
- `BinaryLogic` (And, Or)
- `Not`

A SQL generator would have the same structure -- just replace `org.apache.iceberg.expressions.Expressions.*` calls with SQL string building.

---

## 6. SPI Hooks for Connector-Provided Optimization Rules

### 6.1 Current State

The current SPI has `ExternalSourceFactory.filterPushdownSupport()`:

```java
// ExternalSourceFactory.java (line 28)
default FilterPushdownSupport filterPushdownSupport() {
    return null;
}
```

This returns a `FilterPushdownSupport` implementation that handles filter translation. This is the only optimization hook.

**There is no `optimizationRules()` method on any SPI interface.** The proposal's `optimizationRules()` concept does not exist in the current codebase. The proposal document mentions it as a replacement for `filterPushdownSupport()`.

### 6.2 What optimizationRules() Would Look Like

The proposal envisions connectors providing their own optimizer rules:

```java
// Proposed addition to ExternalSourceFactory (or the new DataSourceFactory)
default List<Rule<?, PhysicalPlan>> optimizationRules() {
    return List.of();
}
```

These rules would run as a post-pass in `LocalPhysicalPlanOptimizer`, after the standard "Push to ES" batch. The `DataSourceModule` would collect rules from all registered factories and inject them.

For SQL pushdown, the connector would provide a rule like:

```java
public class JdbcPushdownRule extends ParameterizedOptimizerRule<FilterExec, LocalPhysicalOptimizerContext> {
    private final SqlDialect dialect;

    @Override
    protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
        if (filterExec.child() instanceof ExternalSourceExec exec && "jdbc".equals(exec.sourceType())) {
            String sql = dialect.generateWhere(splitAnd(filterExec.condition()));
            return exec.withPushedFilter(sql);
        }
        return filterExec;
    }
}
```

### 6.3 Alternative: Keep FilterPushdownSupport

The simpler approach (which works today) is to implement `FilterPushdownSupport` for SQL connectors:

```java
public class SqlFilterPushdownSupport implements FilterPushdownSupport {
    private final SqlDialect dialect;

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        StringBuilder where = new StringBuilder();
        List<Expression> remainder = new ArrayList<>();
        for (Expression filter : filters) {
            String sql = dialect.translate(filter);
            if (sql != null) {
                if (where.length() > 0) where.append(" AND ");
                where.append(sql);
            } else {
                remainder.add(filter);
            }
        }
        if (where.length() > 0) {
            return new PushdownResult(where.toString(), remainder);
        }
        return PushdownResult.none(filters);
    }
}
```

This approach uses the existing SPI without any changes, and the existing `PushFiltersToSource` rule already handles it (lines 54-56, 241-286).

---

## 7. QueryRequest Analysis

### 7.1 Current Shape

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/QueryRequest.java`

```java
public record QueryRequest(
    String target,                    // table name or path
    List<String> projectedColumns,    // column names to select
    List<Attribute> attributes,       // full schema attributes
    Map<String, Object> config,       // source config
    int batchSize,                    // rows per page
    BlockFactory blockFactory         // for memory allocation
) { ... }
```

### 7.2 What's Missing for SQL

`QueryRequest` has **no filter field**. For SQL pushdown to work, the `QueryRequest` must carry the pushed filter so the connector can build a complete SQL statement. The filter must be available when `Connector.execute(QueryRequest, Split)` is called.

**Required additions** (at minimum):
```java
public record QueryRequest(
    String target,
    List<String> projectedColumns,
    List<Attribute> attributes,
    Map<String, Object> config,
    int batchSize,
    BlockFactory blockFactory,
    String pushedWhereClause,     // NEW: SQL WHERE clause (null if no filter)
    Integer pushedLimit           // NEW: LIMIT value (null if no limit)
) { ... }
```

Alternatively, the pushed filter could be passed as a more general `Object pushedFilter` field (matching the pattern in `ExternalSourceExec` and `SourceOperatorContext`), letting each connector interpret it.

### 7.3 The Threading Gap

As noted in Section 1.4, there is a gap in how the connector path receives the pushed filter. In `OperatorFactoryRegistry.factory()` (lines 62-81), when the source is a `ConnectorFactory`:

```java
if (sf instanceof ConnectorFactory cf) {
    Connector connector = cf.open(context.config());
    // ... builds QueryRequest without pushedFilter ...
    QueryRequest request = new QueryRequest(target, projectedColumns, ...);
    return new AsyncConnectorSourceOperatorFactory(connector, request, ...);
}
```

The `context.pushedFilter()` is available but **never passed to `QueryRequest` or `Connector`**. This must be fixed for any pushdown (not just SQL) to work on the connector path.

---

## 8. Proposed Architecture for SQL Pushdown PoC

### 8.1 SQL Dialect Abstraction

A `SqlDialect` interface that handles database-specific syntax differences:

```java
public interface SqlDialect {
    /** Database identifier for this dialect (e.g., "postgresql", "mysql") */
    String name();

    /** Quote a column name for this dialect */
    String quoteIdentifier(String name);

    /** Format a literal value for this dialect */
    String formatLiteral(Object value, DataType type);

    /** Translate an ESQL expression to a SQL fragment, or null if unsupported */
    String translate(Expression expr);

    /** Build a complete SELECT statement */
    String buildSelect(String table, List<String> columns, String whereClause, Integer limit);
}
```

Default implementations: `AnsiSqlDialect` (base), `PostgreSqlDialect`, `MySqlDialect`.

### 8.2 SQL AST Builder

Modeled directly on `IcebergPushdownFilters.convert()`, a `SqlExpressionTranslator` that walks the ESQL expression tree:

```java
public class SqlExpressionTranslator {
    private final SqlDialect dialect;

    public String translate(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            String field = dialect.quoteIdentifier(ne.name());
            String value = dialect.formatLiteral(literalValueOf(bc.right()), bc.right().dataType());
            return switch (bc) {
                case Equals ignored -> field + " = " + value;
                case NotEquals ignored -> field + " <> " + value;
                case LessThan ignored -> field + " < " + value;
                case LessThanOrEqual ignored -> field + " <= " + value;
                case GreaterThan ignored -> field + " > " + value;
                case GreaterThanOrEqual ignored -> field + " >= " + value;
                default -> null;
            };
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            // ... translate to "field IN (v1, v2, ...)"
        }
        if (expr instanceof IsNull isNullExpr && isNullExpr.field() instanceof NamedExpression ne) {
            return dialect.quoteIdentifier(ne.name()) + " IS NULL";
        }
        // ... And, Or, Not, Range, IsNotNull
        return null;
    }
}
```

### 8.3 FilterPushdownSupport Implementation

```java
public class SqlFilterPushdownSupport implements FilterPushdownSupport {
    private final SqlDialect dialect;
    private final SqlExpressionTranslator translator;

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<String> sqlFragments = new ArrayList<>();
        List<Expression> remainder = new ArrayList<>();
        for (Expression filter : filters) {
            String sql = translator.translate(filter);
            if (sql != null) {
                sqlFragments.add(sql);
            } else {
                remainder.add(filter);
            }
        }
        if (sqlFragments.isEmpty()) {
            return PushdownResult.none(filters);
        }
        String whereClause = String.join(" AND ", sqlFragments);
        return new PushdownResult(whereClause, remainder);
    }
}
```

### 8.4 Integration Points

1. **Register `SqlFilterPushdownSupport`** on a JDBC `ConnectorFactory` via `filterPushdownSupport()`.
2. **Fix `OperatorFactoryRegistry`** to thread `pushedFilter` into `QueryRequest`.
3. **Add `pushedFilter` field** to `QueryRequest`.
4. **In `JdbcConnector.execute()`**, use the dialect to build a complete SELECT from `QueryRequest.target()`, `projectedColumns()`, and `pushedFilter`.

No new optimizer rules needed -- the existing `PushFiltersToSource` rule already dispatches to `FilterPushdownSupport` for external sources.

---

## 9. Effort Estimate and Dependencies

### What Exists (reusable today, zero changes needed)
- `PushFiltersToSource` external source path (lines 241-286)
- `FilterPushdownSupport` SPI interface
- `FilterPushdownRegistry` and registration in `DataSourceModule`
- `ExternalSourceExec.pushedFilter()` opaque field
- `SourceOperatorContext.pushedFilter()` threading
- `IcebergPushdownFilters` as a reference implementation

### What Must Be Built
1. **`SqlDialect` interface + `AnsiSqlDialect`** -- ~150 lines, straightforward
2. **`SqlExpressionTranslator`** -- ~200 lines, modeled on `IcebergPushdownFilters.convert()`
3. **`SqlFilterPushdownSupport`** -- ~50 lines, implements existing SPI
4. **`QueryRequest` filter field** -- add one field + thread through `OperatorFactoryRegistry`
5. **JDBC connector** (or mock) -- to validate end-to-end

### What Must Be Fixed
- **`OperatorFactoryRegistry.factory()`** connector path (lines 62-81): thread `pushedFilter` to `QueryRequest`
- **Lazy factory registration**: SQL connector's `FilterPushdownSupport` needs eager registration or dynamic lookup

### Size
- Core SQL dialect/translator/pushdown: ~400 lines of new code
- QueryRequest fix: ~20 lines changed
- OperatorFactoryRegistry fix: ~10 lines changed
- Tests (unit for translator, integration for end-to-end): ~500 lines
- Total PoC: ~900-1000 lines

### Risk
Low. The existing infrastructure handles all the hard parts (optimizer rule dispatch, plan node management, opaque filter threading). The SQL generator is a clean, isolated component with no coupling to existing code beyond the expression AST.

---

## 10. Summary of Key Findings

| Question | Answer |
|----------|--------|
| Does SQL generation exist? | **No.** Zero SQL string generation anywhere in the codebase. ES SQL plugin translates SQL-to-ES, not ES-to-SQL. |
| Can ES SQL plugin be reused? | **No.** Different direction, different expression tree (QL vs ESQL core). Pattern is useful as reference only. |
| Best reference implementation? | **`IcebergPushdownFilters.convert()`** -- exact same ESQL expression tree, exact same visitor pattern, just targeting Iceberg expressions instead of SQL strings. |
| Does the SPI support SQL pushdown today? | **Almost.** `FilterPushdownSupport` + `PushFiltersToSource` handle the optimizer side. Gap: `QueryRequest` doesn't carry filters, `OperatorFactoryRegistry` doesn't thread filters to connectors. |
| Does `optimizationRules()` exist? | **No.** Not in any SPI interface. `filterPushdownSupport()` is the only hook. The proposal's `optimizationRules()` is a design idea, not implemented. |
| How big is the PoC? | **~1000 lines** including tests. Most of the infrastructure (optimizer rule, plan node, SPI) already exists. |
| What's the critical path? | Threading `pushedFilter` from `SourceOperatorContext` through `OperatorFactoryRegistry` to `QueryRequest` to `Connector.execute()`. Without this fix, no pushdown of any kind works for the connector path. |
