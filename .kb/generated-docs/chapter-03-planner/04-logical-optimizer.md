<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Generator: Claude Code -->

# Stage 4: Logical Optimizer

**Input**: Resolved `LogicalPlan`
**Output**: Optimized `LogicalPlan`

The logical optimizer transforms the resolved plan into an equivalent but more efficient form. It works purely on the logical level—no knowledge of Lucene, shards, or execution details.

---

## Optimization Philosophy

The optimizer applies algebraic transformations that:
1. **Reduce work**: Fold constants, prune unused columns
2. **Move work earlier**: Push filters toward data sources
3. **Simplify expressions**: Boolean simplification, common subexpression elimination
4. **Prepare for execution**: Combine Sort+Limit into TopN

---

## Rule Batches

**File**: `optimizer/LogicalPlanOptimizer.java`

```java
public class LogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LogicalOptimizerContext> {

    private static final List<Batch<LogicalPlan>> RULES = List.of(
        substitutions(),        // Run once: expand surrogates
        operators(),            // Run until fixed point: main optimizations
        skipCompute(),          // Run once: skip if LIMIT 0
        cleanup(),              // Run once: final transformations
        warnings(),             // Run once: emit warnings
        setAsOptimized()        // Run once: mark plan as optimized
    );
}
```

---

## Substitutions Batch (Run Once)

Expands "surrogate" constructs into their canonical forms:

### SubstituteSurrogatePlans

**LOOKUP → Join**

```
// Before
Lookup[users ON user_id]

// After
LookupJoin
├── left: <original plan>
└── right: EsRelation[users]
    with joinKey: user_id
```

### SubstituteSurrogateAggregations

**AVG → SUM/COUNT**

AVG is a "surrogate" that expands to more primitive operations:

```
// Before
Aggregate[AVG(salary) BY dept]

// After
Eval[$$avg = $$sum / $$count]
└── Aggregate[SUM(salary) AS $$sum, COUNT(salary) AS $$count BY dept]
```

This allows the two-phase aggregation (INITIAL/FINAL) to work correctly—partial averages can't be combined, but partial sums and counts can.

### ReplaceAggregateNestedExpressionWithEval

Nested expressions in aggregations become EVALs:

```
// Before
Aggregate[SUM(price * quantity) BY product]

// After
Aggregate[SUM($$expr) BY product]
└── Eval[$$expr = price * quantity]
```

### ReplaceAliasingEvalWithProject

Simple renames become projections:

```
// Before
Eval[new_name = old_name]

// After
Project[old_name AS new_name]
```

---

## Operators Batch (Run Until Fixed Point)

The main optimization loop. Rules run repeatedly until no rule makes changes.

### ConstantFolding

Evaluate constant expressions at planning time:

```java
public final class ConstantFolding extends OptimizerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformExpressionsDown(Expression.class, e -> {
            if (e.foldable() && e instanceof Literal == false) {
                Object value = e.fold(foldContext);
                return new Literal(e.source(), value, e.dataType());
            }
            return e;
        });
    }
}
```

**Examples**:

```
1 + 2           → 3
NOW()           → 2026-02-05T10:30:00Z (actual timestamp)
"hello" + "!"   → "hello!"
ABS(-5)         → 5
true AND false  → false
```

### FoldNull

Simplify expressions involving NULL:

```
NULL + 5        → NULL
NULL = NULL     → NULL (not true!)
NULL AND true   → NULL
NULL OR true    → true
COALESCE(NULL, 5) → 5
```

### BooleanSimplification

Simplify boolean expressions:

```
true AND x      → x
false AND x     → false
true OR x       → true
false OR x      → x
NOT NOT x       → x
NOT true        → false
x AND x         → x
x OR x          → x
x AND NOT x     → false
x OR NOT x      → true
```

### PropagateEquals

Use equality to simplify:

```
// If we know x = 5
x > 3           → true
x = 5 AND x > 3 → x = 5 (the second is redundant)
```

### CombineBinaryComparisons

Merge overlapping comparisons:

```
x > 3 AND x > 5   → x > 5
x > 3 AND x < 10  → (kept as is, forms a range)
x > 3 AND x < 2   → false (impossible range)
```

### PushDownAndCombineFilters

Push filters toward data sources:

```java
public final class PushDownAndCombineFilters extends OptimizerRule<Filter> {

    @Override
    protected LogicalPlan rule(Filter filter) {
        LogicalPlan child = filter.child();

        // Combine adjacent filters
        if (child instanceof Filter f) {
            return f.with(Predicates.combineAnd(f.condition(), filter.condition()));
        }

        // Push past Eval (if filter doesn't use Eval's fields)
        if (child instanceof Eval eval) {
            return maybePushDownPastUnary(filter, eval);
        }

        // Push past Project (always safe)
        if (child instanceof Project) {
            return pushDownPastProject(filter);
        }

        // Push past OrderBy (filtering doesn't affect sort)
        if (child instanceof OrderBy orderBy) {
            return orderBy.replaceChild(filter.with(orderBy.child(), filter.condition()));
        }

        return filter;
    }
}
```

**Example**:

```
// Before
Filter[host = "web1"]
└── Eval[error = true]
    └── Filter[status >= 400]
        └── EsRelation[logs]

// After (filters combined and pushed down)
Eval[error = true]
└── Filter[status >= 400 AND host = "web1"]
    └── EsRelation[logs]
```

### PushDownAndCombineLimits

Push limits toward data sources:

```
// Before
Limit[10]
└── Eval[x = y + 1]
    └── Filter[...]
        └── EsRelation

// After
Eval[x = y + 1]
└── Filter[...]
    └── Limit[10]
        └── EsRelation
```

### PruneFilters

Remove always-true filters, fail on always-false:

```
// Filter that's always true
Filter[true]
└── ...

// After
... (filter removed)

// Filter that's always false
Filter[false]
└── ...

// After
LocalRelation[empty] (query returns no rows)
```

### PruneColumns

Remove columns that are never used downstream:

```
// Before (status is never used after the filter)
Project[host]
└── Filter[status >= 400]
    └── EsRelation[host, status, message, timestamp, ...]

// After
Project[host]
└── Filter[status >= 400]
    └── EsRelation[host, status]  ← only needed columns
```

### CombineProjections

Merge adjacent projections:

```
// Before
Project[a, b]
└── Project[a, b, c]
    └── EsRelation

// After
Project[a, b]
└── EsRelation
```

### CombineEvals

Merge adjacent EVALs:

```
// Before
Eval[y = x + 1]
└── Eval[x = a + b]
    └── ...

// After
Eval[x = a + b, y = x + 1]
└── ...
```

---

## Cleanup Batch (Run Once)

Final transformations after the main optimization loop.

### ReplaceLimitAndSortAsTopN

Combine Sort + Limit into efficient TopN:

```
// Before
Limit[10]
└── OrderBy[timestamp DESC]
    └── ...

// After
TopN[timestamp DESC, 10]
└── ...
```

TopN is much more efficient than full sort + limit because it only maintains the top N elements.

### ReplaceRowAsLocalRelation

Convert ROW to LocalRelation:

```
// Before
Row[1 AS a, "hello" AS b]

// After
LocalRelation[[a=1, b="hello"]]
```

### SkipQueryOnLimitZero

Short-circuit if LIMIT 0:

```
// Before
Limit[0]
└── <any complex plan>

// After
LocalRelation[empty]
```

---

## Folding: Memory-Bounded Compile-Time Evaluation

Constant folding can produce large values. The `FoldContext` prevents unbounded memory:

```java
public class FoldContext {
    private long allowedBytes;

    public void trackAllocation(Source source, long bytes) {
        allowedBytes -= bytes;
        if (allowedBytes < 0) {
            throw new FoldTooMuchMemoryException(source, bytes, initialAllowedBytes);
        }
    }
}
```

**Example error**: `EVAL x = REPEAT("a", 10000000)` → "Folding query used more than 5% of heap"

---

## Example: Full Optimization

**Query**: `FROM logs | WHERE status >= 400 | EVAL error = true | WHERE host = "web1" | SORT timestamp DESC | LIMIT 10`

### After Analysis

```
Limit[10]
└── OrderBy[timestamp DESC]
    └── Filter[host = "web1"]
        └── Eval[error = true]
            └── Filter[status >= 400]
                └── EsRelation[logs]
```

### After Substitutions

(No changes—no surrogates)

### After Operators (Fixed Point)

```
TopN[timestamp DESC, 10]              ← Sort+Limit combined in cleanup
└── Eval[error = Literal(true)]       ← 'true' is already a literal
    └── Filter[status >= 400 AND host = "web1"]  ← Filters combined & pushed
        └── EsRelation[logs][status, host, timestamp]  ← Pruned columns
```

### After Cleanup

```
TopN[timestamp DESC, 10]
└── Eval[error = true]
    └── Filter[status >= 400 AND host = "web1"]
        └── EsRelation[logs][status, host, timestamp]
```

---

## Files

| File | Purpose |
|------|---------|
| `optimizer/LogicalPlanOptimizer.java` | Main optimizer with batches |
| `optimizer/LogicalOptimizerContext.java` | Configuration for optimizer |
| `optimizer/rules/logical/` | Individual optimization rules |
| `optimizer/rules/OptimizerRules.java` | Base classes for rules |

---

## Next: [Mapper](./05-mapper.md)

The optimized logical plan is now ready to be translated into a physical plan that describes how to execute it.
