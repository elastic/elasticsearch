<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Generator: Claude Code -->

# Stage 3: Analyzer

**Input**: Unresolved `LogicalPlan` + resolved index mappings + enrich policies
**Output**: Resolved `LogicalPlan` (all references resolved, all types known)

The analyzer is where the plan becomes "real." Every reference is resolved to a concrete entity with a known type. If something doesn't exist or types don't match, this is where errors are generated.

---

## What Gets Resolved

| Before (Unresolved) | After (Resolved) | Information Added |
|---------------------|------------------|-------------------|
| `UnresolvedRelation("logs")` | `EsRelation("logs", [fields...])` | Full field list with types |
| `UnresolvedAttribute("status")` | `FieldAttribute("status", INTEGER)` | Data type, nullability |
| `UnresolvedFunction("ABS", [x])` | `Abs(x)` | Concrete function class |
| `UnresolvedStar` | List of all fields | Expanded to actual columns |

---

## Architecture

The analyzer is a **rule-based transformer** that runs batches of rules:

```java
public class Analyzer extends ParameterizedRuleExecutor<LogicalPlan, AnalyzerContext> {

    private static final List<Batch<LogicalPlan>> RULES = List.of(
        // Run once: resolve tables, enrich, functions
        new Batch<>("Initialize", Limiter.ONCE,
            new ResolveTable(),
            new ResolveEnrich(),
            new ResolveLookupTables(),
            new ResolveFunctions()
        ),

        // Run until no changes: resolve references, add casts
        new Batch<>("Resolution",
            new ResolveRefs(),
            new ResolveUnionTypes(),
            new ImplicitCasting()
        ),

        // Run once: final adjustments
        new Batch<>("Finish Analysis", Limiter.ONCE,
            new AddImplicitLimit()
        )
    );

    public LogicalPlan analyze(LogicalPlan plan, AnalyzerContext context) {
        LogicalPlan resolved = execute(plan);  // Run all rules
        verify(resolved);                       // Check for errors
        return resolved;
    }
}
```

---

## Key Rules

### ResolveTable

Converts `UnresolvedRelation` to `EsRelation`:

```java
private class ResolveTable extends ParameterizedAnalyzerRule<UnresolvedRelation> {

    @Override
    protected LogicalPlan rule(UnresolvedRelation plan, AnalyzerContext context) {
        IndexResolution resolution = context.indexResolution().get(plan.indexPattern());

        if (resolution.isValid() == false) {
            // Index doesn't exist or no matching fields
            return plan.withUnresolvedMessage(resolution.toString());
        }

        EsIndex esIndex = resolution.get();

        // Convert mapping to attributes
        List<Attribute> attributes = mappingAsAttributes(esIndex.mapping());

        return new EsRelation(
            plan.source(),
            esIndex,
            plan.indexMode(),
            attributes,
            plan.frozen()
        );
    }

    private List<Attribute> mappingAsAttributes(Map<String, MappedFieldType> mapping) {
        // Each field becomes a FieldAttribute with its DataType
        return mapping.entrySet().stream()
            .map(e -> new FieldAttribute(e.getKey(), esTypeToDataType(e.getValue())))
            .toList();
    }
}
```

### ResolveRefs

Resolves `UnresolvedAttribute` to actual attributes from the input:

```java
private class ResolveRefs extends AnalyzerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        // Get available attributes from children
        AttributeSet available = plan.inputSet();

        return plan.transformExpressionsOnly(UnresolvedAttribute.class, ua -> {
            // Find matching attribute by name
            List<Attribute> matches = available.stream()
                .filter(a -> a.name().equalsIgnoreCase(ua.name()))
                .toList();

            if (matches.size() == 1) {
                return matches.get(0);  // Resolved!
            } else if (matches.isEmpty()) {
                return ua.withUnresolvedMessage("Unknown column [" + ua.name() + "]");
            } else {
                return ua.withUnresolvedMessage("Ambiguous column [" + ua.name() + "]");
            }
        });
    }
}
```

### ResolveFunctions

Resolves `UnresolvedFunction` to concrete function implementations:

```java
private class ResolveFunctions extends AnalyzerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformExpressionsOnly(UnresolvedFunction.class, uf -> {
            // Look up function in registry
            FunctionDefinition def = functionRegistry.resolveFunction(uf.name());

            if (def == null) {
                return uf.withUnresolvedMessage("Unknown function [" + uf.name() + "]");
            }

            // Build the concrete function
            return def.builder().build(uf.source(), uf.arguments());
        });
    }
}
```

### ImplicitCasting

Adds type conversions where needed:

```java
private class ImplicitCasting extends AnalyzerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformExpressionsOnly(BinaryExpression.class, binary -> {
            DataType leftType = binary.left().dataType();
            DataType rightType = binary.right().dataType();

            if (leftType != rightType) {
                // Find common type and add casts
                DataType common = commonType(leftType, rightType);
                Expression left = maybeCast(binary.left(), common);
                Expression right = maybeCast(binary.right(), common);
                return binary.replaceChildren(List.of(left, right));
            }
            return binary;
        });
    }

    private Expression maybeCast(Expression e, DataType target) {
        if (e.dataType() == target) return e;
        return new Cast(e.source(), e, target);
    }
}
```

### AddImplicitLimit

Adds a default LIMIT if none specified (prevents unbounded results):

```java
private class AddImplicitLimit extends AnalyzerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        // Check if there's already a limit
        if (plan.anyMatch(p -> p instanceof Limit)) {
            return plan;
        }

        // Add default limit
        int defaultLimit = configuration.resultTruncationDefaultSize();  // e.g., 1000
        return new Limit(plan.source(), plan, new Literal(Source.EMPTY, defaultLimit, INTEGER));
    }
}
```

---

## Verification

After all rules run, the `Verifier` checks for errors:

```java
public class Verifier {

    public Collection<Failure> verify(LogicalPlan plan) {
        Failures failures = new Failures();

        // Check for unresolved elements
        plan.forEachDown(p -> {
            p.forEachExpression(UnresolvedAttribute.class, ua -> {
                failures.add(fail(ua, ua.unresolvedMessage()));
            });
            p.forEachExpression(UnresolvedFunction.class, uf -> {
                failures.add(fail(uf, uf.unresolvedMessage()));
            });
        });

        // Run PostAnalysisVerificationAware checks
        plan.forEachDown(p -> {
            if (p instanceof PostAnalysisVerificationAware aware) {
                aware.postAnalysisVerification(failures);
            }
            p.forEachExpression(PostAnalysisVerificationAware.class, aware -> {
                aware.postAnalysisVerification(failures);
            });
        });

        // Run PostAnalysisPlanVerificationAware checks (tree-structure)
        plan.forEachExpressionDown(PostAnalysisPlanVerificationAware.class, aware -> {
            BiConsumer<LogicalPlan, Failures> check = aware.postAnalysisPlanVerification();
            plan.forEachDown(p -> check.accept(p, failures));
        });

        return failures.failures();
    }
}
```

---

## Type Checking Examples

### Filter Condition Must Be Boolean

```java
// Filter implements PostAnalysisVerificationAware
@Override
public void postAnalysisVerification(Failures failures) {
    DataType condType = condition.dataType();
    if (condType != BOOLEAN && condType != NULL) {
        failures.add(fail(condition,
            "Condition expression needs to be boolean, found [{}]",
            condType));
    }
}
```

**Example error**: `FROM logs | WHERE status` → "Condition expression needs to be boolean, found [INTEGER]"

### Function Argument Types

Functions validate their arguments in `resolveType()`:

```java
// In Abs function
@Override
protected TypeResolution resolveType() {
    if (childrenResolved() == false) {
        return TypeResolution.TYPE_RESOLVED;
    }
    return TypeResolutions.isNumeric(field(), sourceText(), ParamOrdinal.DEFAULT);
}
```

**Example error**: `FROM logs | EVAL x = ABS("hello")` → "argument of [ABS] must be numeric, found [KEYWORD]"

### Grouping Functions Only in STATS

```java
// GroupingFunction implements PostAnalysisPlanVerificationAware
@Override
public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
    return (plan, failures) -> {
        if (plan instanceof Aggregate == false) {
            plan.forEachExpression(GroupingFunction.class, gf ->
                failures.add(fail(gf,
                    "cannot use grouping function [{}] outside of a STATS command",
                    gf.sourceText())));
        }
    };
}
```

**Example error**: `FROM logs | EVAL b = BUCKET(@timestamp, 1 hour)` → "cannot use grouping function [BUCKET(@timestamp, 1 hour)] outside of a STATS command"

---

## Example: Before and After Analysis

### Before Analysis

```
Limit[100]
└── Filter[GreaterThanOrEqual[UnresolvedAttribute("status"), Literal(400)]]
    └── UnresolvedRelation["logs"]
```

### After Analysis

```
Limit[Literal(100, INTEGER)]
└── Filter[GreaterThanOrEqual[FieldAttribute("status", INTEGER), Literal(400, INTEGER)]]
    └── EsRelation["logs"][
          FieldAttribute("status", INTEGER),
          FieldAttribute("host", KEYWORD),
          FieldAttribute("@timestamp", DATETIME),
          FieldAttribute("message", TEXT),
          ...
        ]
```

Now we know:
- The index `logs` exists with these fields
- `status` is an INTEGER field
- The comparison `status >= 400` is valid (INTEGER >= INTEGER → BOOLEAN)
- The filter condition is BOOLEAN (valid)

---

## Multi-Index Queries

When querying multiple indices with `FROM logs-*`:

```java
// If logs-2024 has status:integer but logs-2023 has status:keyword
// The ResolveUnionTypes rule creates a union type

FieldAttribute("status", UNION[INTEGER, KEYWORD])

// Functions that can't handle union types will fail
// Or implicit casts may be added
```

---

## Files

| File | Purpose |
|------|---------|
| `analysis/Analyzer.java` | Main analyzer with rule batches |
| `analysis/Verifier.java` | Post-analysis verification |
| `analysis/AnalyzerContext.java` | Context with index resolutions |
| `expression/function/EsqlFunctionRegistry.java` | Function lookup |

---

## Next: [Logical Optimizer](./04-logical-optimizer.md)

With a fully resolved plan, the optimizer can now rewrite it for better performance.
