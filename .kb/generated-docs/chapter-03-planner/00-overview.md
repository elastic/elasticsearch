<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Source hash: planner-v3 -->
<!-- Generator: Claude Code (claude-opus-4-5-20251101) -->

# Chapter III: The ES|QL Planner

This chapter provides a deep dive into the ES|QL planner—the component that transforms a query string into an executable physical plan. The planner operates through a series of distinct phases, each with specific responsibilities.

**Sub-chapters:**

| # | Document | Topic |
|---|----------|-------|
| 0 | [This document](#mental-model-the-ast) | Overview, AST mental model, plan types |
| 1 | [Parser](./01-parser.md) | Query string → Unresolved LogicalPlan |
| 2 | [Pre-Analyzer](./02-pre-analyzer.md) | Discover index and enrich references |
| 3 | [Analyzer](./03-analyzer.md) | Resolution, type checking, verification |
| 4 | [Logical Optimizer](./04-logical-optimizer.md) | Rewrites, folding, pushdown |
| 5 | [Mapper](./05-mapper.md) | LogicalPlan → PhysicalPlan |
| 6 | [Physical Optimizer](./06-physical-optimizer.md) | Lucene pushdown, field extraction |
| 7 | [Complete Example](./07-complete-example.md) | Traced query through all stages |

For the request/response lifecycle that invokes the planner, see [Chapter I](../chapter-01-request-response.md). For query management and monitoring, see [Chapter II](../chapter-02-monitoring.md).

---

## The Pipeline at a Glance

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    ES|QL PLANNER PIPELINE                                        │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘

  "FROM logs | WHERE status >= 400 | STATS count(*) BY host | SORT count DESC | LIMIT 10"
                                              │
                                              ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  STAGE 1: PARSER                                                                                ┃
┃  ─────────────────                                                                              ┃
┃  Input:  Query string                                                                           ┃
┃  Output: Unresolved LogicalPlan                                                                 ┃
┃                                                                                                 ┃
┃  What happens:                                                                                  ┃
┃  • ANTLR lexer tokenizes the query string                                                       ┃
┃  • ANTLR parser builds a parse tree (CST)                                                       ┃
┃  • AstBuilder visits the parse tree and creates plan nodes                                      ┃
┃  • References are UNRESOLVED (UnresolvedRelation, UnresolvedAttribute, UnresolvedFunction)      ┃
┃  • No type information yet - just syntactic structure                                           ┃
┃  • Query parameters (?) are substituted with Literal nodes                                      ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                              │
                                              ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  STAGE 2: PRE-ANALYZER                                                                          ┃
┃  ─────────────────────                                                                          ┃
┃  Input:  Unresolved LogicalPlan                                                                 ┃
┃  Output: PreAnalysis record (index patterns, enrich policies, lookup tables)                    ┃
┃                                                                                                 ┃
┃  What happens:                                                                                  ┃
┃  • Scans the plan for UnresolvedRelation nodes → collects index patterns                        ┃
┃  • Scans for Enrich nodes → collects enrich policy references                                   ┃
┃  • Scans for LOOKUP joins → collects lookup index patterns                                      ┃
┃  • Detects special field type requirements (aggregate_metric_double, dense_vector)              ┃
┃  • Does NOT modify the plan - just extracts references for async resolution                     ┃
┃  • EsqlSession uses this to call FieldCaps API and resolve enrich policies                      ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                              │
                              [Async: resolve index mappings, enrich policies]
                                              │
                                              ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  STAGE 3: ANALYZER                                                                              ┃
┃  ─────────────────                                                                              ┃
┃  Input:  Unresolved LogicalPlan + resolved index mappings + enrich policies                     ┃
┃  Output: Resolved LogicalPlan (all references resolved, all types known)                        ┃
┃                                                                                                 ┃
┃  What happens:                                                                                  ┃
┃  • UnresolvedRelation → EsRelation (with full field list from mapping)                          ┃
┃  • UnresolvedAttribute → FieldAttribute/MetadataAttribute (with DataType)                       ┃
┃  • UnresolvedFunction → concrete function (ABS, COUNT, etc.) via FunctionRegistry               ┃
┃  • Implicit type casts inserted where needed (e.g., int compared to long)                       ┃
┃  • Union types resolved for multi-index queries with conflicting field types                    ┃
┃  • Default LIMIT added if missing (prevents unbounded result sets)                              ┃
┃  • VERIFICATION: type checking, semantic validation via marker interfaces                       ┃
┃  • Errors collected: unknown columns, type mismatches, invalid function arguments               ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                              │
                                              ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  STAGE 4: LOGICAL OPTIMIZER                                                                     ┃
┃  ──────────────────────────                                                                     ┃
┃  Input:  Resolved LogicalPlan                                                                   ┃
┃  Output: Optimized LogicalPlan                                                                  ┃
┃                                                                                                 ┃
┃  What happens (in order):                                                                       ┃
┃                                                                                                 ┃
┃  SUBSTITUTIONS (run once):                                                                      ┃
┃  • Surrogate plans expanded: LOOKUP → Join                                                      ┃
┃  • Surrogate aggregations expanded: AVG(x) → SUM(x)/COUNT(x)                                    ┃
┃  • Nested aggregation expressions moved to EVAL: STATS a+b → EVAL $$=a+b | STATS                ┃
┃  • Regex patterns simplified to more efficient forms                                            ┃
┃                                                                                                 ┃
┃  OPERATORS (run until fixed point):                                                             ┃
┃  • CONSTANT FOLDING: 1+2 → 3, NOW() → actual timestamp                                          ┃
┃  • BOOLEAN SIMPLIFICATION: true AND x → x, NOT NOT x → x                                        ┃
┃  • FILTER PUSHDOWN: filters pushed past Eval, Project, OrderBy toward data source              ┃
┃  • FILTER COMBINATION: adjacent WHERE clauses merged with AND                                   ┃
┃  • LIMIT PUSHDOWN: limits pushed toward data source                                             ┃
┃  • PROJECTION PRUNING: unused columns removed from intermediate results                         ┃
┃  • EVAL COMBINATION: adjacent EVAL commands merged                                              ┃
┃                                                                                                 ┃
┃  CLEANUP (run once):                                                                            ┃
┃  • Sort + Limit → TopN (combined for efficiency)                                                ┃
┃  • ROW → LocalRelation                                                                          ┃
┃  • Skip query entirely if LIMIT 0                                                               ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                              │
                                              ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  STAGE 5: MAPPER                                                                                ┃
┃  ───────────────                                                                                ┃
┃  Input:  Optimized LogicalPlan                                                                  ┃
┃  Output: PhysicalPlan (coordinator perspective)                                                 ┃
┃                                                                                                 ┃
┃  What happens:                                                                                  ┃
┃  • EsRelation → FragmentExec (wraps logical subtree to send to data nodes)                      ┃
┃  • Pipeline breakers identified: Aggregate, Limit, TopN need coordination                       ┃
┃  • ExchangeExec inserted at pipeline boundaries (data flow between nodes)                       ┃
┃  • Two-phase aggregation set up: INITIAL (data nodes) + FINAL (coordinator)                     ┃
┃  • Streaming operations stay inside FragmentExec (Filter, Eval, Project)                        ┃
┃  • Decision made: what runs on data nodes vs coordinator                                        ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                              │
                              ┌───────────────┴───────────────┐
                              ▼                               ▼
                   [Coordinator Plan]              [FragmentExec → Data Nodes]
                              │                               │
                              ▼                               ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  STAGE 6: PHYSICAL OPTIMIZER                                                                    ┃
┃  ───────────────────────────                                                                    ┃
┃                                                                                                 ┃
┃  COORDINATOR (PhysicalPlanOptimizer):                                                           ┃
┃  • Minimal optimization - mainly projection pruning at boundaries                               ┃
┃                                                                                                 ┃
┃  DATA NODES (LocalLogicalPlanOptimizer + LocalPhysicalPlanOptimizer):                           ┃
┃  • FragmentExec unwrapped and locally optimized                                                 ┃
┃                                                                                                 ┃
┃  LOCAL LOGICAL OPTIMIZATION:                                                                    ┃
┃  • Additional filter pushdown with local index knowledge                                        ┃
┃  • Inference pushdown for ML models                                                             ┃
┃                                                                                                 ┃
┃  LOCAL PHYSICAL OPTIMIZATION (Lucene pushdown):                                                 ┃
┃  • FILTER → Lucene Query: status >= 400 → {"range":{"status":{"gte":400}}}                      ┃
┃  • TopN → Lucene sort + limit                                                                   ┃
┃  • COUNT(*) → Lucene doc count (no field extraction needed)                                     ┃
┃  • SAMPLE → random sampling at Lucene level                                                     ┃
┃  • FieldExtractExec inserted to load field values from doc values/stored fields                 ┃
┃  • Spatial operations pushed to Lucene where possible                                           ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                              │
                                              ▼
                                    Executable PhysicalPlan
                                    (ready for compute engine)
```

---

## Mental Model: The AST

ES|QL's planner is built on **immutable tree structures**. Understanding these trees is key to understanding the planner.

### Everything is a Node

At the foundation is `Node<T>`—an immutable tree node with children:

```java
public abstract class Node<T extends Node<T>> {
    private final Source source;      // Location in query text (line:column)
    private final List<T> children;   // Child nodes (immutable)

    // Traversal: visit nodes
    public void forEachDown(Consumer<T> action);  // Pre-order (parent first)
    public void forEachUp(Consumer<T> action);    // Post-order (children first)

    // Transformation: return new tree (or same if unchanged)
    public T transformDown(Function<T, T> rule);
    public T transformUp(Function<T, T> rule);
}
```

**Key insight**: Transformations don't mutate—they return a new tree. If nothing changed, the same tree is returned (structural sharing).

### Two Kinds of Trees

```
                    Node<T>
                       │
           ┌──────────┴──────────┐
           │                     │
      QueryPlan<P>          Expression
           │                     │
    ┌──────┴──────┐       ┌──────┴──────┐
    │             │       │             │
LogicalPlan  PhysicalPlan  Attribute   Function
                           Literal    Operator
```

**Plans** are the high-level operations: "filter rows", "aggregate by host", "sort by count".

**Expressions** live inside plans: the filter condition, the aggregation expression, the sort key.

### Plan Types: What vs How

| Aspect | LogicalPlan | PhysicalPlan |
|--------|-------------|--------------|
| **Represents** | User intent ("what") | Execution strategy ("how") |
| **Analogy** | "Get me from Denver to SF" | "Delta DEN→SJC, then SJC→SFO" |
| **Resolution** | Tracks resolved/unresolved | Always resolved |
| **Execution** | Abstract—no execution details | Concrete—which node runs what |
| **Lucene** | Unaware | Pushes operations to Lucene |
| **Network** | Unaware | Has Exchange nodes for data transfer |

**Why separate them?**
1. Logical optimization is algebraic (filter pushdown, constant folding)—doesn't need to know about Lucene
2. Physical optimization is implementation-specific—doesn't need to re-resolve types
3. Same logical plan could produce different physical plans on different clusters
4. Clean separation of concerns enables future federation

### Expression Types

Expressions form trees inside plan nodes:

```
Expression
│
├── NamedExpression (has name + unique NameId)
│   ├── Attribute (column reference)
│   │   ├── FieldAttribute      → field from index mapping
│   │   ├── ReferenceAttribute  → computed value reference
│   │   ├── MetadataAttribute   → _id, _index, _version
│   │   └── UnresolvedAttribute → not yet resolved
│   └── Alias (AS rename)
│
├── Literal (constant: 42, "hello", true)
│
├── UnaryExpression (-x, NOT x, IS NULL)
│
├── BinaryExpression (a + b, a AND b, a > b)
│
├── Function
│   ├── ScalarFunction    → ABS, CONCAT, TO_STRING (row-level)
│   ├── AggregateFunction → COUNT, SUM, AVG (group-level)
│   └── GroupingFunction  → BUCKET (grouping key)
│
└── Unresolved variants (before analysis)
```

### Attribute Identity

A subtle but important point: attributes are identified by `NameId`, not by name:

```java
// Two attributes with same name but different NameIds are DIFFERENT
FieldAttribute host1 = new FieldAttribute("host", nameId=123);
FieldAttribute host2 = new FieldAttribute("host", nameId=456);
host1.semanticEquals(host2);  // FALSE - different columns!

// Same NameId means same column, even if other properties differ
FieldAttribute host1a = host1.withNullability(FALSE);
host1.semanticEquals(host1a);  // TRUE - same NameId
```

This enables tracking columns through transformations, handling self-joins, and detecting when two references point to the same underlying column.

---

## The Rule System

Optimization happens via **rules** organized into **batches**:

```java
public abstract class Rule<E extends T, T extends Node<T>> {
    public abstract T apply(T tree);  // Transform the tree
}

public class Batch<T extends Node<T>> {
    private final String name;
    private final Rule<?, T>[] rules;
    private final Limiter limit;      // ONCE or DEFAULT (up to 100 iterations)
}
```

The `RuleExecutor` runs batches:

```java
for (Batch batch : batches) {
    do {
        for (Rule rule : batch.rules) {
            plan = rule.apply(plan);
        }
    } while (planChanged && !limitReached);
}
```

- **ONCE batches**: Run exactly once (substitutions, cleanup)
- **DEFAULT batches**: Run until no rule makes changes (fixed-point iteration)

---

## Marker Interfaces

Plan nodes and expressions implement marker interfaces to participate in specific pipeline phases:

| Interface | When Called | Purpose |
|-----------|-------------|---------|
| `Resolvable` | Analysis | Track resolution status |
| `TelemetryAware` | After analysis | Report usage statistics |
| `PostAnalysisVerificationAware` | After analysis | Self-validation |
| `PostAnalysisPlanVerificationAware` | After analysis | Tree-structure validation |
| `PostOptimizationVerificationAware` | After optimization | Late validation |
| `TranslationAware` | Physical optimization | Lucene query translation |
| `RewriteableAware` | Physical optimization | Query builder rewrite |

Example: `Filter` implements `PostAnalysisVerificationAware` to verify its condition is boolean:

```java
@Override
public void postAnalysisVerification(Failures failures) {
    if (condition.dataType() != BOOLEAN && condition.dataType() != NULL) {
        failures.add(fail(condition, "Condition must be boolean, found [{}]", condition.dataType()));
    }
}
```

---

## Quick Reference: What Happens Where

| Operation | Stage | Example |
|-----------|-------|---------|
| Parse query syntax | Parser | `"WHERE x > 1"` → `Filter[GreaterThan[x, 1]]` |
| Find index patterns | Pre-Analyzer | `"FROM logs"` → `indexes: {logs}` |
| Resolve field types | Analyzer | `x` → `FieldAttribute[x:integer]` |
| Resolve functions | Analyzer | `ABS(x)` → `Abs[x]` with return type |
| Add implicit LIMIT | Analyzer | No LIMIT → `Limit[1000]` |
| Fold constants | Logical Optimizer | `1 + 2` → `3` |
| Push filters down | Logical Optimizer | Filter above Eval → Filter below Eval |
| Combine filters | Logical Optimizer | `WHERE a` + `WHERE b` → `WHERE a AND b` |
| Expand AVG | Logical Optimizer | `AVG(x)` → `SUM(x)/COUNT(x)` |
| Create TopN | Logical Optimizer | `SORT x | LIMIT 10` → `TopN[x, 10]` |
| Split coordinator/data | Mapper | Insert `ExchangeExec` boundaries |
| Two-phase aggregation | Mapper | `AggregateExec[INITIAL]` + `AggregateExec[FINAL]` |
| Push to Lucene | Physical Optimizer | `Filter[x > 1]` → `query: {"range":...}` |
| Extract fields | Physical Optimizer | Insert `FieldExtractExec` |

---

## Next Steps

- **[Parser](./01-parser.md)** - How the query string becomes a plan
- **[Complete Example](./07-complete-example.md)** - Trace a real query through all stages
