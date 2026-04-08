<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Generator: Claude Code -->

# Stage 6: Physical Optimizer

**Input**: `PhysicalPlan` from Mapper
**Output**: Optimized `PhysicalPlan` (ready for execution)

Physical optimization happens at two levels:
1. **Coordinator**: Minimal optimization (projection pruning)
2. **Data Nodes**: Push operations into Lucene queries

This is where ES|QL's performance comes from—filters, sorts, and aggregations are pushed to Lucene where possible.

---

## Two Optimization Locations

```
┌──────────────────────────────────────────────────────────────────┐
│                        COORDINATOR                                │
│                                                                  │
│  PhysicalPlan from Mapper                                        │
│         │                                                        │
│         ▼                                                        │
│  PhysicalPlanOptimizer (minimal)                                 │
│         │                                                        │
│         ▼                                                        │
│  Optimized coordinator plan                                      │
│         │                                                        │
│         │ FragmentExec sent to data nodes                        │
└─────────┼────────────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────────────────────────────────────────────────────┐
│                        DATA NODE                                  │
│                                                                  │
│  FragmentExec (contains LogicalPlan)                             │
│         │                                                        │
│         ▼                                                        │
│  LocalLogicalPlanOptimizer                                       │
│         │                                                        │
│         ▼                                                        │
│  LocalPhysicalPlanOptimizer                                      │
│    • Push filters to Lucene queries                              │
│    • Push TopN to Lucene sort                                    │
│    • Insert field extraction                                     │
│         │                                                        │
│         ▼                                                        │
│  Executable physical operators                                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Coordinator Optimization

**File**: `optimizer/PhysicalPlanOptimizer.java`

Minimal optimization—the heavy lifting happens on data nodes:

```java
public class PhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, PhysicalOptimizerContext> {

    private static final List<Batch<PhysicalPlan>> RULES = List.of(
        new Batch<>("Plan Boundary", Limiter.ONCE,
            new ProjectAwayColumns()
        )
    );
}
```

### ProjectAwayColumns

Removes columns that won't be needed after an exchange boundary:

```
// Before: all columns flow through exchange
ExchangeExec
└── FragmentExec (produces: host, status, message, timestamp)

// If only 'host' is used afterward
ExchangeExec
└── ProjectExec[host]        ← Added to reduce data transfer
    └── FragmentExec
```

---

## Data Node Optimization

On each data node, the `FragmentExec` is unpacked and optimized locally.

### Local Logical Optimization

**File**: `optimizer/LocalLogicalPlanOptimizer.java`

Additional logical optimizations with local index knowledge:

```java
public class LocalLogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LocalLogicalOptimizerContext> {

    private static final List<Batch<LogicalPlan>> RULES = List.of(
        new Batch<>("Logical Optimization",
            new ReplaceTopNWithLimitAndSort(),
            new PushDownAndCombineFilters(),
            new PushDownEnrich(),
            new PushDownInference()  // ML inference pushdown
        )
    );
}
```

### Local Physical Optimization

**File**: `optimizer/LocalPhysicalPlanOptimizer.java`

This is where Lucene pushdown happens:

```java
public class LocalPhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, LocalPhysicalOptimizerContext> {

    protected static List<Batch<PhysicalPlan>> rules() {
        return List.of(
            new Batch<>("Push to ES",
                new ReplaceSourceAttributes(),
                new PushTopNToSource(),
                new PushLimitToSource(),
                new PushFiltersToSource(),
                new PushSampleToSource(),
                new PushStatsToSource(),
                new EnableSpatialDistancePushdown()
            ),
            new Batch<>("Field extraction", Limiter.ONCE,
                new InsertFieldExtraction(),
                new SpatialDocValuesExtraction()
            )
        );
    }
}
```

---

## Lucene Pushdown Rules

### PushFiltersToSource

Translates filter expressions to Lucene queries:

```java
public class PushFiltersToSource extends PhysicalOptimizerRule<FilterExec> {

    @Override
    protected PhysicalPlan rule(FilterExec filter) {
        PhysicalPlan child = filter.child();

        if (child instanceof EsQueryExec queryExec) {
            Expression condition = filter.condition();

            // Check if the expression can be translated to Lucene
            TranslationAware.Translatable translatable =
                TranslationAware.translatable(condition, lucenePushdownPredicates);

            if (translatable == Translatable.YES) {
                // Fully translatable - push entire filter to Lucene
                Query luceneQuery = translateToQuery(condition);
                return queryExec.withQuery(combineQueries(queryExec.query(), luceneQuery));
            }

            if (translatable == Translatable.RECHECK) {
                // Partially translatable - push to Lucene but keep filter for recheck
                Query luceneQuery = translateToQuery(condition);
                return filter.replaceChild(
                    queryExec.withQuery(combineQueries(queryExec.query(), luceneQuery))
                );
            }

            // Not translatable - keep filter as-is
        }
        return filter;
    }
}
```

#### What Gets Pushed

| Expression | Lucene Query | Notes |
|------------|--------------|-------|
| `field = value` | `TermQuery` | Exact match |
| `field > value` | `RangeQuery` | Range queries |
| `field >= value` | `RangeQuery` | Inclusive range |
| `field LIKE "pat*"` | `WildcardQuery` | Wildcard patterns |
| `field RLIKE "regex"` | `RegexpQuery` | Regex patterns |
| `field IN (a, b, c)` | `TermsQuery` | Multiple values |
| `field IS NULL` | `ExistsQuery` (negated) | Null check |
| `a AND b` | `BooleanQuery (must)` | Boolean AND |
| `a OR b` | `BooleanQuery (should)` | Boolean OR |
| `NOT a` | `BooleanQuery (must_not)` | Boolean NOT |
| `MATCH(field, text)` | `MatchQuery` | Full-text search |
| `geo_field WITHIN bounds` | `GeoShapeQuery` | Spatial queries |

**Example transformation**:

```
// Before
FilterExec[status >= 400 AND host = "web1"]
└── EsQueryExec[logs, query=match_all]

// After
EsQueryExec[logs, query={
  "bool": {
    "must": [
      {"range": {"status": {"gte": 400}}},
      {"term": {"host": "web1"}}
    ]
  }
}]
```

### TranslationAware Interface

Expressions implement `TranslationAware` to participate in Lucene pushdown:

```java
public interface TranslationAware {

    // Can this be translated to Lucene?
    Translatable translatable(LucenePushdownPredicates predicates);

    // Translate to ES query DSL
    Query asQuery(LucenePushdownPredicates predicates, TranslatorHandler handler);

    enum Translatable {
        NO,           // Can't push to Lucene (keep filter operator)
        YES,          // Fully push to Lucene (remove filter operator)
        RECHECK,      // Push to Lucene but keep filter for verification
        YES_BUT_RECHECK_NEGATED  // Push, but recheck if negated
    }
}
```

**Why RECHECK?**

Some expressions can be pushed to Lucene but need verification:
- Field without doc values: Lucene can find candidates but can't verify single-value semantics
- Text field with keyword subfield: Lucene query on keyword may miss ignored fields

### PushTopNToSource

Pushes sorting and limiting to Lucene:

```java
// Before
TopNExec[timestamp DESC, 100]
└── EsQueryExec[logs, query={...}]

// After
EsQueryExec[logs,
  query={...},
  sort=[{"timestamp": {"order": "desc"}}],
  limit=100
]
```

This is huge for performance—Lucene can use its efficient top-N collection instead of materializing all rows.

### PushLimitToSource

Pushes limits to Lucene:

```java
// Before
LimitExec[100]
└── EsQueryExec[logs, query={...}]

// After
EsQueryExec[logs, query={...}, limit=100]
```

### PushStatsToSource

Optimizes `COUNT(*)` to Lucene doc count:

```java
// Before
AggregateExec[COUNT(*), mode=SINGLE]
└── EsQueryExec[logs, query=match_all]

// After
EsStatsQueryExec[logs, stats=[COUNT], query=match_all]
// No need to read any field values - just count matching docs
```

---

## Field Extraction

### InsertFieldExtraction

After filter pushdown, we need to actually load field values for remaining operations:

```java
public class InsertFieldExtraction extends PhysicalOptimizerRule<PhysicalPlan> {

    @Override
    protected PhysicalPlan rule(PhysicalPlan plan) {
        // Find what attributes are needed
        AttributeSet needed = plan.references();

        // Find the EsQueryExec
        return plan.transformDown(EsQueryExec.class, queryExec -> {
            // Determine which fields to extract
            List<Attribute> toExtract = needed.stream()
                .filter(a -> a instanceof FieldAttribute)
                .toList();

            if (toExtract.isEmpty()) {
                return queryExec;
            }

            // Insert FieldExtractExec
            return new FieldExtractExec(queryExec.source(), queryExec, toExtract);
        });
    }
}
```

**Example**:

```
// Before
ProjectExec[host, status]
└── EsQueryExec[logs, query={...}]

// After
ProjectExec[host, status]
└── FieldExtractExec[host, status]   ← Loads field values
    └── EsQueryExec[logs, query={...}]  ← Returns doc IDs only
```

### Field Loading Strategy

`FieldExtractExec` decides how to load each field:

| Field Type | Strategy | Notes |
|------------|----------|-------|
| doc_values enabled | Doc values | Fast columnar access |
| stored field | Stored fields | Slower, row-based |
| _source only | Source extraction | Slowest, parse JSON |

---

## Complete Example

**Optimized Logical Plan** (from Stage 4):

```
TopN[timestamp DESC, 10]
└── Aggregate[COUNT(*) BY host]
    └── Filter[status >= 400]
        └── EsRelation[logs]
```

**After Mapping** (Stage 5):

```
TopNExec[timestamp DESC, 10]
└── AggregateExec[FINAL]
    └── ExchangeExec
        └── FragmentExec
            └── TopN[...]
                └── Aggregate[INITIAL]
                    └── Filter[status >= 400]
                        └── EsRelation[logs]
```

**After Physical Optimization** (Data Node):

```
TopNExec[timestamp DESC, 10]                    ← Local TopN
└── AggregateExec[INITIAL]                       ← Partial aggregation
    └── FieldExtractExec[host]                   ← Load host for grouping
        └── EsQueryExec[logs,
              query={"range":{"status":{"gte":400}}},  ← Filter pushed!
              limit=null                               ← No limit (agg needs all)
            ]
```

**What Lucene Does**:
1. Execute range query on `status` field
2. For each matching doc, extract `host` from doc values
3. ESQL computes partial COUNT(*) grouped by host
4. Results sent to coordinator

---

## When Pushdown Doesn't Happen

Some cases prevent Lucene pushdown:

| Scenario | Why |
|----------|-----|
| Function on field: `WHERE LENGTH(name) > 5` | Lucene can't evaluate arbitrary functions |
| Field without index: `WHERE runtime_field = 'x'` | No inverted index to query |
| Cross-field comparison: `WHERE a > b` | Lucene indexes fields separately |
| Multivalue semantics: `WHERE MV_COUNT(tags) > 3` | Lucene doesn't track MV counts |

In these cases, the filter stays as a `FilterExec` (compute engine handles it).

---

## Files

| File | Purpose |
|------|---------|
| `optimizer/PhysicalPlanOptimizer.java` | Coordinator physical optimizer |
| `optimizer/LocalLogicalPlanOptimizer.java` | Data node logical optimizer |
| `optimizer/LocalPhysicalPlanOptimizer.java` | Data node physical optimizer |
| `optimizer/rules/physical/local/PushFiltersToSource.java` | Filter → Lucene |
| `optimizer/rules/physical/local/PushTopNToSource.java` | TopN → Lucene sort |
| `optimizer/rules/physical/local/InsertFieldExtraction.java` | Add field loading |
| `capabilities/TranslationAware.java` | Lucene translation interface |
| `planner/TranslatorHandler.java` | Expression → Query translation |

---

## Next: [Complete Example](./07-complete-example.md)

Let's trace a real query through all six stages to see how it transforms at each step.
