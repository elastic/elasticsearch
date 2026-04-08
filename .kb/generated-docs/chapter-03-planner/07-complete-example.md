<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Generator: Claude Code -->

# Complete Example: Tracing a Query

This document traces a representative ES|QL query through all six planner stages, showing exactly what happens at each step.

---

## The Query

```esql
FROM logs-*
| WHERE status >= 400 AND host LIKE "web*"
| EVAL error_type = CASE(status >= 500, "server", "client")
| STATS error_count = COUNT(*), latest = MAX(@timestamp) BY error_type, host
| SORT error_count DESC
| LIMIT 20
```

**What it does**: Find the top 20 host/error-type combinations by error count in the logs, including the most recent error timestamp for each.

---

## Stage 1: Parser

**Input**: Query string

The parser tokenizes and builds an unresolved AST:

```
Limit[20]
└── OrderBy[error_count DESC]
    └── Aggregate[
          COUNT(*) AS error_count,
          MAX(@timestamp) AS latest
          BY error_type, host
        ]
        └── Eval[error_type = CASE(status >= 500, "server", "client")]
            └── Filter[
                  AND[
                    GreaterThanOrEqual[UnresolvedAttribute("status"), Literal(400)],
                    Like[UnresolvedAttribute("host"), "web*"]
                  ]
                ]
                └── UnresolvedRelation["logs-*"]
```

**Key observations**:
- All field references are `UnresolvedAttribute`
- The index pattern is `UnresolvedRelation`
- Functions like `COUNT`, `MAX`, `CASE` are `UnresolvedFunction`
- Literals (400, 500, 20, "server", "client", "web*") are resolved

---

## Stage 2: Pre-Analyzer

**Input**: Unresolved LogicalPlan

The pre-analyzer scans for external references:

```java
PreAnalysis {
    indices: {
        "logs-*" → STANDARD
    },
    enriches: [],
    lookupIndices: [],
    useAggregateMetricDoubleWhenNotSupported: false,
    useDenseVectorWhenNotSupported: false
}
```

**What happens next**: `EsqlSession` calls the FieldCaps API for `logs-*` to get the merged mapping across all matching indices.

**Resolved mapping** (example):
```json
{
  "status": {"type": "integer"},
  "host": {"type": "keyword"},
  "@timestamp": {"type": "date"},
  "message": {"type": "text"}
}
```

---

## Stage 3: Analyzer

**Input**: Unresolved LogicalPlan + Index Resolution

### After ResolveTable

`UnresolvedRelation["logs-*"]` becomes:

```
EsRelation["logs-*"][
  FieldAttribute("status", INTEGER),
  FieldAttribute("host", KEYWORD),
  FieldAttribute("@timestamp", DATETIME),
  FieldAttribute("message", TEXT)
]
```

### After ResolveRefs

Field references resolve against available attributes:

```
// status → FieldAttribute("status", INTEGER)
// host → FieldAttribute("host", KEYWORD)
// @timestamp → FieldAttribute("@timestamp", DATETIME)
```

### After ResolveFunctions

```
// COUNT(*) → Count[Literal(*)]
// MAX(@timestamp) → Max[@timestamp:DATETIME]
// CASE(...) → Case[conditions, values]
```

### After ImplicitCasting

The CASE function returns KEYWORD (both branches are strings), no casts needed.

### After AddImplicitLimit

No change—we already have `LIMIT 20`.

### Final Analyzed Plan

```
Limit[Literal(20, INTEGER)]
└── OrderBy[error_count:LONG DESC]
    └── Aggregate[
          Count[*] AS error_count:LONG,
          Max[@timestamp:DATETIME] AS latest:DATETIME
          BY error_type:KEYWORD, host:KEYWORD
        ]
        └── Eval[error_type:KEYWORD = Case[
              GreaterThanOrEqual[status:INTEGER, 500:INTEGER] → "server":KEYWORD,
              DEFAULT → "client":KEYWORD
            ]]
            └── Filter[And[
                  GreaterThanOrEqual[status:INTEGER, 400:INTEGER],
                  Like[host:KEYWORD, "web*"]
                ]]
                └── EsRelation["logs-*"][status:INTEGER, host:KEYWORD, @timestamp:DATETIME, ...]
```

**Key observations**:
- Every expression now has a known `DataType`
- All attributes are `FieldAttribute` with types
- Functions are concrete implementations

### Verification

The verifier checks:
- ✓ Filter condition is BOOLEAN (AND of comparisons)
- ✓ Aggregation has valid grouping keys
- ✓ CASE branches return compatible types

---

## Stage 4: Logical Optimizer

### Substitutions Batch

**No surrogates to expand** (no AVG, no LOOKUP)

### Operators Batch

#### ConstantFolding

No pure constant expressions to fold. (400, 500 are already literals)

#### PushDownAndCombineFilters

The filter is already at the bottom—can't push past EsRelation.

#### PruneColumns

Only needed columns are kept:

```
// Used: status (filter, CASE), host (filter, grouping), @timestamp (MAX)
// Pruned: message (never used)

EsRelation["logs-*"][status:INTEGER, host:KEYWORD, @timestamp:DATETIME]
```

### Cleanup Batch

#### ReplaceLimitAndSortAsTopN

```
// Before
Limit[20]
└── OrderBy[error_count DESC]

// After
TopN[error_count DESC, 20]
```

### Final Optimized Plan

```
TopN[error_count DESC, 20]
└── Aggregate[
      Count[*] AS error_count,
      Max[@timestamp] AS latest
      BY error_type, host
    ]
    └── Eval[error_type = Case[status >= 500 → "server", DEFAULT → "client"]]
        └── Filter[status >= 400 AND host LIKE "web*"]
            └── EsRelation["logs-*"][status, host, @timestamp]
```

---

## Stage 5: Mapper

The mapper decides what runs where:

### Mapping EsRelation

`EsRelation` → `FragmentExec` (send to data nodes)

### Streaming Operations Stay in Fragment

Filter and Eval are streaming—they stay inside the fragment.

### Aggregate Becomes Two-Phase

```
Aggregate[INITIAL] on data nodes → ExchangeExec → Aggregate[FINAL] on coordinator
```

### TopN Also Distributed

Local TopN on data nodes (reduce data), final TopN on coordinator.

### Physical Plan After Mapping

```
TopNExec[error_count DESC, 20]                      ← COORDINATOR: final TopN
└── ExchangeExec[inBetweenAggs=false]               ← Collect results
    └── AggregateExec[FINAL, Count, Max BY error_type, host]  ← COORDINATOR: merge
        └── ExchangeExec[inBetweenAggs=true]        ← Collect partial aggregates
            └── FragmentExec ─────────────────────────────────────────────────┐
                │                                                             │
                │  TopN[error_count DESC, 20]           ← Local TopN          │
                │  └── Aggregate[INITIAL, Count, Max BY error_type, host]     │
                │      └── Eval[error_type = CASE(...)]                       │
                │          └── Filter[status >= 400 AND host LIKE "web*"]     │
                │              └── EsRelation["logs-*"]                       │
                └─────────────────────────────────────────────────────────────┘
```

---

## Stage 6: Physical Optimizer

### Coordinator Optimization

**ProjectAwayColumns**: No changes needed—columns are already minimal.

### Data Node Optimization

The `FragmentExec` is sent to each data node holding shards of `logs-*`.

On each data node:

#### Local Logical Optimization

No additional changes.

#### PushFiltersToSource

The filter `status >= 400 AND host LIKE "web*"` is pushed to Lucene:

```java
// status >= 400
TranslationAware.translatable(GreaterThanOrEqual) → YES

// host LIKE "web*"
TranslationAware.translatable(Like) → YES

// Combined
BoolQuery {
  must: [
    RangeQuery { field: "status", gte: 400 },
    WildcardQuery { field: "host", value: "web*" }
  ]
}
```

#### InsertFieldExtraction

Fields needed after the Lucene query:
- `status` (for CASE condition)
- `host` (for grouping)
- `@timestamp` (for MAX aggregation)

#### Final Data Node Plan

```
TopNExec[error_count DESC, 20]
└── AggregateExec[INITIAL, Count, Max BY error_type, host]
    └── EvalExec[error_type = CASE(status >= 500, "server", "client")]
        └── FieldExtractExec[status, host, @timestamp]
            └── EsQueryExec["logs-*",
                  query = {
                    "bool": {
                      "must": [
                        {"range": {"status": {"gte": 400}}},
                        {"wildcard": {"host": {"value": "web*"}}}
                      ]
                    }
                  }
                ]
```

---

## Execution Flow

### On Each Data Node

```
1. EsQueryExec executes Lucene query
   └── Returns: doc IDs matching status >= 400 AND host LIKE "web*"

2. FieldExtractExec loads field values
   └── For each doc: {status: 503, host: "web-01", @timestamp: "2026-02-05T10:30:00Z"}

3. EvalExec computes error_type
   └── status=503 >= 500? → error_type = "server"

4. AggregateExec[INITIAL] computes partial aggregates
   └── For group (error_type="server", host="web-01"):
       - partial_count = 47
       - partial_max_timestamp = "2026-02-05T10:30:00Z"

5. TopNExec keeps top 20 groups by partial_count
   └── Reduces data before sending to coordinator
```

### On Coordinator

```
1. ExchangeExec collects partial aggregates from all data nodes
   └── Receives: [(server, web-01, 47, ts1), (client, web-02, 123, ts2), ...]

2. AggregateExec[FINAL] merges partial results
   └── For each unique (error_type, host):
       - error_count = SUM of partial counts
       - latest = MAX of partial timestamps

3. TopNExec selects top 20 by error_count DESC
   └── Final result
```

---

## Summary: What Happened Where

| Stage | Key Transformation |
|-------|-------------------|
| **Parser** | Query string → Unresolved tree |
| **Pre-Analyzer** | Found index pattern `logs-*` |
| **Analyzer** | Resolved all fields, functions, types |
| **Logical Optimizer** | Combined Sort+Limit → TopN, pruned unused columns |
| **Mapper** | Split into coordinator/data node execution, two-phase aggregation |
| **Physical Optimizer** | Pushed filter to Lucene query, added field extraction |

### Performance Wins

1. **Lucene filter pushdown**: Only matching documents are processed
2. **Early field pruning**: Only 3 fields extracted instead of all
3. **Local TopN**: Each data node sends at most 20 groups
4. **Two-phase aggregation**: Aggregates computed locally before transfer
5. **Combined TopN**: Sort+Limit as single efficient operation

### What Would Hurt Performance

If we couldn't push to Lucene (e.g., `WHERE LENGTH(host) > 5`):
- Full table scan
- All documents processed by compute engine
- Much higher CPU and memory usage

---

## Visual Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ FROM logs-* | WHERE status >= 400 AND host LIKE "web*"                      │
│ | EVAL error_type = CASE(status >= 500, "server", "client")                 │
│ | STATS error_count = COUNT(*), latest = MAX(@timestamp) BY error_type, host│
│ | SORT error_count DESC | LIMIT 20                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                            ┌───────────┴───────────┐
                            ▼                       ▼
                    ┌──────────────┐        ┌──────────────┐
                    │  Data Node 1 │        │  Data Node 2 │
                    │              │        │              │
                    │ Lucene Query │        │ Lucene Query │
                    │      ↓       │        │      ↓       │
                    │ Extract flds │        │ Extract flds │
                    │      ↓       │        │      ↓       │
                    │ EVAL + STATS │        │ EVAL + STATS │
                    │   (partial)  │        │   (partial)  │
                    │      ↓       │        │      ↓       │
                    │  Local Top20 │        │  Local Top20 │
                    └──────┬───────┘        └──────┬───────┘
                           │                       │
                           └───────────┬───────────┘
                                       ▼
                            ┌──────────────────┐
                            │   Coordinator    │
                            │                  │
                            │  Merge partials  │
                            │       ↓          │
                            │  Final STATS     │
                            │       ↓          │
                            │  Global Top20    │
                            │       ↓          │
                            │    Results       │
                            └──────────────────┘
```

---

## Back to [Overview](./00-overview.md)
