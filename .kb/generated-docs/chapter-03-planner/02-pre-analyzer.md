<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Generator: Claude Code -->

# Stage 2: Pre-Analyzer

**Input**: Unresolved `LogicalPlan`
**Output**: `PreAnalysis` record (references to resolve)

The pre-analyzer scans the unresolved plan to discover what external resources need to be fetched before analysis can proceed. It's like a "linker" that finds all the external references.

---

## Why Pre-Analysis?

Before we can resolve `UnresolvedRelation("logs")` to `EsRelation`, we need to know:
- Does the index `logs` exist?
- What fields does it have?
- What are their types?

This requires calling the **FieldCaps API**—an async operation. The pre-analyzer identifies all such references so they can be resolved in batch before analysis.

---

## What Gets Discovered

| Reference Type | Source | Resolution Needed |
|----------------|--------|-------------------|
| Index patterns | `FROM logs`, `FROM logs-*` | FieldCaps API → field mappings |
| Enrich policies | `ENRICH hosts ON ip` | Enrich policy lookup |
| Lookup indices | `LOOKUP users ON user_id` | FieldCaps API (in LOOKUP mode) |
| Special field types | Aggregate metrics, dense vectors | Feature flag checks |

---

## The PreAnalysis Record

**File**: `analysis/PreAnalyzer.java`

```java
public record PreAnalysis(
    Map<IndexPattern, IndexMode> indices,       // Main indices to query
    List<Enrich> enriches,                      // Enrich policy references
    List<IndexPattern> lookupIndices,           // LOOKUP table indices
    boolean useAggregateMetricDoubleWhenNotSupported,
    boolean useDenseVectorWhenNotSupported
) {
    public static PreAnalysis EMPTY = new PreAnalysis(Map.of(), List.of(), List.of(), false, false);
}
```

---

## How It Works

```java
public class PreAnalyzer {

    public PreAnalysis preAnalyze(LogicalPlan plan) {
        Map<IndexPattern, IndexMode> indices = new LinkedHashMap<>();
        List<IndexPattern> lookupIndices = new ArrayList<>();
        List<Enrich> unresolvedEnriches = new ArrayList<>();

        // Find all UnresolvedRelation nodes
        plan.forEachUp(UnresolvedRelation.class, relation -> {
            IndexPattern pattern = relation.indexPattern();
            IndexMode mode = relation.indexMode();

            if (mode == IndexMode.LOOKUP) {
                lookupIndices.add(pattern);
            } else {
                indices.put(pattern, mode);
            }
        });

        // Find all Enrich nodes
        plan.forEachUp(Enrich.class, unresolvedEnriches::add);

        // Check for special field type requirements
        boolean needsAggMetric = checkForAggregateMetricDouble(plan);
        boolean needsDenseVector = checkForDenseVector(plan);

        return new PreAnalysis(indices, unresolvedEnriches, lookupIndices,
                               needsAggMetric, needsDenseVector);
    }
}
```

---

## Example

Query: `FROM logs | ENRICH hosts ON ip | LOOKUP users ON user_id`

### Unresolved Plan

```
LookupJoin[user_id]
├── Enrich[policy=hosts, matchField=ip]
│   └── UnresolvedRelation["logs"]
└── UnresolvedRelation["users", mode=LOOKUP]
```

### PreAnalysis Output

```java
PreAnalysis {
    indices: {
        "logs" → STANDARD
    },
    enriches: [
        Enrich(policy="hosts", matchField="ip")
    ],
    lookupIndices: [
        "users"
    ],
    useAggregateMetricDoubleWhenNotSupported: false,
    useDenseVectorWhenNotSupported: false
}
```

---

## What EsqlSession Does With This

The `EsqlSession` uses the `PreAnalysis` to make async calls:

```java
// In EsqlSession.execute()

PreAnalysis preAnalysis = preAnalyzer.preAnalyze(parsed);

// 1. Resolve main indices via FieldCaps
indexResolver.resolveAsMergedMapping(
    preAnalysis.indices().keySet(),
    listener.delegateResponse((indexResolution, e) -> {

        // 2. Resolve enrich policies
        enrichPolicyResolver.resolvePolicies(
            preAnalysis.enriches(),
            listener.delegateResponse((enrichResolution, e2) -> {

                // 3. Resolve lookup tables
                lookupResolver.resolveLookupTables(
                    preAnalysis.lookupIndices(),
                    listener.delegateResponse((lookupResolution, e3) -> {

                        // NOW we can analyze with all resolutions
                        LogicalPlan analyzed = analyzer.analyze(
                            parsed,
                            indexResolution,
                            enrichResolution,
                            lookupResolution
                        );
                    })
                );
            })
        );
    })
);
```

---

## Index Modes

The `IndexMode` affects how the index is treated:

| Mode | Description | Used By |
|------|-------------|---------|
| `STANDARD` | Normal index | `FROM logs` |
| `TIME_SERIES` | TSDB index with @timestamp | `FROM metrics` (TSDB) |
| `LOOKUP` | Lookup table for joins | `LOOKUP users` |

---

## Enrich Policy Resolution

For `ENRICH hosts ON ip`, the resolver needs to find:

1. The enrich policy definition (`hosts`)
2. The backing enrich index (`.enrich-hosts-*`)
3. The match field type
4. The enrich fields and their types

This becomes an `EnrichResolution` that the analyzer uses.

---

## Files

| File | Purpose |
|------|---------|
| `analysis/PreAnalyzer.java` | Scans plan for external references |
| `session/EsqlSession.java` | Orchestrates async resolution |
| `session/IndexResolver.java` | Resolves index mappings |
| `enrich/EnrichPolicyResolver.java` | Resolves enrich policies |

---

## Next: [Analyzer](./03-analyzer.md)

With all external references resolved, the analyzer can now resolve the plan—turning unresolved nodes into resolved ones with full type information.
