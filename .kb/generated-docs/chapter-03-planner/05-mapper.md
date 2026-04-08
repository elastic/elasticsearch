<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Generator: Claude Code -->

# Stage 5: Mapper

**Input**: Optimized `LogicalPlan`
**Output**: `PhysicalPlan` (coordinator perspective)

The mapper translates the logical plan ("what") into a physical plan ("how"). This is where decisions are made about what runs on data nodes vs. the coordinator, and where data transfer boundaries are established.

---

## The Core Question

For each operation in the logical plan, the mapper decides:

1. **Where does this run?** Data nodes, coordinator, or both?
2. **Does this break the pipeline?** Does data need to be collected before continuing?
3. **How do we split aggregations?** INITIAL phase on data nodes, FINAL on coordinator?

---

## Key Concepts

### FragmentExec: Work for Data Nodes

A `FragmentExec` wraps a logical plan subtree that will be sent to data nodes:

```java
public class FragmentExec extends LeafExec {
    private final LogicalPlan fragment;  // Logical plan to execute remotely

    // This fragment will be:
    // 1. Sent to each data node holding relevant shards
    // 2. Locally optimized on each data node
    // 3. Converted to physical operators
    // 4. Executed against local shards
}
```

### ExchangeExec: Data Transfer Boundary

An `ExchangeExec` marks where data flows between execution contexts:

```java
public class ExchangeExec extends UnaryExec {
    private final boolean inBetweenAggs;  // True if between INITIAL and FINAL aggregation

    // Data flows:
    // - From data nodes to coordinator (results collection)
    // - Between aggregation phases (partial results)
}
```

### Pipeline Breakers

Some operations require all input before producing output:

| Operation | Why It Breaks Pipeline |
|-----------|------------------------|
| Aggregate | Need all rows to compute final result |
| TopN | Need all rows to find top N |
| Limit | Need to coordinate to avoid over-fetching |

"Streaming" operations (Filter, Eval, Project) can process rows one at a time and stay inside the fragment.

---

## How Mapping Works

**File**: `planner/mapper/Mapper.java`

```java
public class Mapper {

    public PhysicalPlan map(LogicalPlan plan) {
        return mapPlan(plan);
    }

    private PhysicalPlan mapPlan(LogicalPlan p) {
        if (p instanceof LeafPlan leaf) {
            return mapLeaf(leaf);
        }
        if (p instanceof UnaryPlan unary) {
            return mapUnary(unary);
        }
        if (p instanceof BinaryPlan binary) {
            return mapBinary(binary);
        }
        throw new UnsupportedOperationException("Unknown plan: " + p);
    }

    private PhysicalPlan mapLeaf(LeafPlan leaf) {
        if (leaf instanceof EsRelation) {
            // Data source becomes a fragment to send to data nodes
            return new FragmentExec(leaf);
        }
        if (leaf instanceof LocalRelation local) {
            return new LocalSourceExec(local);
        }
        return MapperUtils.mapLeaf(leaf);
    }

    private PhysicalPlan mapUnary(UnaryPlan unary) {
        PhysicalPlan child = mapPlan(unary.child());

        // Keep streaming operations inside fragments
        if (child instanceof FragmentExec fragment) {
            if (isStreamingOperation(unary)) {
                // Just extend the fragment with this operation
                return new FragmentExec(unary.replaceChild(fragment.fragment()));
            }
        }

        // Pipeline breakers need special handling
        if (unary instanceof Aggregate agg) {
            return mapAggregate(agg, child);
        }
        if (unary instanceof TopN topN) {
            return mapTopN(topN, child);
        }
        if (unary instanceof Limit limit) {
            return mapLimit(limit, child);
        }

        // Default: just wrap the logical operation
        return MapperUtils.map(unary, child);
    }

    private boolean isStreamingOperation(UnaryPlan plan) {
        return plan instanceof Filter
            || plan instanceof Eval
            || plan instanceof Project;
    }
}
```

### Mapping Aggregates

Aggregations become two-phase:

```java
private PhysicalPlan mapAggregate(Aggregate agg, PhysicalPlan child) {
    // If child is a fragment, add exchange to get data from data nodes
    if (child instanceof FragmentExec fragment) {
        // Put INITIAL aggregation inside the fragment
        LogicalPlan withInitialAgg = agg.replaceChild(fragment.fragment());
        child = new FragmentExec(withInitialAgg);

        // Add exchange to collect partial results
        child = new ExchangeExec(child.source(), child, true);  // inBetweenAggs=true
    }

    // FINAL aggregation on coordinator
    return new AggregateExec(
        agg.source(),
        child,
        agg.groupings(),
        agg.aggregates(),
        AggregatorMode.FINAL
    );
}
```

### Mapping TopN

TopN also needs coordination:

```java
private PhysicalPlan mapTopN(TopN topN, PhysicalPlan child) {
    if (child instanceof FragmentExec fragment) {
        // Local TopN on each data node (reduces data transfer)
        LogicalPlan withLocalTopN = topN.replaceChild(fragment.fragment());
        child = new FragmentExec(withLocalTopN);

        // Exchange to collect results
        child = new ExchangeExec(child.source(), child, false);
    }

    // Final TopN on coordinator (merge sorted results)
    return new TopNExec(topN.source(), child, topN.orders(), topN.limit());
}
```

---

## Example: Mapping a Query

**Logical Plan**:

```
TopN[count DESC, 10]
└── Aggregate[COUNT(*) AS count BY host]
    └── Filter[status >= 400]
        └── EsRelation[logs]
```

**Physical Plan After Mapping**:

```
TopNExec[count DESC, 10]                          ← Coordinator: final TopN
└── ExchangeExec[inBetweenAggs=false]             ← Collect from data nodes
    └── AggregateExec[FINAL]                      ← Coordinator: final aggregation
        └── ExchangeExec[inBetweenAggs=true]      ← Collect partial aggregates
            └── FragmentExec ─────────────────────────────────────────┐
                │                                                     │
                │  LogicalPlan to execute on each data node:          │
                │                                                     │
                │  TopN[count DESC, 10]           ← Local TopN        │
                │  └── Aggregate[COUNT(*) BY host] ← INITIAL agg     │
                │      └── Filter[status >= 400]                     │
                │          └── EsRelation[logs]                       │
                └─────────────────────────────────────────────────────┘
```

### What Runs Where

**On Each Data Node**:
1. Read matching documents from local shards
2. Filter by `status >= 400`
3. Compute partial `COUNT(*)` grouped by `host` (INITIAL mode)
4. Apply local TopN to reduce data (only keep top 10 per node)
5. Send partial results to coordinator

**On Coordinator**:
1. Receive partial aggregates from all data nodes
2. Combine partial counts into final counts (FINAL mode)
3. Apply final TopN across all results
4. Return top 10 to client

---

## Two-Phase Aggregation

Aggregations are split into INITIAL and FINAL modes:

| Aggregation | INITIAL (Data Node) | FINAL (Coordinator) |
|-------------|---------------------|---------------------|
| COUNT | Count local rows | Sum partial counts |
| SUM | Sum local values | Sum partial sums |
| AVG | (expanded to SUM/COUNT) | (expanded to SUM/COUNT) |
| MIN | Find local min | Min of partial mins |
| MAX | Find local max | Max of partial maxes |
| COUNT_DISTINCT | Build local HLL sketch | Merge HLL sketches |

This is why AVG is expanded to SUM/COUNT in the optimizer—you can't combine partial averages correctly.

---

## Exchange Types

The `inBetweenAggs` flag affects how data is exchanged:

| inBetweenAggs | Purpose | Data Format |
|---------------|---------|-------------|
| `true` | Between INITIAL and FINAL aggregation | Partial aggregate state |
| `false` | Collecting results for TopN/Limit | Row data |

---

## Binary Plans: Joins

For joins (like LOOKUP), both sides need to be mapped:

```java
private PhysicalPlan mapBinary(BinaryPlan binary) {
    PhysicalPlan left = mapPlan(binary.left());
    PhysicalPlan right = mapPlan(binary.right());

    if (binary instanceof LookupJoin join) {
        // Lookup table is broadcast to data nodes
        return new LookupJoinExec(join.source(), left, right, join.joinKey());
    }

    return MapperUtils.mapBinary(binary, left, right);
}
```

---

## Local Relations

If the plan involves only local data (like `ROW 1, 2, 3`), no fragments are created:

```java
// ROW becomes LocalRelation in optimizer
// Mapper converts it to LocalSourceExec

LocalRelation[[a=1, b=2]]

// Maps to
LocalSourceExec[[a=1, b=2]]  // Executed entirely on coordinator
```

---

## Files

| File | Purpose |
|------|---------|
| `planner/mapper/Mapper.java` | Main mapping logic |
| `planner/mapper/MapperUtils.java` | Helper methods |
| `plan/physical/FragmentExec.java` | Logical plan fragment |
| `plan/physical/ExchangeExec.java` | Data transfer boundary |
| `plan/physical/AggregateExec.java` | Aggregation execution |
| `plan/physical/TopNExec.java` | TopN execution |

---

## Next: [Physical Optimizer](./06-physical-optimizer.md)

The physical plan is now ready for final optimization, where operations are pushed into Lucene queries.
