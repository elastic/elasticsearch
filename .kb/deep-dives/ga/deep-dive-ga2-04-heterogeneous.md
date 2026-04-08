# Deep Dive: Heterogeneous Queries (Cross-Source Federation)

## Summary

Heterogeneous queries -- where a single ESQL query touches both Elasticsearch indices and external data sources -- are **not supported today**. The planner, mapper, and execution engine all assume uniform source types within a single execution path. Supporting cross-source federation would require changes at every layer: parser, planner, optimizer, execution dispatcher, and operator coordination.

---

## 1. Plan Node Types: EsRelation vs ExternalRelation

### EsRelation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/EsRelation.java`

```
Line 27: public class EsRelation extends LeafPlan {
```

- Extends `LeafPlan` (which extends `LogicalPlan`)
- Represents an Elasticsearch index with fields: `indexPattern`, `indexMode`, `originalIndices`, `concreteIndices`, `indexNameWithModes`, `attrs`
- Is serializable (implements `writeTo`/`readFrom` for cross-node transport)
- Does NOT implement `ExecutesOn` -- it is implicitly a "data node" plan

### ExternalRelation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java`

```
Line 46: public class ExternalRelation extends LeafPlan implements ExecutesOn.Coordinator {
```

- Extends `LeafPlan` (same parent as `EsRelation`)
- Implements `ExecutesOn.Coordinator` -- explicitly marked as coordinator-only
- Holds `sourcePath`, `SourceMetadata`, and `FileSet`
- Is NOT serializable (line 76-77: `writeTo` throws `UnsupportedOperationException`)
- Has a `toPhysicalExec()` method (line 115) that creates an `ExternalSourceExec` directly

### Can They Coexist in a Plan Tree?

Both extend `LeafPlan`, so they are structurally compatible as children of the same tree. However:
- They are **never combined in practice** because `EXTERNAL` and `FROM` are separate `sourceCommand` alternatives in the grammar
- The only way to get both in a tree today would be through `FROM a, (subquery)` syntax (UnionAll), but subqueries always parse as `FROM` commands, not `EXTERNAL`

### LeafPlan Hierarchy

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/LeafPlan.java`

```
Line 15: public abstract class LeafPlan extends LogicalPlan {
```

Both `EsRelation` and `ExternalRelation` share the same abstract parent.

---

## 2. Physical Planning Fork: Where ES and External Paths Diverge

### Mapper (Coordinator-side logical-to-physical mapping)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

```java
// Line 85-93:
private PhysicalPlan mapLeaf(LeafPlan leaf) {
    if (leaf instanceof EsRelation esRelation) {
        return new FragmentExec(esRelation);  // ES path: wrap in FragmentExec
    }
    // ExternalRelation is handled by MapperUtils.mapLeaf()
    return MapperUtils.mapLeaf(leaf);  // External path: calls toPhysicalExec()
}
```

**This is the critical fork.** The decision happens at `Mapper.mapLeaf()`:
- `EsRelation` --> `FragmentExec` (carries a logical plan fragment to be sent to data nodes)
- `ExternalRelation` --> `MapperUtils.mapLeaf()` --> `ExternalSourceExec` (coordinator-only physical node)

### MapperUtils.mapLeaf()

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java`

```java
// Line 68-85:
static PhysicalPlan mapLeaf(LeafPlan p) {
    if (p instanceof LocalRelation local) {
        return new LocalSourceExec(local.source(), local.output(), local.supplier());
    }
    if (p instanceof ExternalRelation external) {
        return external.toPhysicalExec();  // --> ExternalSourceExec
    }
    if (p instanceof ShowInfo showInfo) {
        return new ShowExec(showInfo.source(), showInfo.output(), showInfo.values());
    }
    return unsupported(p);
}
```

### How ExternalRelation Bypasses FragmentExec

The bypass is by design:

1. `ExternalRelation` is never wrapped in `FragmentExec` (Mapper.mapLeaf checks `instanceof EsRelation` first)
2. `ExternalRelation.toPhysicalExec()` (line 115-127 of ExternalRelation.java) creates an `ExternalSourceExec` directly
3. `ExternalSourceExec` implements `ExecutesOn.Coordinator` (line 48 of ExternalSourceExec.java)
4. Since there's no `FragmentExec`, there's no `ExchangeExec` added around it
5. When `breakPlanBetweenCoordinatorAndDataNode` runs, it finds no `ExchangeExec`, so `dataNodePlan` is null

**However**: with split-based distribution, an `ExchangeExec` IS added around the `ExternalSourceExec` during the Mapper's `mapUnary` method when pipeline breakers (Aggregate, Limit, TopN) are encountered. This happens because `mapUnary` calls `addExchangeForFragment` which checks `child instanceof FragmentExec`. For external sources, the child is `ExternalSourceExec`, NOT `FragmentExec`, so no exchange is added. Instead, the external distribution path in `ComputeService.executePlan()` handles this separately.

---

## 3. FragmentExec: What It Represents and How It Is Dispatched

### Structure

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/FragmentExec.java`

```java
// Line 24:
public class FragmentExec extends LeafExec implements EstimatesRowSize {
    private final LogicalPlan fragment;    // The logical plan to execute on data nodes
    private final QueryBuilder esFilter;   // Optional Lucene filter to push down
    private final int estimatedRowSize;
}
```

`FragmentExec` represents a **logical plan fragment** that will be:
1. Serialized and sent to data nodes
2. Re-planned locally by `LocalMapper` on each data node
3. Converted to `EsSourceExec` (via `LocalMapper.mapLeaf`, line 70-71 of LocalMapper.java)
4. Further optimized by local physical optimizer

### Dispatch Path

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`

The dispatch chain is:

1. `executePlan()` (line 443) calls `breakPlanBetweenCoordinatorAndDataNode()` (line 460)
2. This splits at `ExchangeExec` boundaries -- everything below the exchange becomes `dataNodePlan`
3. `getIndices()` (line 481, 1141-1148) extracts ES indices from `FragmentExec` -> `EsRelation` nodes
4. `dataNodeComputeHandler.startComputeOnDataNodes()` (line 637) dispatches `dataNodePlan` to nodes holding those shards

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java`

```java
// Line 197-203:
public static void forEachRelation(PhysicalPlan plan, Consumer<EsRelation> action) {
    plan.forEachDown(FragmentExec.class, f -> f.fragment().forEachDown(EsRelation.class, r -> {
        if (r.indexMode() != IndexMode.LOOKUP) {
            action.accept(r);
        }
    }));
}
```

This method ONLY looks at `FragmentExec` nodes containing `EsRelation`. It completely ignores `ExternalSourceExec`.

---

## 4. ExternalSourceExec Execution

### Structure

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`

```java
// Line 48:
public class ExternalSourceExec extends LeafExec implements EstimatesRowSize, ExecutesOn.Coordinator {
```

### Coordinator-Only vs Distributed

ExternalSourceExec has TWO execution modes:

**Mode 1: Coordinator-only** (when `dataNodePlan == null` in executePlan)
- `ExternalSourceExec` is part of the coordinator plan
- Executed directly by `runCompute()` on the coordinator
- `planExternalSource()` in `LocalExecutionPlanner` (line 1223-1278) creates operators

**Mode 2: Distributed** (when splits enable distribution)
- `SplitDiscoveryPhase` resolves splits, `AdaptiveStrategy` assigns to nodes
- `executeExternalDistribution()` (ComputeService line 726) sends split assignments to data nodes
- `DataNodeComputeHandler.handleExternalSourceRequest()` (line 792-808) injects splits into the plan on data nodes

### Operator Creation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/LocalExecutionPlanner.java`

```java
// Line 345-346:
} else if (node instanceof ExternalSourceExec externalSource) {
    return planExternalSource(externalSource, context);
}
```

The `planExternalSource` method (line 1223-1278) delegates to `OperatorFactoryRegistry.factory()` which dispatches based on `sourceType` to the appropriate SPI implementation.

---

## 5. Multi-FROM Today

### Parser

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`

```
// Line 40-49:
sourceCommand
    : fromCommand
    | rowCommand
    | showCommand
    | timeSeriesCommand
    | promqlCommand
    | {this.isDevVersion()}? explainCommand
    | {this.isDevVersion()}? externalCommand
    ;
```

`FROM` and `EXTERNAL` are alternative `sourceCommand` productions. They CANNOT be mixed in the same source command. However, `FROM a, (subquery)` is supported:

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`

```java
// Line 368-418: visitRelation()
```

Multi-FROM creates a `UnionAll` node (line 416):
```java
return new UnionAll(source(ctxs.getFirst(), ctxs.getLast()), mainQueryAndSubqueries, List.of());
```

### How UnionAll Maps to Physical Plan

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

```java
// Line 254-267: mapUnionAll()
private PhysicalPlan mapUnionAll(UnionAll unionAll) {
    int childSize = unionAll.children().size();
    List<PhysicalPlan> newChildren = new ArrayList<>(childSize);
    for (int i = 0; i < childSize; i++) {
        PhysicalPlan child = mapInner(unionAll.children().get(i));
        if (child instanceof FragmentExec) {
            child = new ExchangeExec(child.source(), child);
        }
        newChildren.add(child);
    }
    return new MergeExec(unionAll.source(), newChildren, unionAll.output());
}
```

Each child is mapped individually. Each `FragmentExec` child gets an `ExchangeExec`. The results are merged via `MergeExec`.

### Can `a` Be ES and `b` Be External?

**Not today.** Subqueries in `FROM` are parsed via `visitSubquery()` (line 437-448), which calls `visitFromCommand()`. Each subquery starts with its own `FROM`, producing an `UnresolvedRelation` (ES). The grammar does not allow `EXTERNAL` as a subquery source.

Even if it did, the execution layer would break because `executePlan()` assumes uniform source types (see section 8).

---

## 6. JOIN Support

### Join Plan Node

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/join/Join.java`

```java
// Line 75:
public class Join extends BinaryPlan implements PostAnalysisVerificationAware, SortAgnostic, ExecutesOn, PostOptimizationVerificationAware {
```

Only `LEFT` join type is supported (line 186-188, and verified in mapBinary line 186-187).

### Physical Join Nodes

Two physical join types exist:

1. **HashJoinExec** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/HashJoinExec.java`, line 29): In-memory hash join. Right side must be `LocalSourceExec` (data already materialized).

2. **LookupJoinExec** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/LookupJoinExec.java`, line 27): Right side must be a `FragmentExec` wrapping an `EsRelation` with `indexMode == IndexMode.LOOKUP`.

### How Joins Are Mapped

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

```java
// Line 183-233: mapBinary()
private PhysicalPlan mapBinary(BinaryPlan bp) {
    if (bp instanceof Join join) {
        JoinConfig config = join.config();
        if (config.type() != JoinTypes.LEFT) {
            throw new EsqlIllegalArgumentException("unsupported join type [" + config.type() + "]");
        }
        if (join.isRemote()) {
            return new FragmentExec(bp);  // Entire join pushed to data node
        }
        PhysicalPlan left = mapInner(bp.left());
        if (left instanceof FragmentExec) {
            return new FragmentExec(bp);  // Left is ES: push entire join to data node
        }
        PhysicalPlan right = mapInner(bp.right());
        if (right instanceof LocalSourceExec localData) {
            return new HashJoinExec(...);  // Right is local data: hash join
        }
        if (right instanceof FragmentExec fragment) {
            boolean isIndexModeLookup = isIndexModeLookup(fragment);
            if (isIndexModeLookup) {
                return new LookupJoinExec(...);  // Right is lookup index
            }
        }
    }
    return MapperUtils.unsupported(bp);
}
```

### Could a JOIN Have One ES Side and One External Side?

**Not today.** The mapper has three paths:
1. Left is `FragmentExec` -> whole join becomes `FragmentExec` (both sides must be ES)
2. Right is `LocalSourceExec` -> `HashJoinExec` (right is materialized data)
3. Right is `FragmentExec` with LOOKUP index -> `LookupJoinExec`

If the left side were an `ExternalSourceExec` (external source) and the right were a `FragmentExec`:
- `left instanceof FragmentExec` would be `false`, so it wouldn't push to data node
- It would try to map right, get a `FragmentExec`, check for LOOKUP mode
- If the right is a regular ES index (not LOOKUP mode), `MapperUtils.unsupported(bp)` would throw

If the right side were an `ExternalSourceExec`:
- `right instanceof LocalSourceExec` would be false
- `right instanceof FragmentExec` would be false
- `MapperUtils.unsupported(bp)` would throw

**Neither direction works.** Cross-source JOINs are structurally impossible in the current mapper.

### LocalMapper (Data Node Side)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/LocalMapper.java`

```java
// Line 143-151: mapBinary()
EsRelation rightRelation = null;
if (binary.right() instanceof EsRelation esRelation) {
    rightRelation = esRelation;
} else if (binary.right() instanceof Filter filter && filter.child() instanceof EsRelation esRelation) {
    rightRelation = esRelation;
}
if (rightRelation == null) {
    throw new EsqlIllegalArgumentException("Unsupported right plan for lookup join [" + binary.right().nodeName() + "]");
}
```

The LocalMapper REQUIRES the right side to be an `EsRelation`. An `ExternalRelation` on the right would throw.

---

## 7. Source Type Detection and Planning Assumptions

### Mapper: The Source Type Fork

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

The `Mapper` class makes the decision at `mapLeaf()` (line 85-93). It uses `instanceof` checks:
- `EsRelation` -> `FragmentExec` (data node path)
- Everything else -> `MapperUtils.mapLeaf()` (coordinator path)

### Mapper: Uniform Source Assumption in mapUnary

```java
// Line 98-109:
if (mappedChild instanceof FragmentExec) {
    // COORDINATOR enrich must not be included to the fragment
    if (unary instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
        mappedChild = addExchangeForFragment(enrich.child(), mappedChild);
        return MapperUtils.mapUnary(unary, mappedChild);
    }
    // in case of a fragment, push to it any streaming operator
    if (unary instanceof PipelineBreaker == false
        || (unary instanceof Limit limit && limit.local())
        || (unary instanceof TopN topN && topN.local())) {
        return new FragmentExec(unary);  // Push operator into the fragment
    }
}
```

When the child is a `FragmentExec`, streaming operators (Filter, Project, Eval, etc.) are pushed INTO the fragment. This is a critical optimization that assumes the entire sub-tree is ES-based.

If the child were an `ExternalSourceExec`, the `instanceof FragmentExec` check would fail, and the operator would be mapped as a coordinator-side physical operator via `MapperUtils.mapUnary()`.

### ComputeService.executePlan: The Execution Dispatch

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`

The `executePlan()` method (line 443-597) has mutually exclusive branches:

```
1. dataNodePlan == null && !hasConcreteIndices -> coordinator-only execution (line 490-529)
2. dataNodePlan != null && !hasConcreteIndices && distributed -> external distribution (line 531-551)
3. dataNodePlan != null && !hasConcreteIndices && !distributed -> ASSERTION ERROR (line 553-558)
4. dataNodePlan != null && hasConcreteIndices -> ES data node execution (line 559+)
```

**There is no branch for "both ES indices AND external sources."** The control flow assumes one or the other.

### PlannerUtils.forEachRelation: ES-Only Traversal

```java
// Line 197-203:
public static void forEachRelation(PhysicalPlan plan, Consumer<EsRelation> action) {
    plan.forEachDown(FragmentExec.class, f -> f.fragment().forEachDown(EsRelation.class, r -> {
        if (r.indexMode() != IndexMode.LOOKUP) {
            action.accept(r);
        }
    }));
}
```

This method only finds `EsRelation` nodes inside `FragmentExec`. It is used by `getIndices()` (line 1141-1148) to determine which ES indices to dispatch to. External sources are invisible to this traversal.

---

## 8. What Exactly Needs to Change for Cross-Source Federation

### Problem Summary

The current architecture has a hard fork: a query tree touches either ES indices OR external sources, never both. The fork manifests at multiple levels.

### Layer 1: Parser/Grammar

**Current**: `EXTERNAL` and `FROM` are alternative `sourceCommand` productions.
**Needed**: Either allow `FROM` to reference external sources (e.g., `FROM datasource::expr`), or allow subqueries to use `EXTERNAL`.

Files to change:
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` (line 40-49, source command alternatives; subquery production)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java` (line 368-418, visitRelation; line 437-448, visitSubquery)

### Layer 2: Mapper (Logical-to-Physical)

**Current**: `Mapper.mapLeaf()` forks on `instanceof EsRelation` vs everything else. `mapUnary()` pushes streaming ops into `FragmentExec`. `mapBinary()` requires both sides to be ES or local data.

**Needed**:
- A new binary plan mapping for `Join` where one side is `FragmentExec` and the other is `ExternalSourceExec`
- A mechanism to keep streaming operators above a mixed-source junction

Files to change:
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java` (line 85-93, mapLeaf; line 183-233, mapBinary)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java`

### Layer 3: Execution Dispatch

**Current**: `ComputeService.executePlan()` has mutually exclusive branches for ES vs external. `getIndices()` only looks at `FragmentExec` -> `EsRelation`.

**Needed**:
- A combined execution path that can dispatch `FragmentExec` children to ES data nodes AND `ExternalSourceExec` children to external distribution simultaneously
- Both exchange sources need to feed into the same coordinator plan

Files to change:
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java` (line 443-597, executePlan; line 1141-1148, getIndices)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java` (line 116-128, breakPlanBetweenCoordinatorAndDataNode; line 197-203, forEachRelation)

### Layer 4: Plan Split (`breakPlanBetweenCoordinatorAndDataNode`)

**Current**: Splits at the FIRST `ExchangeExec`. Assumes a single exchange boundary.

**Needed**: For a mixed-source query (e.g., `FROM es_index | JOIN ON x (EXTERNAL "s3://...")`) the plan would have TWO leaf-level sources feeding into a join. The coordinator plan needs TWO exchange sources (one from ES data nodes, one from external distribution). `breakPlanBetweenCoordinatorAndDataNode` only handles one.

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java` (line 116-128)

### Layer 5: JOIN Execution

**Current**: `LookupJoinExec` requires right side to be `FragmentExec` wrapping `EsRelation` in LOOKUP mode. `HashJoinExec` requires right side to be `LocalSourceExec`.

**Needed**: A new join physical plan type (or extended `LookupJoinExec`) that can have an `ExternalSourceExec` on one side and a `FragmentExec` on the other.

Files to change:
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java` (line 183-233, mapBinary)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/LocalMapper.java` (line 118-171, mapBinary)
- Potentially a new physical plan node for cross-source joins

### Layer 6: Optimizer Assumptions

**Current**: `PushFiltersToSource` (physical optimizer rule) handles `EsQueryExec` and `ExternalSourceExec` separately. `localPlan()` only transforms `FragmentExec` nodes.

**Needed**: If both source types coexist in a plan, the optimizer needs to handle filter pushdown to both source types within the same plan tree.

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java` (line 45-57)

### Layer 7: ExternalRelation Serialization

**Current**: `ExternalRelation.writeTo()` throws `UnsupportedOperationException` (line 76-77).

**Needed**: If an `ExternalRelation` appears inside a `FragmentExec` (e.g., for a join pushed to data nodes), it must be serializable. However, this is complex because external source metadata may not be representable in a serializable form.

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java` (line 76-81)

---

## Architectural Diagnosis

The fundamental issue is that the current architecture models ES indices and external sources as **entirely separate execution paths** that converge only at the syntax level (`sourceCommand` alternatives). There is no plan-level abstraction that unifies them.

To enable cross-source federation, the key architectural change would be:

1. **Unified source abstraction in the mapper**: Instead of forking at `instanceof EsRelation` vs `ExternalRelation`, introduce a common physical plan pattern where both source types produce data via exchanges, and the coordinator merges them.

2. **Multi-exchange coordinator plan**: `breakPlanBetweenCoordinatorAndDataNode` must support multiple exchange boundaries in a single plan tree, with different source types below each exchange.

3. **Join pipeline**: A new join strategy that asynchronously materializes one side (external or ES) and streams against the other.

The most practical near-term path is likely to support cross-source federation only via `UnionAll` (multi-FROM with subqueries), not via JOINs, because `UnionAll` already creates independent sub-plans that are executed separately and merged.
