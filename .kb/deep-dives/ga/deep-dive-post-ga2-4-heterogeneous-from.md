# Deep Dive: Heterogeneous FROM (UNION ALL across ES indices + external sources)

**Scope:** UNION ALL only, NOT JOINs. The question: what needs to change so that `FROM es_logs, my_s3::*.parquet` produces a plan where one child reads from ES and another reads from an external source, with results merged on the coordinator?

---

## 1. mapUnionAll in Mapper.java

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`, lines 254-267.

```java
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

### Key Observations

1. **Each child is mapped independently** via `mapInner()`. This is good -- each child traverses the full leaf+unary+binary mapping logic independently.

2. **The critical line is the `instanceof FragmentExec` check** (line 261). After `mapInner`:
   - An `EsRelation` leaf maps to `FragmentExec` (via `mapLeaf` at line 87), and then `mapUnionAll` wraps it in `ExchangeExec`.
   - An `ExternalRelation` leaf maps to `ExternalSourceExec` (via `MapperUtils.mapLeaf` at line 76 of MapperUtils.java), which is NOT `instanceof FragmentExec`.

3. **So today, if a child maps to `ExternalSourceExec`, it passes through without being wrapped in `ExchangeExec`.** The result would be a `MergeExec` with heterogeneous children:
   - Child 0: `ExchangeExec` -> `FragmentExec` (ES)
   - Child 1: `ExternalSourceExec` (external -- no exchange wrapping)

4. **But the comment in the code says**: "ComputeService.executePlan has trouble with executing plan without coordinator plan, adding exchange solves the issue." The lack of an ExchangeExec around the ExternalSourceExec child may cause problems in `executePlan`.

### What would need to change

The `mapUnionAll` method wraps only `FragmentExec` children in `ExchangeExec`. For external sources, we have two options:
- **Option A:** Wrap `ExternalSourceExec` children in `ExchangeExec` too, so the distribution infrastructure can find them. This is what happens for standalone external queries today -- the external source gets wrapped in ExchangeExec by the Mapper when it encounters pipeline breakers above external sources (e.g., `Limit` calls `addExchangeForFragment` which only handles `FragmentExec`).
- **Option B:** Leave them unwrapped and handle the asymmetry in `breakPlanIntoSubPlansAndMainPlan`.

Option A is cleaner, but the `collapseExternalSourceExchanges` method in ComputeService (line 299-305) will later strip the ExchangeExec from ExternalSourceExec children that don't need distribution. So wrapping is safe -- the later phase knows how to collapse them.

**Verdict:** `mapUnionAll` needs a one-line addition: also wrap `ExternalSourceExec` in `ExchangeExec`. Or better: wrap ALL non-exchange children (any child that isn't already providing an exchange boundary).

---

## 2. MergeExec Physical Plan Node

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/MergeExec.java`

```java
public class MergeExec extends PhysicalPlan {
    private final List<Attribute> output;

    public MergeExec(Source source, List<PhysicalPlan> children, List<Attribute> output) {
        super(source, children);
        this.output = output;
    }

    public List<LocalSupplier> suppliers() {
        return children().stream().map(LocalSourceExec.class::cast).map(LocalSourceExec::supplier).toList();
    }
}
```

### Key Observations

1. **MergeExec makes NO assumptions about child types.** It takes `List<PhysicalPlan>` -- any mix of children is structurally valid.

2. **The `suppliers()` method is a concern.** It casts ALL children to `LocalSourceExec`. This is called from `LocalExecutionPlanner` (for the FORK case, after all children have been resolved to `LocalSourceExec` via local optimization). For the heterogeneous FROM case, this method should never be called during the `breakPlanIntoSubPlansAndMainPlan` path, because MergeExec is replaced before the plan reaches `LocalExecutionPlanner`.

3. **MergeExec is NOT serializable** (`getWriteableName` throws `UnsupportedOperationException`). It only exists in the coordinator's pre-split plan.

**Verdict:** MergeExec itself is fine for heterogeneous children. No changes needed to the node itself.

---

## 3. breakPlanIntoSubPlansAndMainPlan

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java`, lines 101-114.

```java
public static Tuple<List<PhysicalPlan>, PhysicalPlan> breakPlanIntoSubPlansAndMainPlan(PhysicalPlan plan) {
    var subplans = new Holder<List<PhysicalPlan>>();
    PhysicalPlan mainPlan = plan.transformUp(MergeExec.class, me -> {
        subplans.set(
            me.children()
                .stream()
                .map(child -> (PhysicalPlan) new ExchangeSinkExec(child.source(), child.output(), false, child))
                .toList()
        );
        return new ExchangeSourceExec(me.source(), me.output(), false);
    });
    return new Tuple<>(subplans.get(), mainPlan);
}
```

### Key Observations

1. **This method is source-type agnostic.** It wraps EVERY child of MergeExec in `ExchangeSinkExec`, regardless of child type. Whether the child is `ExchangeExec->FragmentExec` (ES) or `ExchangeExec->ExternalSourceExec` (external) or even bare `ExternalSourceExec`, each child gets wrapped in `ExchangeSinkExec`.

2. **The MergeExec is replaced with a single `ExchangeSourceExec`**, which becomes the coordinator's source for the merged data from all branches.

3. **Each subplan is then independently sent to `executePlan()`** (see ComputeService lines 408-438). This is the key design: each subplan is an independent execution unit with its own session, exchange sink, and full execution lifecycle.

**Verdict:** This method already handles heterogeneous children correctly. No changes needed.

---

## 4. breakPlanBetweenCoordinatorAndDataNode

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java`, lines 116-128.

```java
public static Tuple<PhysicalPlan, PhysicalPlan> breakPlanBetweenCoordinatorAndDataNode(PhysicalPlan plan, Configuration config) {
    var dataNodePlan = new Holder<PhysicalPlan>();
    PhysicalPlan coordinatorPlan = plan.transformUp(ExchangeExec.class, e -> {
        var subplan = e.child();
        dataNodePlan.set(new ExchangeSinkExec(e.source(), e.output(), e.inBetweenAggs(), subplan));
        return new ExchangeSourceExec(e.source(), e.output(), e.inBetweenAggs());
    });
    return new Tuple<>(coordinatorPlan, dataNodePlan.get());
}
```

### Key Observations -- CRITICAL BUG POTENTIAL

1. **This method uses `Holder<PhysicalPlan>` -- a single holder for the data node plan.** If `transformUp` encounters MULTIPLE `ExchangeExec` nodes, each successive one overwrites the previous one in the holder. **Only the last ExchangeExec visited survives.**

2. **But this is NOT a problem for the heterogeneous FROM case**, because by the time this method is called on a subplan, it is called on a SINGLE child of the MergeExec (from step 3 above). Each subplan is processed independently in `executePlan()`, and each subplan has at most ONE ExchangeExec boundary.

3. **The execution flow is:**
   - `execute()` calls `breakPlanIntoSubPlansAndMainPlan()` -- splits MergeExec into N subplans
   - For each subplan, `executePlan()` is called independently
   - Inside `executePlan()`, `breakPlanBetweenCoordinatorAndDataNode()` splits that single subplan at its one ExchangeExec boundary

4. **For an ES subplan:** `ExchangeSinkExec -> ExchangeExec -> FragmentExec` splits into coordinator `ExchangeSinkExec -> ExchangeSourceExec` and data node `ExchangeSinkExec -> FragmentExec`.

5. **For an external subplan with distribution:** `ExchangeSinkExec -> ExchangeExec -> ExternalSourceExec` splits similarly and goes through `executeExternalDistribution()`.

6. **For an external subplan without distribution:** The `collapseExternalSourceExchanges()` call in `applyExternalDistributionStrategy()` removes the ExchangeExec wrapping the ExternalSourceExec. Result: `ExchangeSinkExec -> ExternalSourceExec` -- no ExchangeExec left, so `breakPlanBetweenCoordinatorAndDataNode` returns null for `dataNodePlan`. This hits the `dataNodePlan == null` path in `executePlan()` (line 490), running coordinator-only.

**Verdict:** No changes needed to `breakPlanBetweenCoordinatorAndDataNode`. The two-level split design (subplans first, then coordinator/data-node second) means each subplan is independently simple.

---

## 5. executePlan Flow for Each Subplan

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`, lines 443-552.

The critical insight is `executePlan()` has THREE execution paths:

### Path A: No data node plan (`dataNodePlan == null`, line 490)
- Coordinator-only execution. `runCompute()` runs the plan locally.
- **This is what happens for non-distributed external sources today.**

### Path B: Distributed external source (`distributionResult.isDistributed() && hasConcreteIndices == false`, line 532)
- Calls `executeExternalDistribution()` which distributes external splits across data nodes.
- **This is what happens for distributed external sources today.**

### Path C: ES indices (`hasConcreteIndices == true`, line 558+)
- Standard ES execution: coordinator plan + data node plan dispatched to nodes holding shards.
- **This is the standard ES path.**

### The Key Problem

There's a structural assertion at line 553-556:
```java
if (hasConcreteIndices == false) {
    var error = "expected concrete indices with data node plan but got empty; data node plan " + dataNodePlan;
    assert false : error;
}
```

This means: if we have a data node plan but no concrete indices, AND it's not a distributed external source, it's an error. This path would be hit if somehow an external source had an ExchangeExec that wasn't collapsed and wasn't distributed.

**For the heterogeneous FROM case, this is NOT a problem** because each subplan is independent -- the ES subplan has indices, the external subplan either has distribution or runs coordinator-only.

### What's Actually Needed

The `executePlan()` method already handles each case correctly when called independently per subplan. The two-level split ensures:
- ES subplan -> Path C
- External subplan (non-distributed) -> Path A
- External subplan (distributed) -> Path B

**But there's a subtlety:** `executePlan()` calls `getIndices(resolvedPlan, EsRelation::concreteIndices)` on line 481. For an external subplan, this returns empty (no EsRelation). For an ES subplan, it returns the concrete indices. Since each subplan is processed independently, this works.

**However:** the `executePlan` calls `discoverSplits()` and `applyExternalDistributionStrategy()` at lines 457-458. For an ES subplan, `discoverSplits` would be a no-op (no ExternalSourceExec to find). For an external subplan, `getIndices` would be empty. Each subplan stays on its correct path.

**Verdict:** `executePlan()` already handles heterogeneous children correctly via the two-level split. No changes needed.

---

## 6. Multi-FROM Parsing (visitRelation)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`, lines 368-418.

```java
private LogicalPlan visitRelation(Source source, SourceCommand command,
        EsqlBaseParser.IndexPatternAndMetadataFieldsContext ctx) {
    List<EsqlBaseParser.IndexPatternOrSubqueryContext> ctxs = ctx == null ? null : ctx.indexPatternOrSubquery();
    List<EsqlBaseParser.IndexPatternContext> indexPatternsCtx = new ArrayList<>();
    List<EsqlBaseParser.SubqueryContext> subqueriesCtx = new ArrayList<>();
    if (ctxs != null) {
        ctxs.forEach(c -> {
            if (c.indexPattern() != null) {
                indexPatternsCtx.add(c.indexPattern());
            } else {
                subqueriesCtx.add(c.subquery());
            }
        });
    }
    IndexPattern table = new IndexPattern(source, visitIndexPattern(indexPatternsCtx));
    List<Subquery> subqueries = visitSubqueriesInFromCommand(subqueriesCtx);
    // ... metadata fields ...
    UnresolvedRelation unresolvedRelation = new UnresolvedRelation(source, table, false, metadataFields, null, command);
    if (subqueries.isEmpty()) {
        return unresolvedRelation;
    } else {
        List<LogicalPlan> mainQueryAndSubqueries = new ArrayList<>(subqueries.size() + 1);
        if (table.indexPattern().isEmpty() == false) {
            mainQueryAndSubqueries.add(unresolvedRelation);
        }
        mainQueryAndSubqueries.addAll(subqueries);
        if (mainQueryAndSubqueries.size() == 1) {
            return table.indexPattern().isEmpty() ? subqueries.get(0).plan() : unresolvedRelation;
        } else {
            return new UnionAll(source(ctxs.getFirst(), ctxs.getLast()), mainQueryAndSubqueries, List.of());
        }
    }
}
```

### Key Observations

1. **The current FROM grammar** (from EsqlBaseParser.g4 line 102-104):
   ```
   fromCommand : FROM indexPatternAndMetadataFields ;
   indexPatternAndMetadataFields : indexPatternOrSubquery (COMMA indexPatternOrSubquery)* metadata? ;
   indexPatternOrSubquery : indexPattern | {this.isDevVersion()}? subquery ;
   ```

2. **EXTERNAL is a SEPARATE source command** (line 110-112):
   ```
   externalCommand : DEV_EXTERNAL stringOrParameter commandNamedParameters ;
   ```

3. **FROM and EXTERNAL are completely separate grammar rules.** FROM produces `indexPatternOrSubquery` items, while EXTERNAL is its own top-level source command. You cannot write `FROM es_logs, EXTERNAL "s3://bucket/file.parquet"` today -- the grammar doesn't allow it.

4. **Even subqueries inside FROM** (line 120-121) are `LP fromCommand (PIPE processingCommand)* RP`, meaning they must start with a FROM, not EXTERNAL.

### The Grammar Gap

The target syntax `FROM es_logs, my_s3::*.parquet` requires either:
- **Option A:** Extend `indexPattern` to recognize `datasource::expression` syntax and produce an `UnresolvedExternalRelation` alongside `UnresolvedRelation` within the same FROM.
- **Option B:** Allow EXTERNAL as a subquery inside FROM, e.g., `FROM es_logs, (EXTERNAL "s3://...")`.
- **Option C (target):** Extend `indexPatternOrSubquery` to include a new `externalPattern` alternative that handles `datasource::expr`.

**Option C is what the roadmap proposes.** The `indexPattern` rule already has `(clusterString COLON)` prefix for CCS. The `datasource::expr` syntax would use `CAST_OP` (`::`) as a discriminator. After parsing, it would produce an `UnresolvedExternalRelation` mixed into the same `UnionAll`.

### What the Parser Would Produce

With the target syntax `FROM es_logs, my_s3::*.parquet`:
```
UnionAll
  |- UnresolvedRelation("es_logs")
  |- UnresolvedExternalRelation("my_s3::*.parquet")
```

The `visitRelation` method at line 416 already creates a `UnionAll` with `List<LogicalPlan>`. It would just need to also accept external patterns in that list. The key change is in the grammar and the visitor logic that splits items into `indexPatternsCtx` vs external patterns.

**Verdict:** Grammar needs to be extended to allow `datasource::expr` as an `indexPatternOrSubquery` alternative. The `visitRelation` method needs to handle the new syntax and produce `UnresolvedExternalRelation` children alongside `UnresolvedRelation` children in the `UnionAll`.

---

## 7. ExchangeExec and Exchange Handling for Both Source Types

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExchangeExec.java`

### Exchange Architecture

ExchangeExec is a logical boundary marker. It says: "data below here runs remotely and results flow back through an exchange." The actual exchange wiring happens in:

1. `breakPlanIntoSubPlansAndMainPlan()` -- replaces MergeExec with ExchangeSourceExec, wraps children with ExchangeSinkExec
2. `breakPlanBetweenCoordinatorAndDataNode()` -- replaces ExchangeExec with ExchangeSourceExec, wraps child with ExchangeSinkExec
3. `ComputeService.execute()` -- creates `ExchangeSourceHandler` and `ExchangeSinkHandler` and wires them together

### How Multiple Exchanges Feed Into One Coordinator MergeExec

The design at `ComputeService.execute()` (lines 354-441) already handles multiple subplans feeding into one coordinator:

```java
// Line 356: The main coordinator plan reads from ExchangeSourceExec
PhysicalPlan mainPlan = new OutputExec(subplansAndMainPlan.v2(), collectedPages::add);

// Line 366-368: One ExchangeSourceHandler on the coordinator
ExchangeSourceHandler mainExchangeSource = new ExchangeSourceHandler(...);

// Lines 408-438: Each subplan gets its own ExchangeSinkHandler, wired to the main source
for (int i = 0; i < subplans.size(); i++) {
    ExchangeSinkHandler exchangeSink = exchangeService.createSinkHandler(childSessionId, ...);
    mainExchangeSource.addRemoteSink(exchangeSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
    executePlan(childSessionId, rootTask, flags, subplan, ...);
}
```

**This is N-to-1 fan-in:** multiple sink handlers (one per subplan) feed into a single source handler. The `ExchangeSourceHandler` internally merges pages from all its remote sinks into a single stream.

**For heterogeneous FROM:** subplan 0 (ES) goes through Path C in `executePlan()`, producing pages via shard execution on data nodes. Subplan 1 (external) goes through Path A or B, producing pages via coordinator-only or distributed external execution. Both feed their pages into the same `ExchangeSourceHandler` on the coordinator. The coordinator's main plan (above the MergeExec boundary) reads from the unified exchange source, seeing a merged stream of pages.

**Verdict:** The exchange architecture already supports heterogeneous children. No changes needed to exchange handling.

---

## 8. runCompute

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`, lines 875-974.

```java
void runCompute(CancellableTask task, ComputeContext context, PhysicalPlan plan, ...) {
    // Creates LocalExecutionPlanner
    LocalExecutionPlanner planner = new LocalExecutionPlanner(...);
    // Checks for external sources in the plan
    boolean hasExternalSource = plan.anyMatch(p -> p instanceof ExternalSourceExec);
    // Uses appropriate local plan optimization
    var localPlan = switch (localPhysicalOptimization) {
        case ENABLED -> hasExternalSource
            ? PlannerUtils.localPlan(..., filterPushdownRegistry, ...)
            : PlannerUtils.localPlan(...);
        case DISABLED -> plan;
    };
    // Plans and runs drivers
    var localExecutionPlan = planner.plan(..., localPlan, ...);
    var drivers = localExecutionPlan.createDrivers(driverSessionId);
    driverRunner.executeDrivers(task, drivers, ...);
}
```

### Key Observations

1. **`runCompute` does NOT assume one exchange source.** It simply plans whatever plan it receives into operators and drivers. The plan might have `ExchangeSourceExec` (fed by remote data), or `ExternalSourceExec` (reading external data locally), or `LocalSourceExec` (local in-memory data), or any combination.

2. **The `hasExternalSource` check** (line 914) only affects which local plan optimization path is used (with or without `filterPushdownRegistry`). This is a minor optimization detail.

3. **For the coordinator main plan** in the heterogeneous FROM case: the plan received by `runCompute` is `OutputExec -> ExchangeSourceExec`. This is the simplest possible plan -- it just reads from the exchange source. The exchange source is fed by the subplan results. `runCompute` handles this trivially.

**Verdict:** `runCompute` is fully generic. No changes needed.

---

## Summary: What Actually Needs to Change

The analysis reveals that the infrastructure is remarkably well-positioned for heterogeneous FROM. The two-level split design (`breakPlanIntoSubPlansAndMainPlan` + per-subplan `executePlan`) means each source type is handled independently in its own execution path. Here's the exhaustive list of changes:

### Must Change (Blockers)

| # | Component | File | Change | Effort |
|---|-----------|------|--------|--------|
| 1 | **Grammar** | `EsqlBaseParser.g4` | Add `datasource::expr` alternative to `indexPatternOrSubquery` rule | Medium |
| 2 | **Lexer** | `From.g4` / `EsqlBaseLexer.g4` | Recognize `::` in FROM_MODE for datasource prefix syntax | Small |
| 3 | **Parser visitRelation** | `LogicalPlanBuilder.java` lines 368-418 | Handle new external pattern in `indexPatternOrSubquery`, create `UnresolvedExternalRelation` alongside `UnresolvedRelation`, put both in `UnionAll` | Medium |
| 4 | **Mapper.mapUnionAll** | `Mapper.java` line 261 | Wrap non-FragmentExec children (specifically `ExternalSourceExec`) in `ExchangeExec` too, so each child has a uniform exchange boundary | Small (1 line) |
| 5 | **PreAnalyzer** | `PreAnalyzer.java` | Already collects both `UnresolvedRelation` (indices) and `UnresolvedExternalRelation` (external paths) from the plan tree. Must ensure both are collected when both appear under a `UnionAll`. **Already works** -- `forEachUp` traverses all children. | None |
| 6 | **Analyzer** | `Analyzer.java` | `ResolveExternalRelations` (line 456) and `ResolveTable` (existing) already resolve their respective node types independently. Must work in a mixed plan. **Already works** -- rules match by type. | None |

### Already Works (No Changes Needed)

| Component | Why It Works |
|-----------|--------------|
| **MergeExec** | Type-agnostic: accepts `List<PhysicalPlan>` children |
| **breakPlanIntoSubPlansAndMainPlan** | Wraps ALL MergeExec children in ExchangeSinkExec, regardless of type |
| **breakPlanBetweenCoordinatorAndDataNode** | Called per-subplan, each subplan has at most one ExchangeExec |
| **executePlan** | Three independent paths (coordinator-only, external-distributed, ES-shards) -- each subplan routes correctly |
| **Exchange infrastructure** | N-to-1 fan-in via ExchangeSourceHandler.addRemoteSink -- handles arbitrary number of heterogeneous sources |
| **runCompute** | Fully generic plan execution |
| **SplitDiscoveryPhase** | Called per-subplan; no-op for ES subplans, discovers splits for external subplans |
| **LocalExecutionPlanner** | Handles ExternalSourceExec and ExchangeSourceExec independently |
| **UnionAll logical plan** | Type-agnostic children list; data type verification works for any source |

### The Change in mapUnionAll (Detail)

Current code (line 261):
```java
if (child instanceof FragmentExec) {
    child = new ExchangeExec(child.source(), child);
}
```

Required change:
```java
if (child instanceof FragmentExec || child instanceof ExternalSourceExec) {
    child = new ExchangeExec(child.source(), child);
}
```

Or more robustly:
```java
if (child instanceof ExchangeExec == false) {
    child = new ExchangeExec(child.source(), child);
}
```

This ensures every child has an ExchangeExec boundary, which is then handled correctly by `breakPlanIntoSubPlansAndMainPlan`. Without this wrapping, an `ExternalSourceExec` child would lack the exchange boundary, and when `executePlan` calls `breakPlanBetweenCoordinatorAndDataNode`, there would be no ExchangeExec to split -- leading to coordinator-only execution that doesn't properly feed into the MergeExec's exchange source.

### Example Execution Flow

Query: `FROM es_logs, my_s3::data/*.parquet | LIMIT 100`

**After parsing:**
```
Limit[100]
  UnionAll
    |- UnresolvedRelation("es_logs")
    |- UnresolvedExternalRelation("my_s3::data/*.parquet")
```

**After analysis:**
```
Limit[100]
  UnionAll
    |- EsRelation("es_logs", [fields...])
    |- ExternalRelation("s3://bucket/data/*.parquet", [fields...])
```

**After optimization** (limit pushed to each branch):
```
Limit[100]
  UnionAll
    |- Limit[100]
    |    |- EsRelation("es_logs", [fields...])
    |- Limit[100]
         |- ExternalRelation("s3://bucket/data/*.parquet", [fields...])
```

**After Mapper (physical plan):**
```
LimitExec[100]
  MergeExec
    |- ExchangeExec                      # ES child
    |    |- FragmentExec
    |         |- Limit[100]
    |              |- EsRelation
    |- ExchangeExec                      # External child (NEW: wrapped)
         |- ExternalSourceExec
```

**After breakPlanIntoSubPlansAndMainPlan:**
```
Main coordinator plan:
  LimitExec[100]
    ExchangeSourceExec  <-- reads merged results from both subplans

Subplan 0 (ES):
  ExchangeSinkExec
    ExchangeExec -> FragmentExec(Limit[100] -> EsRelation)

Subplan 1 (External):
  ExchangeSinkExec
    ExchangeExec -> ExternalSourceExec
```

**Subplan 0 execution (executePlan):**
- `discoverSplits` -> no-op (no ExternalSourceExec)
- `applyExternalDistributionStrategy` -> no-op (no external splits)
- `breakPlanBetweenCoordinatorAndDataNode` splits at ExchangeExec
- `hasConcreteIndices` -> true -> Path C (standard ES execution)
- Data nodes run FragmentExec, results flow via exchange

**Subplan 1 execution (executePlan):**
- `discoverSplits` -> discovers file splits for `*.parquet`
- `applyExternalDistributionStrategy` -> decides coordinator-only or distributed
- If coordinator-only: `collapseExternalSourceExchanges` removes ExchangeExec -> `dataNodePlan == null` -> Path A
- If distributed: `breakPlanBetweenCoordinatorAndDataNode` splits at ExchangeExec -> Path B

**Both subplans feed results into the main ExchangeSourceHandler, which the coordinator's LimitExec reads from.**

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Schema resolution for mixed sources (different field names/types) | Medium | UnionAll already handles schema union; missing fields become null. Already tested for subquery FROM. |
| Metadata fields (_id, _index) from ES don't exist in external | Low | External sources return null for metadata. UnionAll output union handles this. |
| Limit pushdown asymmetry (ES gets local limit, external may not) | Low | Each branch is optimized independently. External branch gets limit too. |
| Error handling: if one subplan fails, does the other get cancelled? | Medium | ComputeListener/cancelQueryOnFailure already handles this for FORK. Same mechanism applies. |
| Output attribute ID conflicts between ES and external branches | Medium | UnionAll already resolves output attributes. Reference attributes are used. Same as subquery FROM. |

---

## Conclusion

Heterogeneous FROM is architecturally straightforward because the existing FORK/UnionAll infrastructure treats each branch as an independent execution unit. The two-level split design means ES and external sources never need to coexist within a single `executePlan` call -- they're separated into independent subplans that each route through their correct execution path.

**Total changes needed:**
1. Grammar extension for `datasource::expr` syntax (the FROM syntax work item)
2. Parser changes to produce `UnresolvedExternalRelation` in mixed FROM
3. One line in `Mapper.mapUnionAll` to wrap external sources in ExchangeExec

Everything else -- the execution engine, exchange infrastructure, split discovery, distribution, local planning -- already works.

**Estimated effort:** 1-2 weeks for the grammar + parser changes (this overlaps with the "FROM syntax" work item in the roadmap). The mapper change is trivial. The existing test infrastructure for FORK/subquery FROM provides a template for heterogeneous FROM tests.
