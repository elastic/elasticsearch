# Deep Dive: External Source Fragment Handling in ES|QL Planner

## 1. How ExternalSourceExec Is Handled in the Mapper -- NOT Wrapped in FragmentExec

### Key File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

In `Mapper.mapLeaf()` (lines 85-93):

```java
private PhysicalPlan mapLeaf(LeafPlan leaf) {
    if (leaf instanceof EsRelation esRelation) {
        return new FragmentExec(esRelation);  // ES indices get wrapped in FragmentExec
    }
    // ExternalRelation is handled by MapperUtils.mapLeaf()
    return MapperUtils.mapLeaf(leaf);  // External sources go here -- NO FragmentExec
}
```

### Key File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java`

In `MapperUtils.mapLeaf()` (lines 68-85):

```java
static PhysicalPlan mapLeaf(LeafPlan p) {
    if (p instanceof LocalRelation local) {
        return new LocalSourceExec(local.source(), local.output(), local.supplier());
    }
    // External data sources (Iceberg, Parquet, etc.)
    // These are executed on the coordinator only, bypassing FragmentExec/ExchangeExec dispatch
    if (p instanceof ExternalRelation external) {
        return external.toPhysicalExec();  // Creates ExternalSourceExec directly
    }
    // ...
}
```

### Key File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java`

`ExternalRelation.toPhysicalExec()` (lines 115-127) creates an `ExternalSourceExec` directly -- no `FragmentExec` wrapper.

### Marker Interfaces

Both `ExternalRelation` (line 46) and `ExternalSourceExec` (line 48) implement `ExecutesOn.Coordinator` (defined in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExecutesOn.java`), which marks them as coordinator-only execution nodes. The same pattern applies in `LocalMapper.mapLeaf()` (lines 69-77 of `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/LocalMapper.java`).

---

## 2. How addExchangeForFragment Works and Why It Skips External Sources

### Key File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java`

In `Mapper.addExchangeForFragment()` (lines 269-278):

```java
private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
    if (child instanceof FragmentExec) {
        child = new FragmentExec(logical);       // Include pipeline breaker in fragment
        child = new ExchangeExec(child.source(), child);  // Mark coordinator/data-node boundary
    }
    return child;
}
```

**What it does**: When the child is a `FragmentExec` (meaning data-node-bound plan with an `EsRelation` at bottom), it:
1. Creates a NEW `FragmentExec` that includes the pipeline breaker (Aggregate, Limit, TopN, etc.) so it can be replanned on data nodes.
2. Wraps it in an `ExchangeExec`, marking the boundary between data node and coordinator execution.

**Called by these pipeline breakers in `mapUnary()` (same file):**
- **Aggregate** (line 121)
- **Limit** (line 141)
- **TopN** (line 146)
- **MetricsInfo** (line 161)
- **TsInfo** (line 173)
- **Coordinator Enrich** (line 101)

**Why it skips external sources**: Since `ExternalRelation` maps to `ExternalSourceExec` (not `FragmentExec`), the `child instanceof FragmentExec` check always returns **false**. The method returns the child unchanged. This means:
- No `ExchangeExec` is ever inserted
- No fragment boundary is ever created
- The pipeline breaker executes on the coordinator

### Concrete Example: STATS on External Source

For `EXTERNAL "s3://..." | STATS count(*) BY field`, tracing through `Mapper.mapUnary()` lines 115-138:

```java
if (unary instanceof Aggregate aggregate) {
    List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);
    mappedChild = addExchangeForFragment(aggregate, mappedChild);  // NO-OP: child is ExternalSourceExec

    if (mappedChild instanceof ExchangeExec exchange) {  // FALSE
        // two-phase path -- not taken
    }
    // "if no exchange was added, try single-pass agg"
    else if (aggregate.groupings().stream()
        .noneMatch(group -> group.anyMatch(expr -> expr instanceof GroupingFunction.NonEvaluatableGroupingFunction))) {
        return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.SINGLE, intermediate);
        // ^^^ Returns: AggregateExec[SINGLE] -> ExternalSourceExec
    }
}
```

Result plan:
```
AggregateExec[SINGLE]    <-- single-phase, coordinator only
  ExternalSourceExec
```

This is confirmed by tests in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionTests.java` (lines 55-61):
```java
AggregateExec aggExec = (AggregateExec) physicalPlan;
assertEquals(AggregatorMode.SINGLE, aggExec.getMode());
assertTrue("Expected ExternalSourceExec child", aggExec.child() instanceof ExternalSourceExec);
```

### Contrast: STATS on ES Index

For `FROM logs | STATS count(*) BY field`:
```
AggregateExec[FINAL]          <-- coordinator: merge partial aggregations
  ExchangeSourceExec           <-- receive from data nodes
    --- boundary ---
  ExchangeSinkExec             <-- send to coordinator
    AggregateExec[INITIAL]     <-- data node: partial aggregation
      FragmentExec -> EsRelation
```

Two-phase (INITIAL on data nodes, FINAL on coordinator), with an exchange boundary.

---

## 3. Coordinator vs Data-Node Split for External Sources

### Key File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java`

`breakPlanBetweenCoordinatorAndDataNode()` (lines 116-128):

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

Since the Mapper never creates an `ExchangeExec` for external sources, `dataNodePlan.get()` returns **null** for pure external source queries.

### The Three Execution Paths in executePlan

Key File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`

In `executePlan()` (starting at line 443):

**Step 1: Split discovery and distribution planning (lines 457-462)**
```java
final PhysicalPlan splitPlan = discoverSplits(physicalPlan);
final ExternalDistributionResult distributionResult = applyExternalDistributionStrategy(splitPlan, configuration);
final PhysicalPlan resolvedPlan = distributionResult.plan();
Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan =
    PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(resolvedPlan, configuration);
```

**Path A: Coordinator-only -- dataNodePlan is null (lines 490-529)**
This is the DEFAULT path for pure external source queries. No exchange in the plan means no data node plan. Everything runs on the coordinator:
```
OutputExec -> AggregateExec[SINGLE] -> ExternalSourceExec[splits=discovered]
```

**Path B: Distributed external -- distribution triggered, no ES indices (lines 532-551)**
```java
if (distributionResult.isDistributed() && hasConcreteIndices == false) {
    executeExternalDistribution(..., (ExchangeSinkExec) dataNodePlan, ...);
}
```
This path requires `dataNodePlan` to be non-null (i.e., an `ExchangeExec` in the plan). Since the Mapper never creates one for external sources, **this path is currently unreachable for pure external source queries produced by the normal query flow**.

The `AdaptiveStrategy` (in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/AdaptiveStrategy.java`, lines 42-77) DOES decide to distribute when `splits > 1` and `hasAggregation || manySplits`, BUT without an ExchangeExec in the plan, the distribution code path cannot be entered.

**Implication**: The distribution infrastructure (AdaptiveStrategy, RoundRobinStrategy, WeightedRoundRobinStrategy, startExternalComputeOnDataNodes) is fully implemented but there's a gap: the Mapper must be modified to create ExchangeExec nodes for external sources to enable the distributed path.

The distribution tests in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExternalSourceDataNodeTests.java` (line 126) manually construct `ExchangeExec -> ExternalSourceExec` plans to test the break/distribution logic, confirming this is the expected plan structure when distribution is enabled.

**Path C: Standard ES data node execution with possible external sources mixed in (lines 553+)**
Normal ES index path with ExchangeExec from FragmentExec.

### collapseExternalSourceExchanges (lines 299-306)

```java
static PhysicalPlan collapseExternalSourceExchanges(PhysicalPlan plan) {
    return plan.transformUp(ExchangeExec.class, exchange -> {
        if (exchange.child() instanceof ExternalSourceExec) {
            return exchange.child();  // Remove the exchange, keep ExternalSourceExec
        }
        return exchange;
    });
}
```

Called in the non-distributed path (line 284) to strip away exchanges wrapping ExternalSourceExec. This is relevant when/if the Mapper is modified to insert exchanges for external sources -- in the non-distributed case, those exchanges would be collapsed back.

---

## 4. DataNodeComputeHandler: External Splits vs ES Shard Splits

### Key File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/DataNodeComputeHandler.java`

### Request Routing (messageReceived, lines 732-790)

```java
public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
    if (request.externalSplits().isEmpty() == false && request.shards().isEmpty()) {
        handleExternalSourceRequest(request, (CancellableTask) task, listener, planTimeProfile);
        return;
    }
    // ... normal ES shard handling
}
```

Routing logic: external splits with no ES shards --> external path. Otherwise --> normal ES shard path.

### ES Shard Path: runComputeOnDataNode (lines 640-729)

For ES index queries:
1. Creates TWO exchange layers: `internalSink` (shard-to-node-reduce) and `externalSink` (node-to-coordinator)
2. `DataNodeRequestExecutor` batches shards based on `maxConcurrentShards` (line 676)
3. For each batch, acquires `SearchContext` objects per shard (lines 546-615)
4. Runs per-shard or per-batch compute with the `FragmentExec`-based plan (lines 494-541)
5. Runs a **node-level reduction** (INTERMEDIATE aggregate) that merges shard outputs before sending to coordinator (lines 690-721)

### External Source Path: handleExternalSourceRequest (lines 792-879)

For external source queries:
1. Takes the plan (`ExchangeSinkExec` wrapping the data node work)
2. **Injects splits** (line 807-808): `sinkExec.child().transformUp(ExternalSourceExec.class, exec -> exec.withSplits(request.externalSplits()))`
3. Creates compute context with `EmptyIndexedByShardId.instance()` -- no search contexts (line 837)
4. Runs compute directly with **NO shard batching, NO node-level reduction** (lines 843-866)
5. Uses **only one exchange layer** (node-to-coordinator)

### Coordinator Dispatch: startExternalComputeOnDataNodes (lines 247-400)

The coordinator iterates through `distributionPlan.nodeAssignments()` and sends a `DataNodeRequest` to each node with:
- Empty shards list: `List.of()` (line 356)
- The assigned external splits: `nodeSplits` (line 362)
- The plan wrapped in ExchangeSinkExec (line 357)

### Comparison Table

| Aspect | ES Shard Path | External Source Path |
|--------|--------------|---------------------|
| Search contexts | Acquired per shard | None (`EmptyIndexedByShardId`) |
| Batching | `maxConcurrentShards` batches | Single compute call |
| Node-level reduction | Yes (INTERMEDIATE aggregate) | **No reduction** |
| Exchange layers | 2 (internal + external) | 1 (external only) |
| Plan transformation | Shard-specific SearchContext | Split injection via `withSplits()` |
| Error handling | Shard-level failure tracking | Simple compute-level errors |
| Parallelism | Shard-per-pipeline or per-batch | All splits in one compute |

---

## Summary: The Complete Picture

### For ES Index Queries (`FROM logs | STATS count(*) BY f`):
```
Coordinator:                    Data Node (per node):
OutputExec                      ExchangeSinkExec [external]
  AggregateExec[FINAL]            AggregateExec[INTERMEDIATE] [node reduction]
    ExchangeSourceExec              ExchangeSourceExec [internal]
                                      ExchangeSinkExec [internal]
                                        AggregateExec[INITIAL]    [per shard]
                                          EsSourceExec (from FragmentExec)
```
- **Two-phase** aggregation: INITIAL on data node shards, FINAL on coordinator, with optional INTERMEDIATE node-level reduction
- Exchange boundary created by Mapper's `addExchangeForFragment` when it detects `FragmentExec`
- Multiple data nodes run in parallel

### For External Source Queries (`EXTERNAL "s3://..." | STATS count(*) BY f`) -- Current State:
```
Coordinator only:
OutputExec
  AggregateExec[SINGLE]
    ExternalSourceExec[splits=all]
```
- **Single-phase** aggregation on coordinator
- No exchange, no data node plan
- All work happens on one node (coordinator)

### For External Source Queries -- When Distribution Is Enabled (requires Mapper changes):
```
Coordinator:                    Data Node (per node):
OutputExec                      ExchangeSinkExec
  AggregateExec[FINAL]            ExternalSourceExec[splits=assigned]
    ExchangeSourceExec
```
- Distribution sends raw (unaggregated) pages back from data nodes
- **No node-level reduction** (unlike ES shard path)
- No INITIAL aggregate on data nodes -- data nodes read and send raw data
- Coordinator does the full aggregation

### The Gap

The fundamental issue is that the Mapper uses `FragmentExec` as the **sole signal** to insert exchanges and create two-phase aggregation. Since `ExternalRelation` maps directly to `ExternalSourceExec` (not `FragmentExec`), the entire exchange/two-phase machinery is bypassed.

**Impact on specific operations:**
1. **STATS/aggregation**: Always SINGLE-mode on coordinator (no partial aggregation on data nodes)
2. **TopN (SORT+LIMIT)**: No TopN pushdown to data nodes
3. **LIMIT**: No limit pushdown to data nodes
4. **Distribution**: Infrastructure exists but Mapper doesn't produce the ExchangeExec needed to trigger it

**To enable two-phase aggregation for external sources**, one would need to:
- (a) Modify `Mapper.addExchangeForFragment` to also detect `ExternalSourceExec` and insert an exchange
- (b) Or create a new `ExternalFragmentExec` node that triggers the same exchange insertion
- (c) Or insert the exchange in a new optimizer pass between the Mapper and `executePlan`

Option (a) is simplest: change the condition from `child instanceof FragmentExec` to `child instanceof FragmentExec || child instanceof ExternalSourceExec`.

But this alone wouldn't give INITIAL/FINAL two-phase on data nodes because the `LocalMapper` handles `ExternalRelation` -> `ExternalSourceExec` (not to `EsSourceExec`), so the data-node local plan would just be `ExternalSourceExec` with no partial aggregation. To get actual two-phase execution, the exchange insertion would need to also wrap the aggregate into the data-node plan, similar to how `FragmentExec` carries the aggregate to be replanned by `LocalMapper` on data nodes.
