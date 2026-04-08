# Deep Dive #24: Load-Aware Distribution for External Sources

## Executive Summary

The current external-source distribution uses three strategies -- round-robin, weighted round-robin (LPT algorithm on split byte size), and an adaptive strategy that picks between them. None considers runtime node metrics (CPU, heap, queue depth, active ESQL tasks). Elasticsearch already has a mature load-aware routing system for search (adaptive replica selection via `ResponseCollectorService`), but it operates at the shard-routing level and is not wired into the external-source distribution path. Integrating load-awareness is a medium-complexity task that involves: (1) obtaining node-level load signals at distribution time, (2) incorporating those signals into the weight calculation of `WeightedRoundRobinStrategy`, and (3) building a feedback loop so completed external-source tasks update the load model.

---

## 1. Current Distribution Architecture

### 1.1 Strategy Interface

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionStrategy.java`

```java
public interface ExternalDistributionStrategy {
    ExternalDistributionPlan planDistribution(ExternalDistributionContext context);
}
```

Single method. Takes context, returns plan. The plan is either `LOCAL` (coordinator-only) or a `Map<String, List<ExternalSplit>>` mapping node IDs to their assigned splits.

### 1.2 Context (Inputs Available to Any Strategy)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionContext.java`

```java
public record ExternalDistributionContext(
    PhysicalPlan plan,
    List<ExternalSplit> splits,
    DiscoveryNodes availableNodes,
    QueryPragmas pragmas
) {}
```

**What is available:**
- `PhysicalPlan plan` -- the physical plan tree (used to detect aggregation, limit, topN)
- `List<ExternalSplit> splits` -- the splits to distribute, each with `estimatedSizeInBytes()` (may be -1)
- `DiscoveryNodes availableNodes` -- all discovery nodes in the cluster (roles, addresses, attributes). **No runtime stats (CPU, heap, queue depth).**
- `QueryPragmas pragmas` -- user-provided pragma overrides

**What is NOT available:** Any node-level runtime metrics. No `NodeStats`, no `ResponseCollectorService.ComputedNodeStats`, no thread pool stats, no circuit breaker stats. The context is purely structural.

### 1.3 Distribution Plan (Output)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionPlan.java`

```java
public record ExternalDistributionPlan(
    Map<String, List<ExternalSplit>> nodeAssignments,
    boolean distributed
) {
    public static final ExternalDistributionPlan LOCAL = new ExternalDistributionPlan(Map.of(), false);
}
```

Simple mapping from node ID to list of splits. Consumed by `DataNodeComputeHandler.startExternalComputeOnDataNodes()` which iterates over the map and sends a `DataNodeRequest` (with `externalSplits` field) to each node.

### 1.4 AdaptiveStrategy

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/AdaptiveStrategy.java` (85 lines)

Decision flow:
1. **Single split or zero splits** --> `LOCAL` (lines 44-46)
2. **LIMIT-only plan** (limit but no agg, no topN) --> `LOCAL` (lines 50-51)
3. **No eligible nodes** --> `LOCAL` (lines 54-57)
4. **Has aggregation OR split count > node count** --> distribute:
   - If all splits have `estimatedSizeInBytes() > 0` --> `WeightedRoundRobinStrategy.assignByWeight()` (line 71)
   - Otherwise --> `RoundRobinStrategy.assignRoundRobin()` (line 73)
5. **Otherwise** (few splits, no aggregation) --> `LOCAL`

Uses `NodeEligibilityStrategy.DATA_NODES_ONLY` by default -- filters to `canContainData()` nodes.

### 1.5 WeightedRoundRobinStrategy (LPT Algorithm)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/WeightedRoundRobinStrategy.java` (lines 67-89)

```java
static ExternalDistributionPlan assignByWeight(List<ExternalSplit> splits, List<DiscoveryNode> nodes) {
    List<ExternalSplit> sorted = new ArrayList<>(splits);
    sorted.sort(Comparator.comparingLong(ExternalSplit::estimatedSizeInBytes).reversed());

    Map<String, List<ExternalSplit>> assignments = new LinkedHashMap<>();
    long[] nodeLoads = new long[nodes.size()];  // <-- THIS is where load-awareness would plug in
    for (DiscoveryNode node : nodes) {
        assignments.put(node.getId(), new ArrayList<>());
    }

    for (ExternalSplit split : sorted) {
        int minIdx = 0;
        for (int i = 1; i < nodeLoads.length; i++) {
            if (nodeLoads[i] < nodeLoads[minIdx]) {
                minIdx = i;
            }
        }
        assignments.get(nodes.get(minIdx).getId()).add(split);
        nodeLoads[minIdx] += split.estimatedSizeInBytes();
    }
    return new ExternalDistributionPlan(assignments, true);
}
```

**Key observation:** The `nodeLoads` array starts at zero for all nodes. The only weight added is the split byte size. This is the natural injection point for pre-existing node load. If `nodeLoads[i]` were initialized to a value reflecting current node busyness, the LPT algorithm would automatically assign fewer splits to heavily loaded nodes.

### 1.6 RoundRobinStrategy

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/RoundRobinStrategy.java` (lines 52-62)

Pure modular round-robin (`splits[i]` goes to `nodes[i % nodes.size()]`). No weighting at all. Used as fallback when split size is unknown.

### 1.7 NodeEligibilityStrategy

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/NodeEligibilityStrategy.java`

Two built-in implementations:
- `DATA_NODES_ONLY` -- filters for `canContainData()` (default)
- `ALL_NODES` -- includes all nodes

Only looks at node roles. No load-based exclusion (e.g., exclude nodes above 90% heap).

### 1.8 Strategy Selection

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java` (lines 245-257)

Resolved from `QueryPragmas.externalDistribution()`:
- `""` or `"adaptive"` --> `AdaptiveStrategy`
- `"coordinator_only"` --> `CoordinatorOnlyStrategy`
- `"round_robin"` --> `RoundRobinStrategy`
- `"weighted_round_robin"` --> `WeightedRoundRobinStrategy`
- anything else --> warning + `AdaptiveStrategy`

---

## 2. Split Discovery and Assignment Flow

### 2.1 Split Discovery

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/SplitDiscoveryPhase.java`

1. `resolveExternalSplits(plan, sourceFactories)` walks the plan tree
2. For each `ExternalSourceExec`, gets the `SplitProvider` from the factory
3. Calls `splitProvider.discoverSplits(context)` which returns `List<ExternalSplit>`
4. Replaces `ExternalSourceExec` with `exec.withSplits(splits)`

### 2.2 Coalescing

**File:** `ComputeService.java` (lines 231-243)

After split discovery, `coalesceSplits()` merges small splits via `SplitCoalescer`.

### 2.3 Distribution Decision

**File:** `ComputeService.java` (lines 259-285)

`applyExternalDistributionStrategy()`:
1. Collects all splits from the plan (`collectExternalSplits`)
2. Builds `ExternalDistributionContext` with `clusterService.state().nodes()`
3. Calls `strategy.planDistribution(context)`
4. Returns `ExternalDistributionResult`

### 2.4 Dispatch to Data Nodes

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/DataNodeComputeHandler.java` (lines 247-400)

`startExternalComputeOnDataNodes()`:
1. Iterates `distributionPlan.nodeAssignments()` entries
2. For each node with splits:
   - Looks up `DiscoveryNode` from cluster state
   - Opens transport connection
   - Opens exchange session
   - Sends `DataNodeRequest` with `externalSplits` list
3. Node-level failures handled per `allowPartialResults` setting

### 2.5 Serialization of Split Assignments

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/DataNodeRequest.java`

`DataNodeRequest` carries a `List<ExternalSplit> externalSplits` field, serialized as `NamedWriteable` objects via `writeNamedWriteableCollection` / `readNamedWriteableCollectionAsList`. Each node gets only its own splits -- the split-to-node mapping is resolved on the coordinator before dispatch.

On the receiving data node, `DataNodeComputeHandler.handleExternalSourceRequest()` (line 792) injects the splits into the plan's `ExternalSourceExec` via `exec.withSplits(request.externalSplits())`.

---

## 3. Available Node Metrics in Elasticsearch

### 3.1 OsStats (OS-level metrics)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/monitor/os/OsStats.java`

- **`Cpu.getPercent()`** -- system CPU usage percentage (short, 0-100)
- **`Cpu.getLoadAverage()`** -- 1m, 5m, 15m load averages (double[3])
- **`Cpu.getAvailableProcessors()`** -- number of available CPU cores
- **`Mem.getTotal()`** -- total physical memory (bytes)
- **`Mem.getFree()`** -- free physical memory (bytes)
- **`Mem.getUsed()`** -- used physical memory (total - free)

### 3.2 JvmStats (JVM heap metrics)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/monitor/jvm/JvmStats.java`

- **`Mem.getHeapUsed()`** -- current heap usage (bytes)
- **`Mem.getHeapMax()`** -- maximum heap size (bytes)
- **`Mem.getHeapUsedPercent()`** -- heap usage percentage (short, 0-100)
- **`Mem.getHeapCommitted()`** -- committed heap (bytes)
- **`Mem.getNonHeapUsed()`** -- non-heap memory usage

### 3.3 ThreadPoolStats (thread pool metrics)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/threadpool/ThreadPoolStats.java`

Per thread pool (by name):
- **`Stats.threads`** -- total threads
- **`Stats.queue`** -- current queue depth
- **`Stats.active`** -- currently active threads
- **`Stats.rejected`** -- total rejected count
- **`Stats.largest`** -- peak thread count
- **`Stats.completed`** -- total completed tasks

Relevant pool names: `esql_worker` (defined at `EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME`), `search`.

### 3.4 CircuitBreakerStats

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/indices/breaker/CircuitBreakerStats.java`

Per breaker (request, fielddata, in_flight_requests, parent):
- **`getLimit()`** -- breaker limit (bytes)
- **`getEstimated()`** -- current estimated usage (bytes)
- **`getTrippedCount()`** -- number of times tripped
- **`getOverhead()`** -- overhead multiplier

### 3.5 ResponseCollectorService (Adaptive Replica Selection)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/node/ResponseCollectorService.java`

This is the most mature load-tracking system in ES. It tracks per-node:
- **Queue size** (EWMA, alpha=0.3)
- **Response time** (EWMA, alpha=0.3)
- **Service time** (EWMA)

And provides a **rank** for each node based on the C3 paper formula (NSDI 2015):
```
rank = responseTime - serviceTime + queueHat^3 * serviceTime
```

Where `queueHat = 1 + concurrencyCompensation + queueSize_EWMA`.

**Key methods:**
- `getAllNodeStatistics()` -- returns `Map<String, ComputedNodeStats>` for all known nodes
- `getNodeStatistics(nodeId)` -- returns `Optional<ComputedNodeStats>` for a specific node
- `addNodeStatistics(nodeId, queueSize, responseTime, serviceTime)` -- updates stats after a completed request

**Current limitation:** Stats are only fed from **search** operations via `SearchExecutionStatsCollector` (file: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/search/SearchExecutionStatsCollector.java`). External source executions do not feed back into this service.

### 3.6 NodeStats (Aggregated Node Statistics)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/admin/cluster/node/stats/NodeStats.java`

Aggregates all the above into a single object per node:
- `OsStats os`
- `JvmStats jvm`
- `ThreadPoolStats threadPool`
- `AllCircuitBreakerStats breaker`
- `AdaptiveSelectionStats adaptiveSelectionStats`
- `ProcessStats process`
- `TransportStats transport`
- etc.

Available via `NodeService.stats()` which is a **local call** on each node. On the coordinator, getting stats from remote nodes requires `TransportNodesStatsAction` which is an RPC -- too expensive for per-query distribution decisions.

---

## 4. Existing Load-Aware Pattern: Adaptive Replica Selection

### 4.1 How It Works

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/routing/OperationRouting.java` (lines 312-322)

For regular search, `OperationRouting.shardRoutings()` calls `indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts)` when `useAdaptiveReplicaSelection` is true (which it is by default).

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/routing/IndexShardRoutingTable.java` (lines 245-260)

`activeInitializingShardsRankedIt()`:
1. Gets per-node `ComputedNodeStats` from `ResponseCollectorService`
2. Ranks nodes by the C3 formula
3. Sorts shard routing entries by rank (lowest rank = most preferred)
4. Adjusts non-winner stats upward so they get a chance on future queries

### 4.2 Feedback Loop

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/search/SearchExecutionStatsCollector.java`

After each search phase result returns:
1. Extracts `serviceTimeEWMA` and `nodeQueueSize` piggybacked on `QuerySearchResult`
2. Computes `responseDuration = now - startNanos`
3. Calls `collector.addNodeStatistics(nodeId, queueSize, responseDuration, serviceTimeEWMA)`

This creates a continuous feedback loop: every search response updates the model used for future routing decisions.

### 4.3 Key Design Properties

- **No RPC needed** -- stats are piggybacked on existing search responses (zero extra network cost)
- **EWMA smoothing** -- prevents oscillation from momentary spikes (alpha=0.3)
- **Concurrency compensation** -- accounts for outstanding requests, prevents thundering herd
- **Winner adjustment** -- non-selected nodes get synthetic stat improvements to prevent starvation

---

## 5. Integration Analysis: Where Load-Awareness Plugs In

### 5.1 Option A: Initialize `nodeLoads` in WeightedRoundRobinStrategy (Simplest)

**Location:** `WeightedRoundRobinStrategy.assignByWeight()` lines 72-73

Currently:
```java
long[] nodeLoads = new long[nodes.size()];
// All zeros -- no pre-existing load
```

With load-awareness:
```java
long[] nodeLoads = new long[nodes.size()];
for (int i = 0; i < nodes.size(); i++) {
    nodeLoads[i] = loadModel.getNodeLoad(nodes.get(i).getId());
}
```

The LPT algorithm already picks the node with minimum load for each split. Pre-seeding `nodeLoads` with a value proportional to current busyness would naturally steer splits away from loaded nodes. The load value needs to be in the same unit-space as `estimatedSizeInBytes` (bytes), so it would be a scaled/normalized value.

### 5.2 Option B: Extend ExternalDistributionContext with Load Info

**Location:** `ExternalDistributionContext` record

Add a field:
```java
public record ExternalDistributionContext(
    PhysicalPlan plan,
    List<ExternalSplit> splits,
    DiscoveryNodes availableNodes,
    QueryPragmas pragmas,
    Map<String, NodeLoadSnapshot> nodeLoads  // NEW
) {}
```

`NodeLoadSnapshot` would carry: heap used percent, esql_worker queue depth, CPU percent, and/or `ResponseCollectorService.ComputedNodeStats`.

The context is constructed in `ComputeService.applyExternalDistributionStrategy()` (line 266). The `ComputeService` already has `clusterService` injected and could also receive `ResponseCollectorService` or a new `ExternalSourceLoadTracker`.

### 5.3 Option C: Reuse ResponseCollectorService Directly

The `ResponseCollectorService` instance is available at the coordinator (it's a singleton created in `NodeConstruction`). It could be injected into `ComputeService` and passed into `ExternalDistributionContext`. The `ComputedNodeStats.rank()` value directly provides a node quality score.

**Caveat:** Currently only populated by search operations. External source executions need a feedback mechanism to also call `addNodeStatistics()` after completion.

### 5.4 Option D: Node Exclusion in NodeEligibilityStrategy

**Location:** `NodeEligibilityStrategy` interface

Add a load-based filter:
```java
NodeEligibilityStrategy HEALTHY_DATA_NODES = allNodes -> {
    // Filter out nodes with heap > 90% or circuit breaker > 85%
    // Requires access to NodeStats
};
```

This would prevent assigning any splits to critically loaded nodes, complementing Option A/B/C which distributes among eligible nodes.

---

## 6. What Metrics to Use (Recommendation)

### 6.1 Primary Signal: ESQL Worker Thread Pool Queue Depth

The `esql_worker` thread pool is the execution pool for ESQL drivers. Its queue depth directly indicates how backlogged a node is with ESQL work. Available via `ThreadPoolStats.Stats.queue` and `ThreadPoolStats.Stats.active`.

**Problem:** This is a local-only metric. Getting it from remote nodes requires an RPC or piggybacking.

### 6.2 Secondary Signal: ResponseCollectorService EWMA Stats

Already available on the coordinator for all nodes (no RPC needed). Provides:
- Queue size EWMA
- Response time EWMA
- Service time EWMA
- Computed rank

**Advantage:** Zero network overhead, EWMA-smoothed, proven in production for search routing.

**Disadvantage:** Currently only fed by search operations, not ESQL external source operations.

### 6.3 Tertiary Signal: JVM Heap Usage Percent

Available locally but not remotely without RPC. Could be piggybacked on `DataNodeComputeResponse`.

### 6.4 Recommended Approach: Piggybacked Stats + ResponseCollectorService

1. Modify `DataNodeComputeResponse` to include a `NodeLoadSnapshot` (heap%, esql_worker queue depth, CPU%)
2. After external source compute completes, feed these stats into `ResponseCollectorService` or a parallel `ExternalSourceLoadTracker`
3. On the next query, the coordinator reads accumulated EWMA stats and passes them into the distribution context
4. `WeightedRoundRobinStrategy` initializes `nodeLoads[i]` based on the load signal

This mirrors exactly how adaptive replica selection works for search, but for external source distribution.

---

## 7. Complexity Assessment

### 7.1 Minimal Viable Implementation (~2-3 weeks H+AI)

1. **Add `ResponseCollectorService` injection to `ComputeService`** -- ~1 hour. Wire it through `TransportActionServices`.

2. **Extend `ExternalDistributionContext`** -- ~2 hours. Add `Map<String, ResponseCollectorService.ComputedNodeStats>` field. Populate from `ResponseCollectorService.getAllNodeStatistics()` in `applyExternalDistributionStrategy()`.

3. **Create `LoadAwareWeightedStrategy`** or modify `WeightedRoundRobinStrategy` -- ~1-2 days. Initialize `nodeLoads[i]` from `ComputedNodeStats.rank()` scaled to bytes-equivalent units.

4. **Add feedback loop: piggyback stats on `DataNodeComputeResponse`** -- ~2-3 days. Add fields to response. On receiving response, call `ResponseCollectorService.addNodeStatistics()` or a custom tracker.

5. **Add load-based exclusion to `NodeEligibilityStrategy`** -- ~1 day. Skip nodes above configurable heap threshold.

6. **Tests** -- ~3-5 days. Unit tests for the strategy, integration tests verifying load-aware behavior, property tests.

7. **Pragmas/settings** -- ~1 day. Add `external_distribution: load_aware` pragma value, configurable thresholds.

### 7.2 Challenges

- **Cold start:** On the first external-source query, there are no stats. Falls back to current behavior (round-robin or LPT by size). Acceptable.
- **Scale mismatch:** `ResponseCollectorService` stats are in nanoseconds/queue-count. `nodeLoads` is in bytes. Need a normalization function. A simple approach: use rank as a multiplier on the median split size.
- **ESQL vs. search contention:** Search operations are much more frequent than external-source operations. Search load dominates the ResponseCollectorService signal. This might actually be fine -- a node busy with search should also get fewer external-source splits.
- **Stale stats:** If external source queries are infrequent, EWMA stats may be stale. The alpha=0.3 decay handles this to some extent. Alternatively, piggyback on all ESQL operations (not just external sources).

### 7.3 No-Go Approach: Real-Time Node Stats RPC

Calling `TransportNodesStatsAction` before every query to get fresh CPU/heap/queue stats from all nodes would add ~5-50ms of latency per query. This is too expensive for a planning step and creates a scaling bottleneck. The piggybacked EWMA approach avoids this entirely.

---

## 8. Key Files Summary

| File | Role |
|------|------|
| `.../plugin/ExternalDistributionStrategy.java` | Strategy interface |
| `.../plugin/ExternalDistributionContext.java` | Strategy input (plan, splits, nodes, pragmas) |
| `.../plugin/ExternalDistributionPlan.java` | Strategy output (node->splits mapping) |
| `.../plugin/AdaptiveStrategy.java` | Decision logic: when to distribute, which sub-strategy |
| `.../plugin/WeightedRoundRobinStrategy.java` | LPT algorithm -- **primary injection point for load** |
| `.../plugin/RoundRobinStrategy.java` | Pure round-robin fallback |
| `.../plugin/NodeEligibilityStrategy.java` | Node filtering (role-based, could add load-based) |
| `.../plugin/ComputeService.java` L259-285 | Orchestrates distribution strategy invocation |
| `.../plugin/DataNodeComputeHandler.java` L247-400 | Dispatches splits to data nodes |
| `.../plugin/DataNodeRequest.java` | Carries `externalSplits` over transport |
| `.../node/ResponseCollectorService.java` | EWMA load tracking for adaptive replica selection |
| `.../search/SearchExecutionStatsCollector.java` | Feedback loop: search responses update load model |
| `.../routing/IndexShardRoutingTable.java` L245-260 | How ARS ranks nodes for shard routing |
| `.../routing/OperationRouting.java` L312-322 | Where ARS is invoked for search |
| `.../monitor/os/OsStats.java` | CPU%, load average, memory stats |
| `.../monitor/jvm/JvmStats.java` | Heap usage, GC stats |
| `.../threadpool/ThreadPoolStats.java` | Per-pool: threads, queue, active, rejected |
| `.../breaker/CircuitBreakerStats.java` | Breaker limit, estimated, tripped count |

All file paths are relative to `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/` (server module) or `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/` (ESQL module).

---

## 9. Verdict

**Effort estimate: 2-3 weeks H+AI** for a solid implementation with piggybacked stats, EWMA load tracking, and weight-biased LPT distribution. The architecture is well-suited for this enhancement -- the `WeightedRoundRobinStrategy.assignByWeight()` `nodeLoads` array is literally the variable that needs a non-zero initial value. The `ResponseCollectorService` provides a proven, production-hardened load-tracking pattern to follow. The main work is plumbing the feedback loop (piggybacking stats on `DataNodeComputeResponse`) and scaling the load signal to be compatible with the byte-based LPT weights.
