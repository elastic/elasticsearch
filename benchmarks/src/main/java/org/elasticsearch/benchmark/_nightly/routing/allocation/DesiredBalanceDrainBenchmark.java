/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark._nightly.routing.allocation;

import org.elasticsearch.benchmark.ExtraParam;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoveryPriority;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocatorMetrics;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceComputer;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceInput;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.allocation.StatelessShardRoutingRoleStrategy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The desired balance computation while a stateless cluster drains: the old index and search nodes cannot remain, empty new nodes of both
 * tiers exist, and the stateless allocation deciders and balancing weights are used with the settings of a default serverless
 * deployment. One call to {@link DesiredBalanceComputer#compute} runs until the desired balance has converged, so the score is the cost of
 * computing the whole drain, which is what a rollout of the cluster waits for.
 * <p>
 * The {@code layout} parameter describes the cluster. The default is a cluster of about 11,000 shards, and the extra layouts are about
 * 41,000 shards (the size of a large project), and the same cluster with every shard already moved, where the computation has nothing
 * to do.
 */
@Fork(1)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 3, time = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class DesiredBalanceDrainBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    // Do NOT make any field final (even if it is not annotated with @Param)! See also
    // http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/elasticsearch/JMHSample_10_ConstantFold.java
    @Param(
        {
            // big indices| primaries per big index| small indices| primaries per small index| old index nodes| new index nodes|
            // old search nodes| new search nodes| percent already moved
            "      20|  60|  1500|  3|  4|  4|  20|  20|  0" }
    )
    @ExtraParam({ "      80| 100|  6457|  2|  7|  8|  45|  45|  0", "      80| 100|  6457|  2|  7|  8|  45|  45|  100" })
    public String layout = "20|60|1500|3|4|4|20|20|0";

    /**
     * The most iterations of the computation, by default enough for it to converge. A computation that moves one shard per iteration
     * takes far longer to converge, so a lower value measures the cost of that many iterations instead.
     */
    @Param({ "100000" })
    @ExtraParam({ "100" })
    public int iterations = 100_000;

    /** {@code cluster.routing.allocation.desired_balance.max_moves_per_iteration}: the most relocations in one call, unlimited by default. */
    @Param({ "2147483647" })
    @ExtraParam({ "256", "10000" })
    public int movesPerIteration = Integer.MAX_VALUE;

    /**
     * {@code all} enables the write load, estimated heap and shared cache capacity deciders (otherwise they return YES immediately)
     * and uses the matching ClusterInfo; {@code default} leaves them at their release defaults. Serverless projects enable them through
     * dynamic cluster settings.
     */
    @Param({ "all" })
    @ExtraParam({ "default" })
    public String deciderMode = "all";

    private DesiredBalanceComputer computer;
    private DesiredBalanceInput input;

    @Setup
    public void setUp() {
        final String[] p = layout.split("\\|");
        final int bigIndices = Integer.parseInt(p[0].trim());
        final int bigPrimaries = Integer.parseInt(p[1].trim());
        final int smallIndices = Integer.parseInt(p[2].trim());
        final int smallPrimaries = Integer.parseInt(p[3].trim());
        final int oldIndexNodes = Integer.parseInt(p[4].trim());
        final int newIndexNodes = Integer.parseInt(p[5].trim());
        final int oldSearchNodes = Integer.parseInt(p[6].trim());
        final int newSearchNodes = Integer.parseInt(p[7].trim());
        final int percentMoved = Integer.parseInt(p[8].trim());

        final boolean allDeciders = deciderMode.equals("all");
        // The allocation settings of a default serverless deployment, as in distribution/archives/src/serverless-default-settings.yml of
        // the serverless repository, which this repository cannot depend on. The balancing weights are the stateless defaults.
        final Settings settings = Settings.builder()
            .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
            .put("cluster.routing.allocation.write_load_decider.enabled", allDeciders ? "enabled" : "disabled")
            .put("cluster.routing.allocation.estimated_heap.threshold_enabled", allDeciders)
            .put("cluster.routing.allocation.shared_cache_capacity.enabled", allDeciders)
            .put("cluster.routing.allocation.index_balance_decider.enabled", true)
            .put("cluster.routing.allocation.desired_balance.enable_balancer_round_summaries", true)
            .put("cluster.routing.allocation.desired_balance.max_moves_per_iteration", movesPerIteration)
            .put("cluster.routing.allocation.node_concurrent_recoveries", 64)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 64)
            .put("cluster.routing.allocation.min_heap_required_for_concurrent_primary_recoveries", "0b")
            .put("cluster.routing.allocation.concurrent_primary_recoveries_per_heap_gb", 1_000_000)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 64)
            .put("indices.recovery.max_concurrent_incoming_recoveries", 32)
            .put("indices.recovery.max_concurrent_outgoing_recoveries", 16)
            .build();
        final StatelessPlugin plugin = new StatelessPlugin(settings);
        final Set<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.addAll(plugin.getSettings());
        final ClusterSettings clusterSettings = new ClusterSettings(settings, allSettings);

        // nodes
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        final List<String> oldNodeIds = new ArrayList<>();
        final List<String> indexNodeIds = new ArrayList<>();
        final List<String> searchNodeIds = new ArrayList<>();
        final List<String> newIndexNodeIds = new ArrayList<>();
        final List<String> newSearchNodeIds = new ArrayList<>();
        final List<String> oldIndexNodeIds = new ArrayList<>();
        final List<String> oldSearchNodeIds = new ArrayList<>();
        int port = 1;
        for (int i = 0; i < oldIndexNodes + newIndexNodes; i++) {
            final String id = (i < oldIndexNodes ? "old-index-" : "new-index-") + i;
            nodes.add(newNode(id, port++, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.MASTER_ROLE));
            indexNodeIds.add(id);
            (i < oldIndexNodes ? oldIndexNodeIds : newIndexNodeIds).add(id);
            if (i < oldIndexNodes) {
                oldNodeIds.add(id);
            }
        }
        for (int i = 0; i < oldSearchNodes + newSearchNodes; i++) {
            final String id = (i < oldSearchNodes ? "old-search-" : "new-search-") + i;
            nodes.add(newNode(id, port++, DiscoveryNodeRole.SEARCH_ROLE));
            searchNodeIds.add(id);
            (i < oldSearchNodes ? oldSearchNodeIds : newSearchNodeIds).add(id);
            if (i < oldSearchNodes) {
                oldNodeIds.add(id);
            }
        }

        // indices and routing table, with every shard started
        final ProjectMetadata.Builder project = ProjectMetadata.builder(ProjectId.DEFAULT);
        final RoutingTable.Builder routingTable = RoutingTable.builder(new StatelessShardRoutingRoleStrategy());
        final Map<String, Long> shardSizes = new HashMap<>();
        final Map<ShardId, ShardAndIndexHeapUsage> heapUsages = new HashMap<>();
        final Map<ClusterInfo.NodeAndShard, String> dataPaths = new HashMap<>();
        final Map<ShardId, Long> dataSetSizes = new HashMap<>();
        final Map<ShardId, Double> writeLoads = new HashMap<>();
        final Map<ShardId, BoostedAndUnboostedCacheRequirements> cacheRequirements = new HashMap<>();
        final Map<String, Long> boostedCommitments = new HashMap<>();
        final Map<String, Long> unboostedCommitments = new HashMap<>();
        final BoostedAndUnboostedCacheRequirements cacheRequirement = new BoostedAndUnboostedCacheRequirements(
            ByteSizeValue.ofMb(20).getBytes(),
            ByteSizeValue.ofMb(100).getBytes()
        );
        final long shardSize = ByteSizeValue.ofGb(2).getBytes();
        final int numIndices = bigIndices + smallIndices;
        int shardCounter = 0;
        for (int i = 0; i < numIndices; i++) {
            final int primaries = i < bigIndices ? bigPrimaries : smallPrimaries;
            final IndexMetadata indexMetadata = IndexMetadata.builder("index-" + i)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(primaries)
                .numberOfReplicas(1)
                .build();
            project.put(indexMetadata, false);
            final Index index = indexMetadata.getIndex();
            final IndexRoutingTable.Builder indexRouting = new IndexRoutingTable.Builder(new StatelessShardRoutingRoleStrategy(), index);
            for (int s = 0; s < primaries; s++) {
                final ShardId shardId = new ShardId(index, s);
                final boolean moved = (shardCounter % 100) < percentMoved;
                final String primaryNode = moved
                    ? newIndexNodeIds.get(shardCounter % newIndexNodeIds.size())
                    : oldIndexNodeIds.get(shardCounter % oldIndexNodeIds.size());
                final String replicaNode = moved
                    ? newSearchNodeIds.get(shardCounter % newSearchNodeIds.size())
                    : oldSearchNodeIds.get(shardCounter % oldSearchNodeIds.size());
                shardCounter++;
                final IndexShardRoutingTable.Builder shard = new IndexShardRoutingTable.Builder(shardId);
                shard.addShard(startedShard(shardId, true, primaryNode, shardSize));
                shard.addShard(startedShard(shardId, false, replicaNode, shardSize));
                indexRouting.addIndexShard(shard);
                shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shardId, true), shardSize);
                shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shardId, false), shardSize);
                heapUsages.put(shardId, new ShardAndIndexHeapUsage(4_000_000L, 2_000_000L, 1_000_000L));
                dataPaths.put(new ClusterInfo.NodeAndShard(primaryNode, shardId), "/data");
                dataPaths.put(new ClusterInfo.NodeAndShard(replicaNode, shardId), "/data");
                dataSetSizes.put(shardId, shardSize);
                writeLoads.put(shardId, 0.05 * (1 + s % 5));
                cacheRequirements.put(shardId, cacheRequirement);
                boostedCommitments.merge(replicaNode, cacheRequirement.boostedCacheRequirementInBytes(), Long::sum);
                unboostedCommitments.merge(replicaNode, cacheRequirement.unboostedCacheRequirementInBytes(), Long::sum);
            }
            routingTable.add(indexRouting);
        }

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(project))
            .routingTable(GlobalRoutingTable.builder().put(ProjectId.DEFAULT, routingTable.build()).build())
            .nodes(nodes.localNodeId(indexNodeIds.get(0)).masterNodeId(indexNodeIds.get(0)))
            .build();

        // cluster info as the collectors would publish it
        final Map<String, DiskUsage> diskUsages = new HashMap<>();
        final Map<String, NodeHeapMetrics> heapMetrics = new HashMap<>();
        final Map<String, ByteSizeValue> maxHeap = new HashMap<>();
        final long heapBytes = ByteSizeValue.ofGb(32).getBytes();
        // large enough that the disk watermarks, which are enabled by default, do not stop the new nodes taking the whole cluster
        final long diskBytes = ByteSizeValue.ofTb(100).getBytes();
        final Map<String, NodeUsageStatsForThreadPools> threadPoolUsage = new HashMap<>();
        final Map<String, NodeCacheSizeAndCommitments> cacheCommitments = new HashMap<>();
        final long cacheBytes = ByteSizeValue.ofGb(200).getBytes();
        for (String id : indexNodeIds) {
            diskUsages.put(id, new DiskUsage(id, id, "/data", diskBytes, diskBytes / 2));
            heapMetrics.put(id, new NodeHeapMetrics(id, heapBytes, new NodeHeapEstimates(heapBytes / 2, heapBytes / 4, heapBytes / 8)));
            maxHeap.put(id, ByteSizeValue.ofBytes(heapBytes));
            threadPoolUsage.put(
                id,
                new NodeUsageStatsForThreadPools(
                    id,
                    Map.of(ThreadPool.Names.WRITE, new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(32, 0.4f, 0L))
                )
            );
        }
        for (String id : searchNodeIds) {
            diskUsages.put(id, new DiskUsage(id, id, "/data", diskBytes, diskBytes / 2));
            heapMetrics.put(id, new NodeHeapMetrics(id, heapBytes, new NodeHeapEstimates(0, heapBytes / 4, 0)));
            maxHeap.put(id, ByteSizeValue.ofBytes(heapBytes));
            cacheCommitments.put(
                id,
                new NodeCacheSizeAndCommitments(
                    cacheBytes,
                    boostedCommitments.getOrDefault(id, 0L),
                    unboostedCommitments.getOrDefault(id, 0L)
                )
            );
        }
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .leastAvailableSpaceUsage(diskUsages)
            .mostAvailableSpaceUsage(diskUsages)
            .shardSizes(shardSizes)
            .shardDataSetSizes(dataSetSizes)
            .dataPath(dataPaths)
            .nodeHeapMetrics(heapMetrics)
            .estimatedShardHeapUsages(heapUsages)
            .defaultShardHeapUsageForShardsWithoutMetrics(new ShardAndIndexHeapUsage(4_000_000L, 2_000_000L, 1_000_000L))
            .maxHeapSizePerNode(maxHeap)
            .nodeUsageStatsForThreadPools(threadPoolUsage)
            .shardWriteLoads(writeLoads)
            .shardCacheRequirements(cacheRequirements)
            .nodeCacheSizeAndCommitments(cacheCommitments)
            .build();

        // production stateless deciders and weights, plus a decider that makes the old nodes unable to keep their shards. A node
        // shutdown marker does that in production, but cluster-level allocation filters would also switch the index balance decider off.
        final List<AllocationDecider> deciders = new ArrayList<>(
            ClusterModule.createAllocationDeciders(settings, clusterSettings, List.of(plugin))
        );
        final Set<String> draining = new HashSet<>(oldNodeIds);
        deciders.add(new AllocationDecider() {
            @Override
            public Decision canRemain(
                org.elasticsearch.cluster.metadata.IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return draining.contains(node.nodeId()) ? Decision.NO : Decision.YES;
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                // as with a node shutdown marker, a draining node does not accept shards
                return draining.contains(node.nodeId()) ? Decision.NO : Decision.YES;
            }
        });
        final AllocationDeciders allocationDeciders = new AllocationDeciders(deciders);
        final BalancerSettings balancerSettings = new BalancerSettings(clusterSettings);
        final BalancedShardsAllocator allocator = new BalancedShardsAllocator(
            balancerSettings,
            WriteLoadForecaster.DEFAULT,
            plugin.getBalancingWeightsFactory(balancerSettings, clusterSettings),
            BalancedShardsAllocatorMetrics.NOOP
        );
        final AllocationService allocationService = new AllocationService(
            allocationDeciders,
            allocator,
            () -> ClusterInfo.EMPTY,
            () -> SnapshotShardSizeInfo.EMPTY,
            new StatelessShardRoutingRoleStrategy(),
            MeterRegistry.NOOP,
            clusterSettings
        );
        final RoutingAllocation routingAllocation = allocationService.createAllocationQueryContext(
            clusterState,
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY
        ).routingAllocation();

        computer = new DesiredBalanceComputer(clusterSettings, systemTimeProvider(), allocator, (shard, allocation) -> {
            throw new AssertionError("explainer is not expected to be called");
        });
        input = DesiredBalanceInput.create(1, routingAllocation);

        // Check the computation does what the benchmark describes, which also warms the classes it uses: nothing is left on the nodes
        // that cannot remain shards, and every shard is assigned. A computation stopped after a number of iterations has not finished.
        final DesiredBalance balance = compute();
        if (String.valueOf(balance.finishReason()).equals("CONVERGED") == false) {
            return;
        }
        for (ShardAssignment assignment : balance.assignments().values()) {
            if (assignment.unassigned() != 0) {
                throw new AssertionError("shards are left unassigned: " + assignment);
            }
            for (String nodeId : assignment.nodeIds()) {
                if (draining.contains(nodeId)) {
                    throw new AssertionError("a shard is left on the draining node " + nodeId + ": " + assignment);
                }
            }
        }
    }

    @Benchmark
    public DesiredBalance compute() {
        final AtomicInteger remaining = new AtomicInteger(iterations);
        return computer.compute(new DesiredBalance(1, Map.of()), input, new ArrayDeque<>(), ignored -> remaining.decrementAndGet() > 0);
    }

    private static DiscoveryNode newNode(String id, int port, DiscoveryNodeRole... roles) {
        return new DiscoveryNode(
            null,
            id,
            new TransportAddress(TransportAddress.META_ADDRESS, port),
            Map.of(),
            Set.of(roles),
            (VersionInformation) null
        );
    }

    private static ShardRouting startedShard(ShardId shardId, boolean primary, String nodeId, long size) {
        return ShardRouting.newUnassigned(
            shardId,
            primary,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "benchmark"),
            primary ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY,
            RecoveryPriority.UNKNOWN
        ).initialize(nodeId, null, size).moveToStarted(size);
    }

    private static TimeProvider systemTimeProvider() {
        return new TimeProvider() {
            @Override
            public long relativeTimeInMillis() {
                return System.nanoTime() / 1_000_000;
            }

            @Override
            public long relativeTimeInNanos() {
                return System.nanoTime();
            }

            @Override
            public long rawRelativeTimeInMillis() {
                return System.nanoTime() / 1_000_000;
            }

            @Override
            public long absoluteTimeInMillis() {
                return System.currentTimeMillis();
            }
        };
    }
}
