/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_COLD_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;

public class ClusterAllocationSimulationTests extends ESAllocationTestCase {

    private static final Logger logger = LogManager.getLogger(ClusterAllocationSimulationTests.class);

    private static int nodeCount(
        Random random,
        long tierBytes,
        long nodeBytes,
        double tierWriteLoad,
        double nodeIndexingThreads,
        int minCount
    ) {
        var watermarkNodeBytes = (long) (nodeBytes * 0.85);
        return Math.max(
            Math.toIntExact((tierBytes + watermarkNodeBytes - 1) / watermarkNodeBytes),
            Math.max((int) Math.ceil(tierWriteLoad / nodeIndexingThreads), minCount)
        ) + RandomNumbers.randomIntBetween(random, 0, 2);
    }

    /**
     * This test creates a somewhat realistic cluster and runs the shards allocator until convergence, then reports on the balance quality
     * of the resulting allocation. This is useful for exploring changes to the balancer (although such experiments may want to increase the
     * size of the cluster somewhat) and by running it in CI we can at least be sure that the balancer doesn't throw anything unexpected and
     * does eventually converge in these situations.
     */
    public void testRunSimulation() {

        var settings = Settings.builder()
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.55f)
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.45f)
            .put(BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 10.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 5e-11f)
            .build();

        var results = new ArrayList<Double>();

        for (int i = 0; i < 100; i++) {
            results.add(testBalanceQuality(settings, new Random(42)));
        }

        logger.info("Experiment results {}", new MetricSummary<>(results));
    }

    public void testRunSingleExperiment() {
        var settings = Settings.builder()
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.55f)
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.45f)
            .put(BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 10.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 5e-11f)
            .build();

        testBalanceQuality(settings, new Random(42));
    }

    public double testBalanceQuality(Settings settings, Random random) {

        final var shardSizesByIndex = new HashMap<String, Long>();
        final var tiers = new String[] { DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD };
        final var tierSizes = Arrays.stream(tiers).collect(toMap(Function.identity(), s -> 0L));
        final var tierWriteLoads = Arrays.stream(tiers).collect(toMap(Function.identity(), s -> 0.0));
        final var maxSizeVariance = ByteSizeValue.ofMb(256).getBytes();

        final var shardCountByTier = new HashMap<String, Integer>();

        final var metadataBuilder = Metadata.builder();
        for (var dataStreamIndex = RandomNumbers.randomIntBetween(random, 1, 2); dataStreamIndex >= 0; dataStreamIndex -= 1) {
            final var hotIndices = RandomNumbers.randomIntBetween(random, 2, 10);
            final var warmIndices = RandomNumbers.randomIntBetween(random, 10, 20);
            final var coldIndices = RandomNumbers.randomIntBetween(random, 10, 40);
            final var hotShards = RandomPicks.randomFrom(random, new int[] { 1, 2, 4, 8 });
            final var shrunkShards = RandomPicks.randomFrom(random, new int[] { 1, 2 });
            final var approxIndexSize = RandomPicks.randomFrom(
                random,
                new ByteSizeValue[] { ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(10), ByteSizeValue.ofGb(50) }
            );

            final var indexCount = hotIndices + warmIndices + coldIndices;
            for (var index = 0; index < indexCount; index++) {
                final var tier = index < coldIndices ? DataTier.DATA_COLD
                    : index < coldIndices + warmIndices ? DataTier.DATA_WARM
                    : DataTier.DATA_HOT;
                final var indexName = Strings.format("index-%03d-%s-%03d", dataStreamIndex, tier.charAt(5), index);
                final var shardCount = DataTier.DATA_HOT.equals(tier) ? hotShards : shrunkShards;
                final var replicaCount = DataTier.DATA_COLD.equals(tier) ? 0 : 1;
                final var indexWriteLoad = index == indexCount - 1 ? (RandomNumbers.randomIntBetween(random, 1, 8000) / 1000.0) : 0.0;
                final var shardSize = approxIndexSize.getBytes() / shardCount //
                 + RandomNumbers.randomLongBetween(random, -maxSizeVariance, maxSizeVariance);
                assert shardSize > 0;

                shardCountByTier.compute(tier, (key, previous) -> (previous != null ? previous : 0) + shardCount * (1 + replicaCount));

                metadataBuilder.put(
                    IndexMetadata.builder(indexName)
                        .settings(
                            Settings.builder()
                                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount)
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".fake_tier", tier)
                                .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), tier)
                        )
                        .indexWriteLoadForecast(indexWriteLoad)
                        .shardSizeInBytesForecast(shardSize)
                );

                shardSizesByIndex.put(indexName, shardSize);
                tierSizes.computeIfPresent(tier, (ignored, size) -> size + shardSize * shardCount * (replicaCount + 1));
                tierWriteLoads.computeIfPresent(tier, (ignored, writeLoad) -> writeLoad + indexWriteLoad * shardCount * (replicaCount + 1));
            }
        }
        final var metadata = metadataBuilder.build();

        final var routingTableBuilder = RoutingTable.builder();
        for (final var indexMetadata : metadata) {
            routingTableBuilder.addAsNew(indexMetadata);
        }

        final var nodeSizeBytesByTier = Map.of(
            DataTier.DATA_HOT,
            RandomNumbers.randomLongBetween(random, ByteSizeValue.ofGb(500).getBytes(), ByteSizeValue.ofTb(4).getBytes()),
            DataTier.DATA_WARM,
            RandomNumbers.randomLongBetween(random, ByteSizeValue.ofTb(1).getBytes(), ByteSizeValue.ofTb(10).getBytes()),
            DataTier.DATA_COLD,
            RandomNumbers.randomLongBetween(random, ByteSizeValue.ofTb(1).getBytes(), ByteSizeValue.ofTb(10).getBytes())
        );

        final var indexingThreadsByTier = Map.of(
            DataTier.DATA_HOT,
            8.0/*randomDoubleBetween(4.0, 16.0, true)*/,
            DataTier.DATA_WARM,
            1.0,
            DataTier.DATA_COLD,
            1.0
        );

        final var nodeCountByTier = Map.of(
            DataTier.DATA_HOT,
            nodeCount(
                random,
                tierSizes.get(DataTier.DATA_HOT),
                nodeSizeBytesByTier.get(DataTier.DATA_HOT),
                tierWriteLoads.get(DataTier.DATA_HOT),
                indexingThreadsByTier.get(DataTier.DATA_HOT),
                8
            ),
            DataTier.DATA_WARM,
            nodeCount(
                random,
                tierSizes.get(DataTier.DATA_WARM),
                nodeSizeBytesByTier.get(DataTier.DATA_WARM),
                tierWriteLoads.get(DataTier.DATA_WARM),
                indexingThreadsByTier.get(DataTier.DATA_WARM),
                2
            ),
            DataTier.DATA_COLD,
            nodeCount(
                random,
                tierSizes.get(DataTier.DATA_COLD),
                nodeSizeBytesByTier.get(DataTier.DATA_COLD),
                tierWriteLoads.get(DataTier.DATA_COLD),
                indexingThreadsByTier.get(DataTier.DATA_COLD),
                1
            )
        );

        logger.info("Node count by tier {}", nodeCountByTier);
        logger.info("Shard count by tier {}", shardCountByTier);

        final var discoveryNodesBuilder = new DiscoveryNodes.Builder();
        discoveryNodesBuilder.add(
            new DiscoveryNode("master", "master", buildNewFakeTransportAddress(), Map.of(), Set.of(MASTER_ROLE), Version.CURRENT)
        ).localNodeId("master").masterNodeId("master");
        for (var nodeIndex = 0; nodeIndex < nodeCountByTier.get(DataTier.DATA_HOT) + nodeCountByTier.get(DataTier.DATA_WARM)
            + nodeCountByTier.get(DataTier.DATA_COLD); nodeIndex++) {
            final var tierRole = nodeIndex < nodeCountByTier.get(DataTier.DATA_HOT) ? DATA_HOT_NODE_ROLE
                : nodeIndex < nodeCountByTier.get(DataTier.DATA_HOT) + nodeCountByTier.get(DataTier.DATA_WARM) ? DATA_WARM_NODE_ROLE
                : DATA_COLD_NODE_ROLE;

            final var nodeId = Strings.format("node-%s-%03d", tierRole.roleNameAbbreviation(), nodeIndex);
            discoveryNodesBuilder.add(
                new DiscoveryNode(
                    nodeId,
                    nodeId,
                    buildNewFakeTransportAddress(),
                    Map.of("fake_tier", tierRole.roleName()),
                    Set.of(tierRole),
                    Version.CURRENT
                )
            );
        }

        final var unassignedClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(discoveryNodesBuilder)
            .metadata(metadata)
            .routingTable(routingTableBuilder)
            .build();

        logger.info(
            "Simulating a cluster with {} nodes and {} shards",
            unassignedClusterState.nodes().size(),
            unassignedClusterState.metadata().getTotalNumberOfShards()
        );

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var directExecutor = new PrioritizedEsThreadPoolExecutor(
            "master-service",
            1,
            1,
            1,
            TimeUnit.SECONDS,
            r -> { throw new AssertionError("should not create new threads"); },
            null,
            null,
            PrioritizedEsThreadPoolExecutor.StarvationWatcher.NOOP_STARVATION_WATCHER
        ) {

            @Override
            public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                execute(command);
            }

            @Override
            public void execute(Runnable command) {
                command.run();
            }
        };

        final var masterService = new MasterService(
            settings,
            clusterSettings,
            threadPool,
            new TaskManager(settings, threadPool, Set.of())
        ) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return directExecutor;
            }
        };

        final var applierService = new ClusterApplierService("master", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return directExecutor;
            }
        };

        masterService.setClusterStateSupplier(applierService::state);
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            publishListener.onResponse(null);
        });

        applierService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        applierService.setInitialState(unassignedClusterState);

        final var clusterService = new ClusterService(settings, clusterSettings, masterService, applierService);

        final var clusterInfoService = new TestClusterInfoService(clusterService);

        for (final var discoveryNode : unassignedClusterState.nodes()) {
            clusterInfoService.addNode(
                discoveryNode.getId(),
                LongStream.of(
                    discoveryNode.hasRole(DATA_HOT_NODE_ROLE.roleName()) ? nodeSizeBytesByTier.get(DataTier.DATA_HOT) : 0,
                    discoveryNode.hasRole(DATA_WARM_NODE_ROLE.roleName()) ? nodeSizeBytesByTier.get(DataTier.DATA_WARM) : 0,
                    discoveryNode.hasRole(DATA_COLD_NODE_ROLE.roleName()) ? nodeSizeBytesByTier.get(DataTier.DATA_COLD) : 0
                ).sum()
            );
        }

        for (final var shardSizeByIndex : shardSizesByIndex.entrySet()) {
            clusterInfoService.addIndex(shardSizeByIndex.getKey(), shardSizeByIndex.getValue());
        }

        final var tuple = createNewAllocationService(settings, threadPool, clusterService, clusterInfoService);
        final var allocationService = tuple.getKey();

        final var initializingPrimaries = allocationService.executeWithRoutingAllocation(
            unassignedClusterState,
            "initialize-primaries",
            routingAllocation -> {
                final var routingNodes = routingAllocation.routingNodes();
                final var nodeIds = routingNodes.stream().map(RoutingNode::nodeId).toArray(String[]::new);
                final var unassignedIterator = routingNodes.unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize(RandomPicks.randomFrom(random, nodeIds), null, 0L, routingAllocation.changes());
                    }
                }
            }
        );

        final var startedPrimaries = startInitializingShardsAndReroute(allocationService, initializingPrimaries);

        final var initializingReplicas = allocationService.executeWithRoutingAllocation(
            startedPrimaries,
            "initialize-replicas",
            routingAllocation -> {
                final var routingNodes = routingAllocation.routingNodes();
                final var nodeIds = routingNodes.stream().map(RoutingNode::nodeId).toArray(String[]::new);
                final var unassignedIterator = routingNodes.unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    final var badNodes = routingAllocation.routingTable()
                        .index(shardRouting.index())
                        .shard(shardRouting.id())
                        .assignedShards()
                        .stream()
                        .map(ShardRouting::currentNodeId)
                        .collect(toSet());
                    unassignedIterator.initialize(
                        randomValueOtherThanMany(badNodes::contains, () -> RandomPicks.randomFrom(random, nodeIds)),
                        null,
                        0L,
                        routingAllocation.changes()
                    );
                }
            }
        );

        final var startedReplicas = startInitializingShardsAndReroute(allocationService, initializingReplicas);

        deterministicTaskQueue.runAllTasksInTimeOrder();

        final var clusterState = applyStartedShardsUntilNoChange(startedReplicas, allocationService);

        final var sizeMetricPerTier = Arrays.stream(tiers).collect(toMap(tier -> tier.substring(5, 6), tier -> new TierMetrics()));

        for (final var routingNode : clusterState.getRoutingNodes()
            .stream()
            .sorted(Comparator.comparing(shardRoutings -> shardRoutings.nodeId().substring(7)))
            .toList()) {

            long diskUsageBytes = 0L;
            double totalWriteLoad = 0.0;

            for (ShardRouting shardRouting : routingNode) {
                diskUsageBytes += shardSizesByIndex.get(shardRouting.index().getName());
                totalWriteLoad += TEST_WRITE_LOAD_FORECASTER.getForecastedWriteLoad(clusterState.metadata().index(shardRouting.index()))
                    .orElseThrow(() -> new AssertionError("missing write load"));
            }

            sizeMetricPerTier.get(routingNode.nodeId().substring(5, 6)).addNode(routingNode.size(), totalWriteLoad, diskUsageBytes);
        }

        var totalNormalizedDeviation = 0.0;

        for (final var tier : tiers) {
            final var tierAbbr = tier.substring(5, 6);
            final var sizeMetric = sizeMetricPerTier.get(tierAbbr);

            var shardCountSummary = new MetricSummary<>(sizeMetric.shardCount);
            var writeLoadSummary = new MetricSummary<>(sizeMetric.writeLoad);
            var diskUsageSummary = new MetricSummary<>(sizeMetric.diskUsage);

            logger.info(
                "Tier {}, nodes {}, shards {}, write load {}, disk {}",
                tier,
                sizeMetric.count,
                shardCountSummary,
                writeLoadSummary,
                diskUsageSummary
            );

            totalNormalizedDeviation += combine(
                MetricSummary::getNormalizedStdDeviation,
                shardCountSummary,
                writeLoadSummary,
                diskUsageSummary
            );
        }

        logger.info("Total normalized deviation {}", totalNormalizedDeviation);

        return totalNormalizedDeviation;
    }

    private static double combine(ToDoubleFunction<MetricSummary<?>> metric, MetricSummary<?>... summary) {
        return Arrays.stream(summary).mapToDouble(metric).sum();
    }

    private Map.Entry<MockAllocationService, ShardsAllocator> createNewAllocationService(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        ClusterInfoService clusterInfoService
    ) {
        var strategyRef = new SetOnce<AllocationService>();
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            new BalancedShardsAllocator(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                TEST_WRITE_LOAD_FORECASTER
            ),
            threadPool,
            clusterService,
            (clusterState, routingAllocationAction) -> strategyRef.get()
                .executeWithRoutingAllocation(clusterState, "reconcile-desired-balance", routingAllocationAction)
        );
        var strategy = new MockAllocationService(
            randomAllocationDeciders(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), random()),
            new TestGatewayAllocator(),
            desiredBalanceShardsAllocator,
            clusterInfoService,
            () -> SnapshotShardSizeInfo.EMPTY
        );
        strategyRef.set(strategy);
        return Map.entry(strategy, desiredBalanceShardsAllocator);
    }

    private static class TestClusterInfoService implements ClusterInfoService {

        private final ClusterService clusterService;
        private final Map<String, Long> nodeSizes = new HashMap<>();
        private final Map<String, Long> indexSizes = new HashMap<>();

        private TestClusterInfoService(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        public void addNode(String nodeId, long size) {
            nodeSizes.put(nodeId, size);
        }

        public void addIndex(String indexName, long size) {
            indexSizes.put(indexName, size);
        }

        @Override
        public ClusterInfo getClusterInfo() {

            var state = clusterService.state();

            var diskSpaceUsage = new HashMap<String, DiskUsage>();
            for (DiscoveryNode node : state.nodes()) {
                var nodeSize = nodeSizes.getOrDefault(node.getId(), 0L);
                diskSpaceUsage.put(node.getId(), new DiskUsage(node.getId(), node.getName(), "/data", nodeSize, nodeSize));
            }

            var shardSizes = new HashMap<String, Long>();
            var dataPath = new HashMap<ClusterInfo.NodeAndShard, String>();
            for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
                var shardRouting = indexRoutingTable.shard(0).primaryShard();
                var shardSize = indexSizes.get(shardRouting.shardId().getIndexName());
                if (shardSize == null) {
                    logger.error("Failed to find index [{}]", shardRouting.shardId().getIndexName());
                    continue;
                }
                if (shardRouting.unassigned()) {
                    continue;
                }
                diskSpaceUsage.compute(shardRouting.currentNodeId(), (k, currentUsage) -> {
                    if (currentUsage == null) {
                        logger.error("Failed to find node [{}]", k);
                        return null;
                    }
                    return currentUsage.copyWithFreeBytes(currentUsage.freeBytes() - shardSize);
                });
                shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shardRouting), shardSize);
                dataPath.put(new ClusterInfo.NodeAndShard(shardRouting.currentNodeId(), shardRouting.shardId()), "/data");
            }

            return new ClusterInfo(diskSpaceUsage, diskSpaceUsage, shardSizes, Map.of(), dataPath, Map.of());
        }

    }

    private static class MetricSummary<T extends Number> {
        final List<T> data;
        final DoubleSummaryStatistics summary;

        MetricSummary(List<T> data) {
            this.data = data;
            this.summary = data.stream().mapToDouble(Number::doubleValue).summaryStatistics();
        }

        double getCount() {
            return summary.getCount();
        }

        double getSum() {
            return summary.getSum();
        }

        double getMin() {
            return summary.getMin();
        }

        double getMax() {
            return summary.getMax();
        }

        double getOverage() {
            return getMax() - getAvg();
        }

        double getAvg() {
            return getSum() / getCount();
        }

        double getAvgDeviation() {
            double total = 0.0;
            double avg = getAvg();
            for (T d : data) {
                total += Math.abs(avg - d.doubleValue());
            }

            return total / getCount();
        }

        double getStdDeviation() {
            double total = 0.0;
            double avg = getAvg();
            for (T d : data) {
                total += Math.pow(avg - d.doubleValue(), 2);
            }
            return Math.sqrt(total / getCount());
        }

        double getNormalizedAvgDeviation() {
            double total = 0.0;
            double avg = getAvg();
            if (avg == 0.0) {
                return 0.0;
            }

            for (T d : data) {
                total += Math.abs((avg - d.doubleValue()) / avg);
            }
            return total / getCount();
        }

        double getNormalizedStdDeviation() {
            double total = 0.0;
            double avg = getAvg();
            if (avg == 0.0) {
                return 0.0;
            }

            for (T d : data) {
                total += Math.pow((avg - d.doubleValue()) / avg, 2);
            }
            return Math.sqrt(total / getCount());
        }

        @Override
        public String toString() {
            return "{size=" + data.size() + ", min=" + getMin() + ", max=" + getMax() +
            // ", overage=" + getOverage() +
                ", avg=" + getAvg() +
                // ", avg deviation=" + getAvgDeviation() +
                // ", std deviation=" + getStdDeviation() +
                // ", normalized avg deviation=" + getNormalizedAvgDeviation() +
                ", normalized std deviation=" + getNormalizedStdDeviation() + "}";
        }
    }

    private static class TierMetrics {
        int count;
        final List<Integer> shardCount = new ArrayList<>();
        final List<Double> writeLoad = new ArrayList<>();
        final List<Long> diskUsage = new ArrayList<>();

        void addNode(int shardCount, double writeLoad, long diskUsage) {
            this.count++;
            this.shardCount.add(shardCount);
            this.writeLoad.add(writeLoad);
            this.diskUsage.add(diskUsage);
        }
    }
}
