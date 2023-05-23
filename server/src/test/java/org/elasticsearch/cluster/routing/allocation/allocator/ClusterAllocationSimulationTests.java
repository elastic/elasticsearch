/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_COLD_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;

public class ClusterAllocationSimulationTests extends ESAllocationTestCase {

    private static final Logger logger = LogManager.getLogger(ClusterAllocationSimulationTests.class);

    private static int nodeCount(long tierBytes, long nodeBytes, double tierWriteLoad, double nodeIndexingThreads, int minCount) {
        var watermarkNodeBytes = (long) (nodeBytes * 0.85);
        return Math.max(
            Math.toIntExact((tierBytes + watermarkNodeBytes - 1) / watermarkNodeBytes),
            Math.max((int) Math.ceil(tierWriteLoad / nodeIndexingThreads), minCount)
        ) + between(0, 2);
    }

    /**
     * This test creates a somewhat realistic cluster and runs the shards allocator until convergence, then reports on the balance quality
     * of the resulting allocation. This is useful for exploring changes to the balancer (although such experiments may want to increase the
     * size of the cluster somewhat) and by running it in CI we can at least be sure that the balancer doesn't throw anything unexpected and
     * does eventually converge in these situations.
     */
    public void testBalanceQuality() throws IOException {

        final var shardSizesByIndex = new HashMap<String, Long>();
        final var tiers = new String[] { DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD };
        final var tierSizes = Arrays.stream(tiers).collect(Collectors.toMap(Function.identity(), s -> 0L));
        final var tierWriteLoads = Arrays.stream(tiers).collect(Collectors.toMap(Function.identity(), s -> 0.0));
        final var maxSizeVariance = ByteSizeValue.ofMb(20).getBytes();

        final var metadataBuilder = Metadata.builder();
        for (var dataStreamIndex = between(5, 10); dataStreamIndex >= 0; dataStreamIndex -= 1) {
            final var hotIndices = between(2, 10);
            final var warmIndices = between(10, 20);
            final var coldIndices = between(10, 40);
            final var hotShards = randomFrom(1, 2, 4, 8);
            final var shrunkShards = randomFrom(1, 2);
            final var approxIndexSize = randomFrom(ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(10), ByteSizeValue.ofGb(50));

            final var indexCount = hotIndices + warmIndices + coldIndices;
            for (var index = 0; index < indexCount; index++) {
                final var tier = index < coldIndices ? DataTier.DATA_COLD
                    : index < coldIndices + warmIndices ? DataTier.DATA_WARM
                    : DataTier.DATA_HOT;
                final var indexName = Strings.format("index-%03d-%s-%03d", dataStreamIndex, tier.charAt(5), index);
                final var shardCount = DataTier.DATA_HOT.equals(tier) ? hotShards : shrunkShards;
                final var replicaCount = DataTier.DATA_COLD.equals(tier) ? 0 : 1;
                final var indexWriteLoad = index == indexCount - 1 ? (scaledRandomIntBetween(1, 8000) / 1000.0) : 0.0;
                final var shardSize = approxIndexSize.getBytes() / shardCount + randomLongBetween(-maxSizeVariance, maxSizeVariance);

                metadataBuilder.put(
                    IndexMetadata.builder(indexName)
                        .settings(
                            indexSettings(Version.CURRENT, shardCount, replicaCount).put(
                                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".fake_tier",
                                tier
                            )
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

        final var routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        for (final var indexMetadata : metadata) {
            routingTableBuilder.addAsNew(indexMetadata);
        }

        final var nodeSizeBytesByTier = Map.of(
            DataTier.DATA_HOT,
            randomLongBetween(ByteSizeValue.ofGb(500).getBytes(), ByteSizeValue.ofTb(4).getBytes()),
            DataTier.DATA_WARM,
            randomLongBetween(ByteSizeValue.ofTb(1).getBytes(), ByteSizeValue.ofTb(10).getBytes()),
            DataTier.DATA_COLD,
            randomLongBetween(ByteSizeValue.ofTb(1).getBytes(), ByteSizeValue.ofTb(10).getBytes())
        );

        final var indexingThreadsByTier = Map.of(
            DataTier.DATA_HOT,
            randomDoubleBetween(4.0, 16.0, true),
            DataTier.DATA_WARM,
            1.0,
            DataTier.DATA_COLD,
            1.0
        );

        final var nodeCountByTier = Map.of(
            DataTier.DATA_HOT,
            nodeCount(
                tierSizes.get(DataTier.DATA_HOT),
                nodeSizeBytesByTier.get(DataTier.DATA_HOT),
                tierWriteLoads.get(DataTier.DATA_HOT),
                indexingThreadsByTier.get(DataTier.DATA_HOT),
                8
            ),
            DataTier.DATA_WARM,
            nodeCount(
                tierSizes.get(DataTier.DATA_WARM),
                nodeSizeBytesByTier.get(DataTier.DATA_WARM),
                tierWriteLoads.get(DataTier.DATA_WARM),
                indexingThreadsByTier.get(DataTier.DATA_WARM),
                2
            ),
            DataTier.DATA_COLD,
            nodeCount(
                tierSizes.get(DataTier.DATA_COLD),
                nodeSizeBytesByTier.get(DataTier.DATA_COLD),
                tierWriteLoads.get(DataTier.DATA_COLD),
                indexingThreadsByTier.get(DataTier.DATA_COLD),
                1
            )
        );

        final var discoveryNodesBuilder = new DiscoveryNodes.Builder();
        discoveryNodesBuilder.add(
            TestDiscoveryNode.create("master", "master", buildNewFakeTransportAddress(), Map.of(), Set.of(MASTER_ROLE))
        ).localNodeId("master").masterNodeId("master");
        for (var nodeIndex = 0; nodeIndex < nodeCountByTier.get(DataTier.DATA_HOT) + nodeCountByTier.get(DataTier.DATA_WARM)
            + nodeCountByTier.get(DataTier.DATA_COLD); nodeIndex++) {
            final var tierRole = nodeIndex < nodeCountByTier.get(DataTier.DATA_HOT) ? DATA_HOT_NODE_ROLE
                : nodeIndex < nodeCountByTier.get(DataTier.DATA_HOT) + nodeCountByTier.get(DataTier.DATA_WARM) ? DATA_WARM_NODE_ROLE
                : DATA_COLD_NODE_ROLE;

            final var nodeId = Strings.format("node-%s-%03d", tierRole.roleNameAbbreviation(), nodeIndex);
            discoveryNodesBuilder.add(
                TestDiscoveryNode.create(
                    nodeId,
                    nodeId,
                    buildNewFakeTransportAddress(),
                    Map.of("fake_tier", tierRole.roleName()),
                    Set.of(tierRole)
                )
            );
        }

        final var unassignedClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(discoveryNodesBuilder)
            .metadata(metadata)
            .routingTable(routingTableBuilder)
            .build();

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var settings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var masterService = new MasterService(
            settings,
            clusterSettings,
            threadPool,
            new TaskManager(settings, threadPool, Set.of())
        ) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                return new StoppableExecutorServiceWrapper(EsExecutors.DIRECT_EXECUTOR_SERVICE);
            }
        };

        final var applierService = new ClusterApplierService("master", settings, clusterSettings, threadPool) {
            private final PrioritizedEsThreadPoolExecutor directExecutor = new PrioritizedEsThreadPoolExecutor(
                "master-service",
                1,
                1,
                1,
                TimeUnit.SECONDS,
                r -> {
                    throw new AssertionError("should not create new threads");
                },
                null,
                null
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
        clusterService.start();

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

        final var tuple = createNewAllocationService(threadPool, deterministicTaskQueue::runAllTasks, clusterService, clusterInfoService);
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
                        unassignedIterator.initialize(randomFrom(nodeIds), null, 0L, routingAllocation.changes());
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
                        .collect(Collectors.toSet());
                    unassignedIterator.initialize(
                        randomValueOtherThanMany(badNodes::contains, () -> randomFrom(nodeIds)),
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

        try (var bos = new BytesStreamOutput(); var results = XContentFactory.jsonBuilder(bos)) {
            results.prettyPrint();

            results.startObject();
            results.startArray("nodes");

            class SizeMetric {
                int count;

                long totalSizeBytes;
                long maxSizeBytes;

                int totalShardCount;
                int maxShardCount;

                double totalWriteLoad;
                double maxWriteLoad;

                void addNode(long sizeBytes, int shardCount, double writeLoad) {
                    count += 1;
                    totalSizeBytes += sizeBytes;
                    maxSizeBytes = Math.max(maxSizeBytes, sizeBytes);
                    totalShardCount += shardCount;
                    maxShardCount = Math.max(maxShardCount, shardCount);
                    totalWriteLoad += writeLoad;
                    maxWriteLoad = Math.max(maxWriteLoad, writeLoad);
                }
            }

            final var sizeMetricPerTier = Arrays.stream(tiers).collect(Collectors.toMap(s -> s.substring(5, 6), s -> new SizeMetric()));

            for (final var routingNode : clusterState.getRoutingNodes()
                .stream()
                .sorted(Comparator.comparing(shardRoutings -> shardRoutings.nodeId().substring(7)))
                .toList()) {

                int shards = 0;
                long totalBytes = 0L;
                double totalWriteLoad = 0.0;

                for (ShardRouting shardRouting : routingNode) {
                    shards += 1;
                    totalBytes += shardSizesByIndex.get(shardRouting.index().getName());
                    totalWriteLoad += TEST_WRITE_LOAD_FORECASTER.getForecastedWriteLoad(clusterState.metadata().index(shardRouting.index()))
                        .orElseThrow(() -> new AssertionError("missing write load"));
                }

                results.startObject();
                results.field("node", routingNode.nodeId());
                results.field("shards", shards);
                bytesField(results, "total_data", totalBytes);
                results.field("total_write_load", totalWriteLoad);
                results.endObject();

                sizeMetricPerTier.get(routingNode.nodeId().substring(5, 6)).addNode(totalBytes, routingNode.size(), totalWriteLoad);
            }

            results.endArray(); // nodes

            results.startArray("tiers");

            for (final var tier : tiers) {
                final var tierAbbr = tier.substring(5, 6);
                final var nodeSizeBytes = nodeSizeBytesByTier.get(tier);
                final var sizeMetric = sizeMetricPerTier.get(tierAbbr);
                assert sizeMetric.count > 0;

                final var totalShardCount = sizeMetric.totalShardCount;
                final var meanShardCount = totalShardCount * 1.0 / sizeMetric.count;
                final var maxShardCount = sizeMetric.maxShardCount;
                final var overageShardCount = maxShardCount - meanShardCount;
                final var overageShardCountRatio = new RatioValue(Math.ceil((1000.0 * overageShardCount) / meanShardCount) / 10.0);

                final var meanSizeBytes = sizeMetric.totalSizeBytes / sizeMetric.count;
                final var maxSizeBytes = sizeMetric.maxSizeBytes;
                final var overageBytes = maxSizeBytes - meanSizeBytes;
                final var overageBytesRatio = new RatioValue(Math.ceil((1000.0 * overageBytes) / meanSizeBytes) / 10.0);
                final var meanWriteLoad = sizeMetric.totalWriteLoad / sizeMetric.count;
                final var maxWriteLoad = sizeMetric.maxWriteLoad;
                final var overageWriteLoad = maxWriteLoad - meanWriteLoad;
                final var overageWriteLoadRatio = new RatioValue(Math.ceil((1000.0 * overageWriteLoad) / meanWriteLoad) / 10.0);
                assert overageBytes >= 0;

                results.startObject();

                results.field("tier", tier);
                results.field("nodes", nodeCountByTier.get(tier));

                results.startObject("shard_count");
                results.field("total", totalShardCount);
                results.field("mean", meanShardCount);
                results.field("max", maxShardCount);
                results.field("overage", overageShardCount);
                results.field("overage_percent", overageShardCountRatio.formatNoTrailingZerosPercent());
                results.endObject(); // shard_count

                results.startObject("data_per_node");
                bytesField(results, "capacity", nodeSizeBytes);
                bytesField(results, "mean", meanSizeBytes);
                bytesField(results, "max", maxSizeBytes);
                bytesField(results, "overage", overageBytes);
                results.field("overage_percent", overageBytesRatio.formatNoTrailingZerosPercent());
                results.endObject(); // data_per_node

                results.startObject("write_load_per_node");
                results.field("indexing_threads", indexingThreadsByTier.get(tier));
                results.field("mean", meanWriteLoad);
                results.field("max", maxWriteLoad);
                results.field("overage", overageWriteLoad);
                results.field("overage_percent", overageWriteLoadRatio.formatNoTrailingZerosPercent());
                results.endObject(); // write_load_per_node

                results.endObject();
            }

            results.endArray(); // tiers

            results.endObject();
            results.flush();
            logger.debug("\n\n{}\n\n", bos.bytes().utf8ToString());
        }
    }

    private void bytesField(XContentBuilder results, String fieldName, long bytes) throws IOException {
        results.field(fieldName, ByteSizeValue.ofBytes(bytes));
        results.field(fieldName + "_in_bytes", bytes);
    }

    private Map.Entry<MockAllocationService, ShardsAllocator> createNewAllocationService(
        ThreadPool threadPool,
        Runnable runAllTasks,
        ClusterService clusterService,
        ClusterInfoService clusterInfoService
    ) {
        var strategyRef = new SetOnce<AllocationService>();
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            createBuiltInClusterSettings(),
            new BalancedShardsAllocator(
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                TEST_WRITE_LOAD_FORECASTER
            ),
            threadPool,
            clusterService,
            (clusterState, routingAllocationAction) -> strategyRef.get()
                .executeWithRoutingAllocation(clusterState, "reconcile-desired-balance", routingAllocationAction)
        ) {
            @Override
            public void allocate(RoutingAllocation allocation, ActionListener<Void> listener) {
                super.allocate(allocation, listener);
                runAllTasks.run();
            }
        };
        var strategy = new MockAllocationService(
            randomAllocationDeciders(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
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
}
