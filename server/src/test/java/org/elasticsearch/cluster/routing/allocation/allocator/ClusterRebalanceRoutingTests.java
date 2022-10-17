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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ClusterRebalanceRoutingTests extends ESAllocationTestCase {

    private static final Logger logger = LogManager.getLogger(ClusterRebalanceRoutingTests.class);

    private Map.Entry<MockAllocationService, ShardsAllocator> createOldAllocationService(
        Settings settings,
        ClusterService clusterService,
        ClusterInfoService clusterInfoService
    ) {
        var shardsAllocator = new BalancedShardsAllocator(settings);
        var allocationService = new MockAllocationService(
            randomAllocationDeciders(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), random()),
            new TestGatewayAllocator(),
            shardsAllocator,
            clusterInfoService,
            () -> SnapshotShardSizeInfo.EMPTY
        );
        return Map.entry(allocationService, shardsAllocator);
    }

    private Map.Entry<MockAllocationService, ShardsAllocator> createNewAllocationService(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        ClusterInfoService clusterInfoService
    ) {
        var strategyRef = new SetOnce<AllocationService>();
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            new BalancedShardsAllocator(settings),
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

    public void testCreateManyIndicesAndAddNewNode() throws InterruptedException {

        var settings = Settings.builder()
            .put(
                ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
            )
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), "10")
            .build();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder().add(newNode("master", Set.of(MASTER_ROLE))).localNodeId("master").masterNodeId("master").build()
            )
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);
        var clusterInfoService = new TestClusterInfoService(clusterService);
        // var allocationService = createOldAllocationService(settings, clusterService, clusterInfoService);
        var allocationService = createNewAllocationService(settings, threadPool, clusterService, clusterInfoService);
        var strategy = allocationService.getKey();

        var nodeNameGenerator = new AtomicInteger(0);
        var indexNameGenerator = new AtomicInteger(0);

        for (int i = 0; i < 5_000; i++) {
            if (clusterInfoService.totalIndicesSize * 1.15 > clusterInfoService.totalNodesSize) {
                startNewNode(clusterService, clusterInfoService, strategy, "node-" + nodeNameGenerator.incrementAndGet(), 1_000_000);
                initializeAllShards(clusterService, strategy, "adding new node");
            }

            var indexSize = switch (randomIntBetween(0, 1000)) {
                case 0 -> randomIntBetween(100_000, 150_000); // rare big index
                default -> randomIntBetween(100, 10_000);
            };

            createIndex(clusterService, clusterInfoService, strategy, "index-" + indexNameGenerator.incrementAndGet(), indexSize, false);
            if (randomIntBetween(0, 9) == 0) {
                initializeAllShards(clusterService, strategy, "starting indices");
            }
            clusterInfoService.verifyDiskUsage();
        }

        var state = clusterService.state();
        var info = clusterInfoService.getClusterInfo();
        for (var entry : info.getNodeMostAvailableDiskUsages().entrySet()) {
            var routingNode = state.getRoutingNodes().node(entry.getKey());
            logger.info("{} with {} indices", entry.getValue(), routingNode != null ? routingNode.numberOfOwningShards() : 0);
        }

        if (allocationService.getValue()instanceof DesiredBalanceShardsAllocator allocator) {
            logger.info("Desired balance allocator stats [{}]", allocator.getStats());
        }

        clusterService.close();
        terminate(threadPool);
    }

    private void startNewNode(
        ClusterService clusterService,
        TestClusterInfoService clusterInfoService,
        AllocationService strategy,
        String nodeName,
        long capacity
    ) throws InterruptedException {
        logger.info("Starting [{}] with capacity [{}]", nodeName, capacity);
        clusterInfoService.addNode(nodeName, capacity);
        var nodeLatch = new CountDownLatch(1);
        clusterService.submitUnbatchedStateUpdateTask("add-node", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return strategy.reroute(addNode(currentState, nodeName), "node-added", ActionListener.noop());
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                nodeLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not happen in test " + e);
            }
        });

        assertTrue(nodeLatch.await(10, TimeUnit.SECONDS));
    }

    private static ClusterState addNode(ClusterState clusterState, String name) {
        return ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(name)).build()).build();
    }

    private void createIndex(
        ClusterService clusterService,
        TestClusterInfoService clusterInfoService,
        AllocationService strategy,
        String indexName,
        long size,
        boolean await
    ) throws InterruptedException {
        logger.info("Adding [{}] with size [{}]", indexName, size);
        clusterInfoService.addIndex(indexName, size);
        var indexLatch = new CountDownLatch(1);
        clusterService.submitUnbatchedStateUpdateTask("add-index", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return strategy.reroute(addIndex(currentState, indexName), "index-added", ActionListener.noop());
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                indexLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not happen in test " + e);
            }
        });
        if (await) {
            assertTrue(indexLatch.await(10, TimeUnit.SECONDS));
        }
    }

    private static ClusterState addIndex(ClusterState clusterState, String name) {

        var metadataBuilder = Metadata.builder(clusterState.metadata());
        var routingTableBuilder = RoutingTable.builder(clusterState.routingTable());

        var indexMetadataBuilder = IndexMetadata.builder(name).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0);

        metadataBuilder.put(indexMetadataBuilder);
        routingTableBuilder.addAsNew(metadataBuilder.get(name));

        return ClusterState.builder(clusterState).metadata(metadataBuilder).routingTable(routingTableBuilder).build();
    }

    private void initializeAllShards(ClusterService clusterService, AllocationService strategy, String reason) throws InterruptedException {

        var movements = new HashMap<ShardId, List<String>>();

        int initializing = 0;
        do {
            var startLatch = new CountDownLatch(2);

            clusterService.submitUnbatchedStateUpdateTask("start-shards", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    var initializing = RoutingNodesHelper.shardsWithState(currentState.getRoutingNodes(), INITIALIZING);
                    var initialized = randomSubsetOf(initializing);
                    for (ShardRouting shardRouting : initialized) {
                        if (shardRouting.relocatingNodeId() != null) {
                            movements.computeIfAbsent(shardRouting.shardId(), k -> new ArrayList<>())
                                .add(shardRouting.relocatingNodeId() + "-->" + shardRouting.currentNodeId());
                        }
                    }
                    return strategy.reroute(
                        strategy.applyStartedShards(currentState, initialized),
                        "start-shards",
                        ActionListener.wrap(startLatch::countDown)
                    );
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    startLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not happen in test " + e);
                }
            });

            assertTrue(startLatch.await(10, TimeUnit.SECONDS));

            initializing = RoutingNodesHelper.shardsWithState(clusterService.state().getRoutingNodes(), INITIALIZING).size();
            logger.info("Starting shards. [{}] remaining", initializing);

        } while (initializing > 0);

        for (var entry : movements.entrySet()) {
            if (entry.getValue().size() > 1) {
                logger.warn("Shard {} moved too many times: {} after {}", entry.getKey(), entry.getValue(), reason);
            }
        }
    }

    private static class TestClusterInfoService implements ClusterInfoService {

        private final ClusterService clusterService;
        private final Map<String, Long> nodeSizes = new HashMap<>();
        private long totalNodesSize = 0L;
        private final Map<String, Long> indexSizes = new HashMap<>();
        private long totalIndicesSize = 0L;

        private TestClusterInfoService(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        public void addNode(String nodeId, long size) {
            nodeSizes.put(nodeId, size);
            totalNodesSize += size;
        }

        public void addIndex(String indexName, long size) {
            indexSizes.put(indexName, size);
            totalIndicesSize += size;
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
                    }
                    return currentUsage.copyWithFreeBytes(currentUsage.freeBytes() - shardSize);
                });
                shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shardRouting), shardSize);
                dataPath.put(new ClusterInfo.NodeAndShard(shardRouting.currentNodeId(), shardRouting.shardId()), "/data");
            }

            return new ClusterInfo(diskSpaceUsage, diskSpaceUsage, shardSizes, Map.of(), dataPath, Map.of());
        }

        private void verifyDiskUsage() {
            var state = clusterService.state();

            var nodeDiskUsage = new HashMap<String, Long>();
            for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
                var shardRouting = indexRoutingTable.shard(0).primaryShard();
                var shardSize = indexSizes.get(shardRouting.shardId().getIndexName());
                if (shardRouting.unassigned()) {
                    continue;
                }
                nodeDiskUsage.compute(shardRouting.currentNodeId(), (k, v) -> v == null ? shardSize : v + shardSize);
            }

            for (DiscoveryNode node : state.nodes()) {
                assertThat(
                    node.getId(),
                    nodeSizes.getOrDefault(node.getId(), 0L),
                    greaterThanOrEqualTo(nodeDiskUsage.getOrDefault(node.getId(), 0L))
                );
            }
            logger.debug("Current disk usage: {}", nodeDiskUsage);
        }
    }
}
