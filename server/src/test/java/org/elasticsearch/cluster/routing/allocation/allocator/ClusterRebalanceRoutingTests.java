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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.not;

public class ClusterRebalanceRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ClusterRebalanceRoutingTests.class);

    private static final long NODE_SIZE = 55_000L;
    private static final Map<String, Long> INDEX_SIZE = new HashMap<>();

    private MockAllocationService createOldAllocationService(Settings settings, ClusterService clusterService) {
        return new MockAllocationService(
            randomAllocationDeciders(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), random()),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            () -> createClusterInfo(clusterService.state()),
            () -> SnapshotShardSizeInfo.EMPTY
        );
    }

    private MockAllocationService createNewAllocationService(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        var strategyRef = new SetOnce<AllocationService>();
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            new BalancedShardsAllocator(settings),
            threadPool,
            clusterService,
            (clusterState, routingAllocationAction) -> strategyRef.get().executeWithRoutingAllocation(
                clusterState,
                "reconcile-desired-balance",
                routingAllocationAction
            )
        );
        var strategy = new MockAllocationService(
            randomAllocationDeciders(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), random()),
            new TestGatewayAllocator(),
            desiredBalanceShardsAllocator,
            () -> createClusterInfo(clusterService.state()),
            () -> SnapshotShardSizeInfo.EMPTY
        );
        strategyRef.set(strategy);
        return strategy;
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
            .nodes(DiscoveryNodes.builder().add(newNode("master", Set.of(MASTER_ROLE))).localNodeId("master").masterNodeId("master").build())
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);
        var strategy = createOldAllocationService(settings, clusterService);
//        var strategy = createNewAllocationService(settings, threadPool, clusterService);

        for (int i = 0; i < 10; i++) {
            startNewNode(clusterService, strategy, "node-" + i);
            initializeAllShards(clusterService, strategy);
            for (int j = 0; j < 10; j++) {
                for (int k = 0; k < 10; k++) {
                    createIndex(clusterService, strategy, "index-" + i + "-" + j + "-" + k, (j * 10 + k + 1) * 10);
                }
                initializeAllShards(clusterService, strategy);
            }
            verifyDiskUsage(clusterService.state());
        }

        var state = clusterService.state();
//        logger.info("--> {}", state.getRoutingNodes().toString());
        for (RoutingNode routingNode : state.getRoutingNodes()) {
            logger.info("--> {}:{}", routingNode.nodeId(), routingNode.numberOfOwningShards());
        }

        clusterService.close();
        terminate(threadPool);
    }

    private void startNewNode(ClusterService clusterService, AllocationService strategy, String nodeName) throws InterruptedException {
        logger.info("Starting [{}]", nodeName);
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
                fail("Should not happen in test");
            }
        });

        assertTrue(nodeLatch.await(1, TimeUnit.SECONDS));
    }

    private static ClusterState addNode(ClusterState clusterState, String name) {
        return ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(name)).build())
            .build();
    }

    private void createIndex(ClusterService clusterService, AllocationService strategy, String indexName, long size) throws InterruptedException {

        INDEX_SIZE.put(indexName, size);

        logger.info("Adding [{}]", indexName);

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
                fail("Should not happen in test");
            }
        });
        assertTrue(indexLatch.await(1, TimeUnit.SECONDS));
    }

    private static ClusterState addIndex(ClusterState clusterState, String name) {

        var metadataBuilder = Metadata.builder(clusterState.metadata());
        var routingTableBuilder = RoutingTable.builder(clusterState.routingTable());

        var indexMetadataBuilder = IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0);

        metadataBuilder.put(indexMetadataBuilder);
        routingTableBuilder.addAsNew(metadataBuilder.get(name));

        return ClusterState.builder(clusterState)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();
    }

    private void initializeAllShards(ClusterService clusterService, AllocationService strategy) throws InterruptedException {
        while (true) {
            var startLatch = new CountDownLatch(2);

            clusterService.submitUnbatchedStateUpdateTask("start-shards", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    var initializing = RoutingNodesHelper.shardsWithState(currentState.getRoutingNodes(), INITIALIZING);
                    var initialized = randomSubsetOf(initializing);
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
                    fail("Should not happen in test");
                }
            });

            assertTrue(startLatch.await(1, TimeUnit.SECONDS));

            var initializing = RoutingNodesHelper.shardsWithState(clusterService.state().getRoutingNodes(), INITIALIZING).size();
            logger.info("Starting shards. [{}] remaining", initializing);

            if (initializing == 0) {
                break;
            }
        }
    }

    private ClusterInfo createClusterInfo(ClusterState clusterState) {

        var diskSpaceUsage = new HashMap<String, DiskUsage>();
        for (DiscoveryNode node : clusterState.nodes()) {
            diskSpaceUsage.put(node.getId(), new DiskUsage(node.getId(), node.getName(), "/data", NODE_SIZE, NODE_SIZE));
        }

        var shardSizes = new HashMap<String, Long>();
        var dataPath = new HashMap<ClusterInfo.NodeAndShard, String>();
        for (IndexRoutingTable indexRoutingTable : clusterState.getRoutingTable()) {
            var shardRouting = indexRoutingTable.shard(0).primaryShard();
            var shardSize = INDEX_SIZE.get(shardRouting.shardId().getIndexName());
            if (shardRouting.unassigned()) {
                continue;
            }
            diskSpaceUsage.compute(shardRouting.currentNodeId(), (k, currentUsage) -> {
                var freeBytes = currentUsage.freeBytes() - shardSize;
                if (freeBytes < 0 || freeBytes > NODE_SIZE) {
                    logger.error("Unexpected free size [{}] for node [{}]", freeBytes, k);
                }
                return currentUsage.copyWithFreeBytes(freeBytes);
            });
            shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shardRouting), shardSize);
            dataPath.put(new ClusterInfo.NodeAndShard(shardRouting.currentNodeId(), shardRouting.shardId()), "/data");
        }

        return new ClusterInfo(
            diskSpaceUsage,
            diskSpaceUsage,
            shardSizes,
            Map.of(),
            dataPath,
            Map.of()
        );
    }

    private void verifyDiskUsage(ClusterState clusterState) {
        var nodeDiskUsage = new HashMap<String, Long>();
        for (IndexRoutingTable indexRoutingTable : clusterState.getRoutingTable()) {
            var shardRouting = indexRoutingTable.shard(0).primaryShard();
            var shardSize = INDEX_SIZE.get(shardRouting.shardId().getIndexName());
            if (shardRouting.unassigned()) {
                continue;
            }
            nodeDiskUsage.compute(shardRouting.currentNodeId(), (k, v) -> v == null ? shardSize : v + shardSize);
        }

        assertThat(nodeDiskUsage, not(hasValue(greaterThan(NODE_SIZE))));
        logger.info("Current disk usage: {}", nodeDiskUsage);
    }
}
