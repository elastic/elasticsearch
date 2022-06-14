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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class ClusterRebalanceRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ClusterRebalanceRoutingTests.class);

    private MockAllocationService createOldAllocationService(Settings settings) {
        return new MockAllocationService(
            randomAllocationDeciders(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), random()),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
    }

    private MockAllocationService createNewAllocationService(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        var rerouteServiceSupplier = new SetOnce<RerouteService>();
        var desiredBalanceShardsAllocator = DesiredBalanceShardsAllocator.create(
            new BalancedShardsAllocator(settings),
            threadPool,
            clusterService,
            rerouteServiceSupplier::get
        );
        var strategy = new MockAllocationService(
            randomAllocationDeciders(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), random()),
            new TestGatewayAllocator(),
            desiredBalanceShardsAllocator,
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
        rerouteServiceSupplier.set(
            (reason, priority, listener) -> clusterService.submitUnbatchedStateUpdateTask(reason, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return strategy.reroute(currentState, reason, listener.map(ignore -> null));
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not happen in test");
                }
            })
        );
        return strategy;
    }

    private ClusterState createInitialClusterState() {
        int nodes = 35;
        var nodesIds = new HashSet<String>();
        var discoveryNodesBuilder = DiscoveryNodes.builder().localNodeId("node-0").masterNodeId("node-0");
        for (int n = 0; n < nodes; n++) {
            String nodeId = "node-" + n;
            nodesIds.add(nodeId);
            discoveryNodesBuilder.add(newNode(nodeId));
        }

        int indices = between(500, 1000);
        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < indices; i++) {
            var primaries = between(1, 10);
            var replicas = between(0, 2);
            var indexName = "index-" + i;
            var indexMetadataBuilder = IndexMetadata.builder(indexName)
                .settings(settings(Version.CURRENT))
                .numberOfShards(primaries)
                .numberOfReplicas(replicas);

            var indexId = new Index(indexName, INDEX_UUID_NA_VALUE);
            var indexRoutingTableBuilder = IndexRoutingTable.builder(indexId);
            for (int p = 0; p < primaries; p++) {
                var remainingNodeIds = new HashSet<>(nodesIds);
                var inSyncAllocationIds = new HashSet<String>();

                var inSyncAllocationId = UUIDs.randomBase64UUID(random());
                indexRoutingTableBuilder.addShard(
                    TestShardRouting.newShardRouting(
                        new ShardId(indexId, p),
                        pickAndRemoveFrom(remainingNodeIds),
                        null,
                        true,
                        STARTED,
                        AllocationId.newInitializing(inSyncAllocationId)
                    )
                );
                inSyncAllocationIds.add(inSyncAllocationId);

                for (int r = 0; r < replicas; r++) {
                    inSyncAllocationId = UUIDs.randomBase64UUID(random());
                    indexRoutingTableBuilder.addShard(
                        TestShardRouting.newShardRouting(
                            new ShardId(indexId, p),
                            pickAndRemoveFrom(remainingNodeIds),
                            null,
                            false,
                            STARTED,
                            AllocationId.newInitializing(inSyncAllocationId)
                        )
                    );
                    inSyncAllocationIds.add(inSyncAllocationId);
                }
                indexMetadataBuilder.putInSyncAllocationIds(p, inSyncAllocationIds);
            }
            metadataBuilder.put(indexMetadataBuilder);
            routingTableBuilder.add(indexRoutingTableBuilder);
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .routingTable(routingTableBuilder)
            .metadata(metadataBuilder)
            .build();
    }

    public void testRebalanceInBigCluster() throws InterruptedException {

        var clusterState = createInitialClusterState();

        var settings = Settings.builder()
            .put(
                ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
            )
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), "20")
            .build();
        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);
        var strategy = createOldAllocationService(settings);
        // var strategy = createNewAllocationService(settings, threadPool, clusterService);

        var relocations = new TreeMap<ShardIdAndPrimary, LinkedHashSet<String>>();
        clusterService.addListener(event -> {
            for (RoutingNode routingNode : event.state().getRoutingNodes()) {
                for (ShardRouting shardRouting : routingNode) {
                    if (shardRouting.relocating()) {
                        relocations.computeIfAbsent(ShardIdAndPrimary.of(shardRouting), (ignored) -> new LinkedHashSet<>())
                            .add(shardRouting.currentNodeId() + "-->" + shardRouting.relocatingNodeId());
                    }
                }
            }
        });

        for (int reroute = 0; reroute < 1000; reroute++) {
            var hasRelocatingShards = new AtomicBoolean(false);
            var latch = new CountDownLatch(2);
            var start = Instant.now();
            clusterService.submitUnbatchedStateUpdateTask("reroute", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    var initializing = RoutingNodesHelper.shardsWithState(currentState.getRoutingNodes(), INITIALIZING);
                    var initialized = randomSubsetOf(initializing);
                    hasRelocatingShards.set(initializing.size() > 0);
                    logger.info("Initialized {}/{} shards", initialized.size(), initializing.size());
                    ClusterState newState = strategy.reroute(
                        strategy.applyStartedShards(currentState, initialized),
                        "reroute",
                        ActionListener.<Void>wrap(latch::countDown)
                    );
                    latch.countDown();
                    return newState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not happen in test");
                }
            });
            assertThat(latch.await(150, TimeUnit.SECONDS), equalTo(true));
            var finish = Instant.now();
            logger.info("Reroute [{}] has completed within [{}]", reroute, Duration.between(start, finish));
            if (reroute > 0 && hasRelocatingShards.get() == false) {
                logger.info("No more relocating shards");
                break;
            }
        }

        var state = clusterService.state();

        long total = 0;
        long unnecessary = 0;
        for (var entry : relocations.entrySet()) {
            total += entry.getValue().size();

            var routing = state.routingTable().index(entry.getKey().shardId().getIndex()).shard(entry.getKey().shardId().id());
            var expectedRelocations = entry.getKey().primary() ? 1 : routing.replicaShards().size();

            if (entry.getValue().size() > expectedRelocations) {
                unnecessary += entry.getValue().size() - expectedRelocations;
                logger.info(
                    "Too many relocations for {}[{}]: (expected only {}) but got: {}",
                    entry.getKey().shardId(),
                    entry.getKey().primary() ? 'P' : 'R',
                    expectedRelocations,
                    entry.getValue()
                );
            }
        }
        logger.info(
            "Relocation history ({} total relocations, {} unnecessary relocations, total shards {})",
            total,
            unnecessary,
            state.routingTable().allShards().size()
        );

        clusterService.close();
        terminate(threadPool);
    }

    private record ShardIdAndPrimary(ShardId shardId, boolean primary) implements Comparable<ShardIdAndPrimary> {

        public static ShardIdAndPrimary of(ShardRouting routing) {
            return new ShardIdAndPrimary(routing.shardId(), routing.primary());
        }

        @Override
        public int compareTo(ShardIdAndPrimary o) {
            return Comparator.comparing(ShardIdAndPrimary::shardId).thenComparing(ShardIdAndPrimary::primary).compare(this, o);
        }
    }

    private <T> T pickAndRemoveFrom(Set<T> values) {
        var value = randomFrom(values);
        values.remove(value);
        return value;
    }
}
