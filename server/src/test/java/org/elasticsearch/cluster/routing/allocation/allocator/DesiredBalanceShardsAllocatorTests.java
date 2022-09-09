/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class DesiredBalanceShardsAllocatorTests extends ESTestCase {

    private static final String TEST_INDEX = "test-index";

    public void testGatewayAllocatorPreemptsAllocation() {
        testAllocate(
            (allocation, unassignedAllocationHandler) -> unassignedAllocationHandler.initialize(
                allocation.nodes().getLocalNodeId(),
                null,
                0L,
                allocation.changes()
            ),
            routingTable -> assertTrue(routingTable.index("test-index").shard(0).primaryShard().assignedToNode())
        );
    }

    public void testGatewayAllocatorStillFetching() {
        testAllocate(
            (allocation, unassignedAllocationHandler) -> unassignedAllocationHandler.removeAndIgnore(
                UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA,
                allocation.changes()
            ),
            routingTable -> {
                var shardRouting = routingTable.shardRoutingTable("test-index", 0).primaryShard();
                assertFalse(shardRouting.assignedToNode());
                assertThat(
                    shardRouting.unassignedInfo().getLastAllocationStatus(),
                    equalTo(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA)
                );
            }
        );
    }

    public void testGatewayAllocatorDoesNothing() {
        testAllocate((allocation, unassignedAllocationHandler) -> {}, routingTable -> {
            var shardRouting = routingTable.shardRoutingTable("test-index", 0).primaryShard();
            assertTrue(shardRouting.assignedToNode());// assigned by a followup reconciliation
            assertThat(shardRouting.unassignedInfo().getLastAllocationStatus(), equalTo(UnassignedInfo.AllocationStatus.NO_ATTEMPT));
        });
    }

    public void testAllocate(
        BiConsumer<RoutingAllocation, ExistingShardsAllocator.UnassignedAllocationHandler> allocateUnassigned,
        Consumer<RoutingTable> verifier
    ) {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var localNode = new DiscoveryNode(
            "node-1",
            "node-1",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );

        var initialState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        var settings = Settings.EMPTY;
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var clusterService = new ClusterService(
            settings,
            clusterSettings,
            new FakeThreadPoolMasterService("node-1", "test", threadPool, deterministicTaskQueue::scheduleNow),
            new ClusterApplierService("node-1", settings, clusterSettings, threadPool) {
                @Override
                protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                    return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
                }
            }
        );
        clusterService.getClusterApplierService().setInitialState(initialState);
        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterService.getMasterService()
            .setClusterStatePublisher(ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();

        var allocationServiceRef = new SetOnce<AllocationService>();
        var reconcileAction = new DesiredBalanceShardsAllocator.DesiredBalanceReconcilerAction() {
            @Override
            public ClusterState apply(ClusterState clusterState, Consumer<RoutingAllocation> routingAllocationAction) {
                return allocationServiceRef.get().executeWithRoutingAllocation(clusterState, "reconcile", routingAllocationAction);
            }
        };

        final var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var dataNodeId = allocation.nodes().getDataNodes().values().iterator().next().getId();
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    unassignedIterator.next();
                    unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        }, threadPool, clusterService, reconcileAction);

        var allocationService = new AllocationService(new AllocationDeciders(List.of()), new GatewayAllocator() {

            @Override
            public void beforeAllocation(RoutingAllocation allocation) {}

            @Override
            public void allocateUnassigned(
                ShardRouting shardRouting,
                RoutingAllocation allocation,
                UnassignedAllocationHandler unassignedAllocationHandler
            ) {
                allocateUnassigned.accept(allocation, unassignedAllocationHandler);
            }

            @Override
            public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}
        }, desiredBalanceShardsAllocator, () -> ClusterInfo.EMPTY, () -> SnapshotShardSizeInfo.EMPTY);
        allocationServiceRef.set(allocationService);

        var listenerCalled = new AtomicBoolean(false);
        clusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                var indexMetadata = createIndex("test-index");
                var newState = ClusterState.builder(currentState)
                    .metadata(Metadata.builder().put(indexMetadata, true))
                    .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
                    .build();
                return allocationService.reroute(newState, "test", ActionListener.wrap(() -> listenerCalled.set(true)));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        deterministicTaskQueue.runAllTasks();

        try {
            assertTrue(listenerCalled.get());
            verifier.accept(clusterService.state().routingTable());
        } finally {
            clusterService.close();
        }
    }

    @AwaitsFix(bugUrl = "desired balance")
    public void testCallListenersOnlyAfterProducingFreshInput() {

        var secondInputSubmitted = new CountDownLatch(1);
        var listenersCalled = new CountDownLatch(2);
        var reroutedIndexes = new CopyOnWriteArraySet<String>();

        var threadPool = new TestThreadPool(getTestName());
        var rerouteServiceSupplier = new SetOnce<RerouteService>();
        var clusterService = ClusterServiceUtils.createClusterService(createInitialClusterState(), threadPool);
        var allocator = new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var dataNodeId = allocation.nodes().getDataNodes().values().iterator().next().getId();
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    var indexName = unassignedIterator.next().getIndexName();
                    unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                }

                try {
                    assertTrue("Should have submitted the second input in time", secondInputSubmitted.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError("Should have submitted the second input in time");
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };

        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(allocator, threadPool, clusterService, null);
        var allocationService = new AllocationService(new AllocationDeciders(List.of()), new GatewayAllocator() {
            @Override
            public void beforeAllocation(RoutingAllocation allocation) {}

            @Override
            public void allocateUnassigned(
                ShardRouting shardRouting,
                RoutingAllocation allocation,
                UnassignedAllocationHandler unassignedAllocationHandler
            ) {
                unassignedAllocationHandler.initialize(allocation.nodes().getLocalNodeId(), null, 0L, allocation.changes());
            }

            @Override
            public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}
        }, desiredBalanceShardsAllocator, () -> ClusterInfo.EMPTY, () -> SnapshotShardSizeInfo.EMPTY);
        rerouteServiceSupplier.set((r, p, l) -> {
            clusterService.submitUnbatchedStateUpdateTask("test-desired-balance-reroute", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    for (IndexRoutingTable indexRoutingTable : currentState.getRoutingTable()) {
                        reroutedIndexes.add(indexRoutingTable.getIndex().getName());
                    }
                    return allocationService.reroute(currentState, "test-desired-balance-reroute", l.map(ignore -> null));
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not happen in test");
                }
            });
        });

        clusterService.submitUnbatchedStateUpdateTask("test-create-index", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                currentState = createIndex(currentState, "index-1");
                currentState = allocationService.reroute(currentState, "test-create-index", ActionListener.wrap(response -> {
                    logger.info("Completing listener 1 with rerouted indexes: {}", reroutedIndexes);
                    assertThat(reroutedIndexes, hasItem("index-1"));
                    listenersCalled.countDown();
                }, exception -> { throw new AssertionError("Should not fail in test"); }));
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not happen in test");
            }
        });

        clusterService.submitUnbatchedStateUpdateTask("test-create-index", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                currentState = createIndex(currentState, "index-2");
                currentState = allocationService.reroute(currentState, "test-create-index", ActionListener.wrap(response -> {
                    logger.info("Completing listener 2 with rerouted indexes: {}", reroutedIndexes);
                    assertThat(reroutedIndexes, hasItem("index-2"));
                    listenersCalled.countDown();
                }, exception -> { throw new AssertionError("Should not fail in test"); }));
                secondInputSubmitted.countDown();
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not happen in test");
            }
        });

        try {
            try {
                assertTrue("Should complete both listeners", listenersCalled.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError("Should complete both listeners");
            }
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    @AwaitsFix(bugUrl = "desired balance")
    public void testFailListenersOnNoLongerMasterException() throws Exception {

        var node1 = createDiscoveryNode("node-1");
        var node2 = createDiscoveryNode("node-2");
        var noLongerMasterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).masterNodeId(node2.getId()))
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var rerouteServiceSupplier = new SetOnce<RerouteService>();
        var allocator = new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var dataNodeId = allocation.nodes().getDataNodes().values().iterator().next().getId();
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                var madeProgress = false;
                while (unassignedIterator.hasNext()) {
                    final var indexName = unassignedIterator.next().getIndexName();
                    if (randomBoolean() || (madeProgress == false && unassignedIterator.hasNext() == false)) {
                        unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                        madeProgress = true;
                    } else {
                        unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
                    }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(allocator, threadPool, null, null);
        var rerouteIsCalled = new CountDownLatch(1);
        rerouteServiceSupplier.set((r, p, l) -> {
            rerouteIsCalled.countDown();
            // desiredBalanceShardsAllocator.clusterChanged(new ClusterChangedEvent("reroute", noLongerMasterState, noLongerMasterState));
        });

        var allocationListenerIsCalled = new CountDownLatch(1);

        var indexMetadata = createIndex("index-1");
        var createIndexState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).masterNodeId(node1.getId()))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).incrementVersion())
            .build();

        var allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            createIndexState.mutableRoutingNodes(),
            createIndexState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );

        desiredBalanceShardsAllocator.allocate(
            allocation,
            ActionListener.wrap(
                response -> { throw new AssertionError("Should not complete in this test"); },
                exception -> allocationListenerIsCalled.countDown()
            )
        );

        try {
            assertTrue("Should call reroute", rerouteIsCalled.await(10, TimeUnit.SECONDS));
            assertTrue("Should fail listener in a following iteration", allocationListenerIsCalled.await(10, TimeUnit.SECONDS));
        } finally {
            terminate(threadPool);
        }
    }

    @AwaitsFix(bugUrl = "desired balance")
    public void testConcurrency() throws Exception {

        var threadPool = new TestThreadPool(getTestName());
        var rerouteServiceSupplier = new SetOnce<RerouteService>();
        var clusterService = ClusterServiceUtils.createClusterService(createInitialClusterState(), threadPool);
        var allocator = new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var dataNodeId = allocation.nodes().getDataNodes().values().iterator().next().getId();
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                var madeProgress = false;
                while (unassignedIterator.hasNext()) {
                    final var indexName = unassignedIterator.next().getIndexName();
                    if (randomBoolean() || (madeProgress == false && unassignedIterator.hasNext() == false)) {
                        unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                        madeProgress = true;
                    } else {
                        unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
                    }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(allocator, threadPool, clusterService, null);
        var allocationService = new AllocationService(new AllocationDeciders(List.of()), new GatewayAllocator() {
            @Override
            public void beforeAllocation(RoutingAllocation allocation) {}

            @Override
            public void allocateUnassigned(
                ShardRouting shardRouting,
                RoutingAllocation allocation,
                UnassignedAllocationHandler unassignedAllocationHandler
            ) {
                unassignedAllocationHandler.initialize(allocation.nodes().getLocalNodeId(), null, 0L, allocation.changes());
            }

            @Override
            public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}
        }, desiredBalanceShardsAllocator, () -> ClusterInfo.EMPTY, () -> SnapshotShardSizeInfo.EMPTY);

        rerouteServiceSupplier.set(new BatchedRerouteService(clusterService, allocationService::reroute));

        var indexNameGenerator = new AtomicInteger();

        var iterations = between(1, 50);
        var listenersCountdown = new CountDownLatch(iterations);
        for (int i = 0; i < iterations; i++) {
            boolean addNewIndex = i == 0 || randomInt(9) == 0;
            if (addNewIndex) {
                clusterService.submitUnbatchedStateUpdateTask("test-create-index", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        var indexName = "index-" + indexNameGenerator.incrementAndGet();
                        var newState = createIndex(currentState, indexName);
                        return allocationService.reroute(newState, "test-create-index", ActionListener.wrap(response -> {
                            var unassigned = clusterService.state().getRoutingTable().index(indexName).primaryShardsUnassigned();
                            assertThat("All shards should be initializing by this point", unassigned, equalTo(0));
                            listenersCountdown.countDown();
                        }, exception -> { throw new AssertionError("Should not fail in test"); }));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("Should not happen in test");
                    }
                });
            } else {
                clusterService.submitUnbatchedStateUpdateTask("test-reroute", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return allocationService.reroute(
                            currentState,
                            "test-reroute",
                            ActionListener.wrap(
                                response -> { listenersCountdown.countDown(); },
                                exception -> { throw new AssertionError("Should not fail in test"); }
                            )
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("Should not happen in test");
                    }
                });
            }
        }

        try {
            assertTrue("Should call all listeners", listenersCountdown.await(10, TimeUnit.SECONDS));
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    private static ClusterState createInitialClusterState() {
        var discoveryNode = createDiscoveryNode("node-0");
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).masterNodeId(discoveryNode.getId()))
            .build();
    }

    private static ClusterState createIndex(ClusterState currentState, String indexName) {
        var indexMetadata = createIndex(indexName);
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true))
            .routingTable(RoutingTable.builder(currentState.routingTable()).addAsNew(indexMetadata).incrementVersion())
            .build();
    }

    private static ClusterState createClusterState(DiscoveryNode discoveryNode, IndexMetadata indexMetadata) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).masterNodeId(discoveryNode.getId()))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();
    }

    private static DiscoveryNode createDiscoveryNode(String nodeId) {
        var transportAddress = buildNewFakeTransportAddress();
        return new DiscoveryNode(
            nodeId,
            nodeId,
            UUIDs.randomBase64UUID(random()),
            transportAddress.address().getHostString(),
            transportAddress.getAddress(),
            transportAddress,
            Map.of(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    private static IndexMetadata createIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();
    }
}
