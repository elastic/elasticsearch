/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.AllocationService.RerouteStrategy;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator.DesiredBalanceReconcilerAction;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.routing.AllocationId.newInitializing;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class DesiredBalanceShardsAllocatorTests extends ESAllocationTestCase {

    private static final String LOCAL_NODE_ID = "node-1";
    private static final String OTHER_NODE_ID = "node-2";

    public void testGatewayAllocatorPreemptsAllocation() {
        final var nodeId = randomFrom(LOCAL_NODE_ID, OTHER_NODE_ID);
        testAllocate(
            (allocation, unassignedAllocationHandler) -> unassignedAllocationHandler.initialize(nodeId, null, 0L, allocation.changes()),
            routingTable -> assertEquals(nodeId, routingTable.index("test-index").shard(0).primaryShard().currentNodeId())
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

        var localNode = newNode(LOCAL_NODE_ID);
        var otherNode = newNode(OTHER_NODE_ID);
        var initialState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        var settings = Settings.EMPTY;
        var clusterSettings = createBuiltInClusterSettings(settings);
        var clusterService = new ClusterService(
            settings,
            clusterSettings,
            new FakeThreadPoolMasterService(LOCAL_NODE_ID, threadPool, deterministicTaskQueue::scheduleNow),
            new ClusterApplierService(LOCAL_NODE_ID, settings, clusterSettings, threadPool) {
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
        var reconcileAction = new DesiredBalanceReconcilerAction() {
            @Override
            public ClusterState apply(ClusterState clusterState, RerouteStrategy routingAllocationAction) {
                return allocationServiceRef.get().executeWithRoutingAllocation(clusterState, "reconcile", routingAllocationAction);
            }
        };

        final var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            clusterSettings,
            createShardsAllocator(),
            threadPool,
            clusterService,
            reconcileAction
        );
        var allocationService = createAllocationService(desiredBalanceShardsAllocator, createGatewayAllocator(allocateUnassigned));
        allocationServiceRef.set(allocationService);

        var listenerCalled = new AtomicBoolean(false);
        clusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                var indexMetadata = createIndex("test-index");
                var newState = ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true))
                    .routingTable(
                        RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, currentState.routingTable())
                            .addAsNew(indexMetadata)
                    )
                    .build();
                return allocationService.reroute(
                    newState,
                    "test",
                    ActionTestUtils.assertNoFailureListener(response -> listenerCalled.set(true))
                );
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        deterministicTaskQueue.runAllTasks();

        try {
            assertTrue(listenerCalled.get());
            final var routingTable = clusterService.state().routingTable();
            verifier.accept(routingTable);
            final var desiredBalance = desiredBalanceShardsAllocator.getDesiredBalance();
            for (final var indexRoutingTable : routingTable) {
                for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                    final var shardRoutingTable = indexRoutingTable.shard(shardId);
                    for (final var assignedShard : shardRoutingTable.assignedShards()) {
                        assertThat(desiredBalance.getAssignment(assignedShard.shardId()).nodeIds(), hasItem(assignedShard.currentNodeId()));
                    }
                }
            }
        } finally {
            clusterService.close();
        }
    }

    public void testShouldNotRemoveAllocationDelayMarkersOnReconcile() {

        var localNode = newNode(LOCAL_NODE_ID);
        var otherNode = newNode(OTHER_NODE_ID);

        var unassignedTimeNanos = System.nanoTime();
        var computationTime = unassignedTimeNanos;
        var reconciliationTime = unassignedTimeNanos + INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(Settings.EMPTY).nanos();

        var delayedUnasssignedInfo = new UnassignedInfo(
            UnassignedInfo.Reason.NODE_RESTARTING,
            null,
            null,
            0,
            unassignedTimeNanos,
            TimeValue.nsecToMSec(unassignedTimeNanos),
            true,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Set.of(),
            "node-3"
        );

        var inSyncAllocationId = UUIDs.randomBase64UUID();
        var index = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putInSyncAllocationIds(0, Set.of(inSyncAllocationId))
            .build();
        var shardId = new ShardId(index.getIndex(), 0);
        var indexRoutingTable = IndexRoutingTable.builder(index.getIndex())
            .addShard(newShardRouting(shardId, LOCAL_NODE_ID, null, true, ShardRoutingState.STARTED, newInitializing(inSyncAllocationId)))
            .addShard(newShardRouting(shardId, null, null, false, ShardRoutingState.UNASSIGNED, delayedUnasssignedInfo))
            .build();

        var initialState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .metadata(Metadata.builder().put(index, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).add(indexRoutingTable).build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(initialState, threadPool);
        var allocationServiceRef = new SetOnce<AllocationService>();
        var reconciledStateRef = new AtomicReference<ClusterState>();
        var reconcileAction = new DesiredBalanceReconcilerAction() {
            @Override
            public ClusterState apply(ClusterState clusterState, RerouteStrategy routingAllocationAction) {
                ClusterState reconciled = allocationServiceRef.get()
                    .executeWithRoutingAllocation(clusterState, "reconcile", routingAllocationAction);
                reconciledStateRef.set(reconciled);
                return reconciled;
            }
        };

        var clusterSettings = createBuiltInClusterSettings();
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            clusterSettings,
            createShardsAllocator(),
            threadPool,
            clusterService,
            reconcileAction
        );
        var allocationService = new AllocationService(
            new AllocationDeciders(List.of()),
            createGatewayAllocator(
                (allocation, unassignedAllocationHandler) -> unassignedAllocationHandler.removeAndIgnore(
                    UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                    allocation.changes()
                )
            ),
            desiredBalanceShardsAllocator,
            () -> ClusterInfo.EMPTY,
            () -> SnapshotShardSizeInfo.EMPTY,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ) {

            int call = 0;
            long[] timeToReturn = new long[] { computationTime, reconciliationTime };

            @Override
            protected long currentNanoTime() {
                return timeToReturn[call++];
            }
        };
        allocationServiceRef.set(allocationService);

        try {
            rerouteAndWait(allocationService, initialState, "test");

            var reconciledState = reconciledStateRef.get();

            // Desired balance computation could be performed _before_ delayed allocation is expired
            // while corresponding reconciliation might happen _after_.
            // In such case reconciliation will not allocate delayed shard as its balance is not computed yet
            // and must NOT clear delayed flag so that a followup reroute is scheduled for the shard.
            var unassigned = reconciledState.getRoutingNodes().unassigned();
            assertThat(unassigned.size(), equalTo(1));
            var unassignedShard = unassigned.iterator().next();
            assertThat(unassignedShard.unassignedInfo().isDelayed(), equalTo(true));

        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testCallListenersOnlyAfterProducingFreshInput() throws InterruptedException {

        var reconciliations = new AtomicInteger(0);
        var listenersCalled = new CountDownLatch(2);
        var clusterStateUpdatesExecuted = new CountDownLatch(2);

        var discoveryNode = newNode("node-0");
        var initialState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).masterNodeId(discoveryNode.getId()))
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(initialState, threadPool);
        var allocationServiceRef = new SetOnce<AllocationService>();
        var reconcileAction = new DesiredBalanceReconcilerAction() {
            @Override
            public ClusterState apply(ClusterState clusterState, RerouteStrategy routingAllocationAction) {
                reconciliations.incrementAndGet();
                return allocationServiceRef.get().executeWithRoutingAllocation(clusterState, "reconcile", routingAllocationAction);
            }
        };

        var gatewayAllocator = createGatewayAllocator();
        var shardsAllocator = createShardsAllocator();
        var clusterSettings = createBuiltInClusterSettings();
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            shardsAllocator,
            threadPool,
            clusterService,
            new DesiredBalanceComputer(clusterSettings, threadPool, shardsAllocator) {
                @Override
                public DesiredBalance compute(
                    DesiredBalance previousDesiredBalance,
                    DesiredBalanceInput desiredBalanceInput,
                    Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
                    Predicate<DesiredBalanceInput> isFresh
                ) {
                    try {
                        // simulate slow balance computation
                        assertTrue(clusterStateUpdatesExecuted.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    return super.compute(previousDesiredBalance, desiredBalanceInput, pendingDesiredBalanceMoves, isFresh);
                }
            },
            reconcileAction
        );
        var allocationService = createAllocationService(desiredBalanceShardsAllocator, gatewayAllocator);
        allocationServiceRef.set(allocationService);

        class CreateIndexTask extends ClusterStateUpdateTask {
            private final String indexName;

            private CreateIndexTask(String indexName) {
                this.indexName = indexName;
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                var indexMetadata = createIndex(indexName);
                var newState = ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true))
                    .routingTable(
                        RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, currentState.routingTable())
                            .addAsNew(indexMetadata)
                    )
                    .build();
                return allocationService.reroute(newState, "test", ActionTestUtils.assertNoFailureListener(response -> {
                    assertThat(
                        "All shards should be initializing by the time listener is called",
                        clusterService.state().getRoutingTable().index(indexName).primaryShardsUnassigned(),
                        equalTo(0)
                    );
                    assertThat(reconciliations.get(), equalTo(1));
                    listenersCalled.countDown();
                }));
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                clusterStateUpdatesExecuted.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }

        clusterService.submitUnbatchedStateUpdateTask("test", new CreateIndexTask("index-1"));
        clusterService.submitUnbatchedStateUpdateTask("test", new CreateIndexTask("index-2"));

        try {
            assertTrue(listenersCalled.await(10, TimeUnit.SECONDS));
            assertThat("Expected single reconciliation after both state updates", reconciliations.get(), equalTo(1));
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testFailListenersOnNoLongerMasterException() throws InterruptedException {

        var listenersCalled = new CountDownLatch(1);
        var newMasterElected = new CountDownLatch(1);
        var clusterStateUpdatesExecuted = new CountDownLatch(1);

        var node1 = newNode(LOCAL_NODE_ID);
        var node2 = newNode(OTHER_NODE_ID);
        var initial = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).masterNodeId(node1.getId()))
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(initial, threadPool);
        var allocationServiceRef = new SetOnce<AllocationService>();
        var reconcileAction = new DesiredBalanceReconcilerAction() {
            @Override
            public ClusterState apply(ClusterState clusterState, RerouteStrategy routingAllocationAction) {
                return allocationServiceRef.get().executeWithRoutingAllocation(clusterState, "reconcile", routingAllocationAction);
            }
        };

        var gatewayAllocator = createGatewayAllocator();
        var shardsAllocator = createShardsAllocator();
        var clusterSettings = createBuiltInClusterSettings();
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            shardsAllocator,
            threadPool,
            clusterService,
            new DesiredBalanceComputer(clusterSettings, threadPool, shardsAllocator) {
                @Override
                public DesiredBalance compute(
                    DesiredBalance previousDesiredBalance,
                    DesiredBalanceInput desiredBalanceInput,
                    Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
                    Predicate<DesiredBalanceInput> isFresh
                ) {
                    try {
                        // fake slow balance computation
                        assertTrue(newMasterElected.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    return super.compute(previousDesiredBalance, desiredBalanceInput, pendingDesiredBalanceMoves, isFresh);
                }
            },
            reconcileAction
        );

        var allocationService = createAllocationService(desiredBalanceShardsAllocator, gatewayAllocator);
        allocationServiceRef.set(allocationService);

        clusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                var indexMetadata = createIndex("index-1");
                var newState = ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true))
                    .routingTable(
                        RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, currentState.routingTable())
                            .addAsNew(indexMetadata)
                    )
                    .build();
                return allocationService.reroute(newState, "test", ActionListener.wrap(response -> {
                    throw new AssertionError("Should not happen in test");
                }, exception -> listenersCalled.countDown()));
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                clusterStateUpdatesExecuted.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        // await
        assertTrue(clusterStateUpdatesExecuted.await(5, TimeUnit.SECONDS));

        var noLongerMaster = ClusterState.builder(clusterService.state())
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).masterNodeId(node2.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, noLongerMaster);

        newMasterElected.countDown();

        try {
            assertTrue(listenersCalled.await(10, TimeUnit.SECONDS));
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testResetDesiredBalance() {

        var node1 = newNode(LOCAL_NODE_ID);
        var node2 = newNode(OTHER_NODE_ID);

        var shardId = new ShardId("test-index", UUIDs.randomBase64UUID(), 0);
        var index = createIndex(shardId.getIndexName());
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).masterNodeId(node1.getId()))
            .metadata(Metadata.builder().put(index, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(index).build())
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);
        var delegateAllocator = createShardsAllocator();
        var clusterSettings = createBuiltInClusterSettings();

        var desiredBalanceComputer = new DesiredBalanceComputer(clusterSettings, threadPool, delegateAllocator) {

            final AtomicReference<DesiredBalance> lastComputationInput = new AtomicReference<>();

            @Override
            public DesiredBalance compute(
                DesiredBalance previousDesiredBalance,
                DesiredBalanceInput desiredBalanceInput,
                Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
                Predicate<DesiredBalanceInput> isFresh
            ) {
                lastComputationInput.set(previousDesiredBalance);
                return super.compute(previousDesiredBalance, desiredBalanceInput, pendingDesiredBalanceMoves, isFresh);
            }
        };

        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            delegateAllocator,
            threadPool,
            clusterService,
            desiredBalanceComputer,
            (reconcilerClusterState, rerouteStrategy) -> reconcilerClusterState
        );

        var service = createAllocationService(desiredBalanceShardsAllocator, createGatewayAllocator());

        try {
            // initial computation is based on DesiredBalance.INITIAL
            rerouteAndWait(service, clusterState, "initial-allocation");
            assertThat(desiredBalanceComputer.lastComputationInput.get(), equalTo(DesiredBalance.INITIAL));

            // any next computation is based on current desired balance
            var current = desiredBalanceShardsAllocator.getDesiredBalance();
            rerouteAndWait(service, clusterState, "next-allocation");
            assertThat(desiredBalanceComputer.lastComputationInput.get(), equalTo(current));

            // when desired balance is resetted then computation is based on balance with no previous assignments
            desiredBalanceShardsAllocator.resetDesiredBalance();
            current = desiredBalanceShardsAllocator.getDesiredBalance();
            rerouteAndWait(service, clusterState, "reset-desired-balance");
            assertThat(
                desiredBalanceComputer.lastComputationInput.get(),
                equalTo(new DesiredBalance(current.lastConvergedIndex(), Map.of()))
            );
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testResetDesiredBalanceOnNoLongerMaster() {

        var node1 = newNode(LOCAL_NODE_ID);
        var node2 = newNode(OTHER_NODE_ID);

        var shardId = new ShardId("test-index", UUIDs.randomBase64UUID(), 0);
        var index = createIndex(shardId.getIndexName());
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).masterNodeId(node1.getId()))
            .metadata(Metadata.builder().put(index, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(index).build())
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);

        var delegateAllocator = createShardsAllocator();
        var desiredBalanceComputer = new DesiredBalanceComputer(createBuiltInClusterSettings(), threadPool, delegateAllocator);
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            delegateAllocator,
            threadPool,
            clusterService,
            desiredBalanceComputer,
            (reconcilerClusterState, rerouteStrategy) -> reconcilerClusterState
        );

        var service = createAllocationService(desiredBalanceShardsAllocator, createGatewayAllocator());

        try {
            rerouteAndWait(service, clusterState, "initial-allocation");
            assertThat(desiredBalanceShardsAllocator.getDesiredBalance(), not(equalTo(DesiredBalance.INITIAL)));

            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.getNodes()).localNodeId(node1.getId()).masterNodeId(node2.getId()))
                .build();
            ClusterServiceUtils.setState(clusterService, clusterState);

            assertThat(
                "desired balance should be resetted on no longer master",
                desiredBalanceShardsAllocator.getDesiredBalance(),
                equalTo(DesiredBalance.INITIAL)
            );
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    private static IndexMetadata createIndex(String name) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
    }

    private static AllocationService createAllocationService(
        DesiredBalanceShardsAllocator desiredBalanceShardsAllocator,
        GatewayAllocator gatewayAllocator
    ) {
        return new AllocationService(
            new AllocationDeciders(List.of()),
            gatewayAllocator,
            desiredBalanceShardsAllocator,
            () -> ClusterInfo.EMPTY,
            () -> SnapshotShardSizeInfo.EMPTY,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
    }

    private static GatewayAllocator createGatewayAllocator() {
        return createGatewayAllocator(DesiredBalanceShardsAllocatorTests::initialize);
    }

    private static void initialize(RoutingAllocation allocation, ExistingShardsAllocator.UnassignedAllocationHandler handler) {
        handler.initialize(allocation.nodes().getLocalNodeId(), null, 0L, allocation.changes());
    }

    private static GatewayAllocator createGatewayAllocator(
        BiConsumer<RoutingAllocation, ExistingShardsAllocator.UnassignedAllocationHandler> allocateUnassigned
    ) {
        return new GatewayAllocator() {

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
        };
    }

    private static ShardsAllocator createShardsAllocator() {
        return new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                var dataNodeId = allocation.nodes().getDataNodes().values().iterator().next().getId();
                var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    unassignedIterator.next();
                    unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };
    }

    private static void rerouteAndWait(AllocationService service, ClusterState clusterState, String reason) {
        PlainActionFuture.<Void, RuntimeException>get(f -> service.reroute(clusterState, reason, f), 10, TimeUnit.SECONDS);
    }
}
