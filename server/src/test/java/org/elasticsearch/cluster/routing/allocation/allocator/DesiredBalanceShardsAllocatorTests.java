/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
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
import org.elasticsearch.common.time.TimeProviderUtils;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.routing.AllocationId.newInitializing;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DesiredBalanceShardsAllocatorTests extends ESAllocationTestCase {

    private static final String LOCAL_NODE_ID = "node-1";
    private static final String OTHER_NODE_ID = "node-2";

    public void testGatewayAllocatorPreemptsAllocation() {
        final var nodeId = randomFrom(LOCAL_NODE_ID, OTHER_NODE_ID);
        testAllocate(
            (shardRouting, allocation, unassignedAllocationHandler) -> unassignedAllocationHandler.initialize(
                nodeId,
                null,
                0L,
                allocation.changes()
            ),
            routingTable -> assertEquals(nodeId, routingTable.index("test-index").shard(0).primaryShard().currentNodeId())
        );
    }

    public void testGatewayAllocatorStillFetching() {
        testAllocate(
            (shardRouting, allocation, unassignedAllocationHandler) -> unassignedAllocationHandler.removeAndIgnore(
                UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA,
                allocation.changes()
            ),
            routingTable -> {
                var shardRouting = routingTable.shardRoutingTable("test-index", 0).primaryShard();
                assertFalse(shardRouting.assignedToNode());
                assertThat(
                    shardRouting.unassignedInfo().lastAllocationStatus(),
                    equalTo(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA)
                );
            }
        );
    }

    public void testGatewayAllocatorDoesNothing() {
        testAllocate((shardRouting, allocation, unassignedAllocationHandler) -> {}, routingTable -> {
            var shardRouting = routingTable.shardRoutingTable("test-index", 0).primaryShard();
            assertTrue(shardRouting.assignedToNode());// assigned by a followup reconciliation
            assertThat(shardRouting.unassignedInfo().lastAllocationStatus(), equalTo(UnassignedInfo.AllocationStatus.NO_ATTEMPT));
        });
    }

    public void testAllocate(AllocateUnassignedHandler allocateUnassigned, Consumer<RoutingTable> verifier) {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var localNode = newNode(LOCAL_NODE_ID);
        var otherNode = newNode(OTHER_NODE_ID);
        var initialState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        final var settings = Settings.builder()
            // disable thread watchdog to avoid infinitely repeating task
            .put(ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.ZERO)
            .build();

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
            reconcileAction,
            EMPTY_NODE_ALLOCATION_STATS,
            TEST_ONLY_EXPLAINER,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        );
        assertValidStats(desiredBalanceShardsAllocator.getStats());
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
            assertValidStats(desiredBalanceShardsAllocator.getStats());
        } finally {
            clusterService.close();
        }
    }

    private void assertValidStats(DesiredBalanceStats stats) {
        assertThat(stats.lastConvergedIndex(), greaterThanOrEqualTo(0L));
        try {
            assertEquals(stats, copyWriteable(stats, writableRegistry(), DesiredBalanceStats::readFrom));
        } catch (Exception e) {
            fail(e);
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
            .addShard(
                shardRoutingBuilder(shardId, LOCAL_NODE_ID, true, ShardRoutingState.STARTED).withAllocationId(
                    newInitializing(inSyncAllocationId)
                ).build()
            )
            .addShard(
                shardRoutingBuilder(shardId, null, false, ShardRoutingState.UNASSIGNED).withUnassignedInfo(delayedUnasssignedInfo).build()
            )
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
            reconcileAction,
            EMPTY_NODE_ALLOCATION_STATS,
            TEST_ONLY_EXPLAINER,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        );
        var allocationService = new AllocationService(
            new AllocationDeciders(List.of()),
            createGatewayAllocator(
                (shardRouting, allocation, unassignedAllocationHandler) -> unassignedAllocationHandler.removeAndIgnore(
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
            assertThat(unassignedShard.unassignedInfo().delayed(), equalTo(true));

        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testIndexCreationInterruptsLongDesiredBalanceComputation() throws Exception {
        var discoveryNode = newNode("node-0");
        var initialState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).masterNodeId(discoveryNode.getId()))
            .build();
        final var ignoredIndexName = "index-ignored";

        var threadPool = new TestThreadPool(getTestName());
        var time = new AtomicLong(threadPool.relativeTimeInMillis());
        var clusterService = ClusterServiceUtils.createClusterService(initialState, threadPool);
        var allocationServiceRef = new SetOnce<AllocationService>();
        var reconcileAction = new DesiredBalanceReconcilerAction() {
            @Override
            public ClusterState apply(ClusterState clusterState, RerouteStrategy routingAllocationAction) {
                return allocationServiceRef.get().executeWithRoutingAllocation(clusterState, "reconcile", routingAllocationAction);
            }
        };

        var gatewayAllocator = createGatewayAllocator((shardRouting, allocation, unassignedAllocationHandler) -> {
            if (shardRouting.getIndexName().equals(ignoredIndexName)) {
                unassignedAllocationHandler.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
            }
        });
        var shardsAllocator = new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                // simulate long computation
                time.addAndGet(1_000);
                var dataNodeId = allocation.nodes().getDataNodes().values().iterator().next().getId();
                var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    unassignedIterator.next();
                    unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                }
                allocation.routingNodes().setBalanceWeightStatsPerNode(Map.of());
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };

        // Make sure the computation takes at least a few iterations, where each iteration takes 1s (see {@code #shardsAllocator.allocate}).
        // By setting the following setting we ensure the desired balance computation will be interrupted early to not delay assigning
        // newly created primary shards. This ensures that we hit a desired balance computation (3s) which is longer than the configured
        // setting below.
        var clusterSettings = createBuiltInClusterSettings(
            Settings.builder().put(DesiredBalanceComputer.MAX_BALANCE_COMPUTATION_TIME_DURING_INDEX_CREATION_SETTING.getKey(), "2s").build()
        );
        final int minIterations = between(3, 10);
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            shardsAllocator,
            threadPool,
            clusterService,
            new DesiredBalanceComputer(clusterSettings, TimeProviderUtils.create(time::get), shardsAllocator, TEST_ONLY_EXPLAINER) {
                @Override
                public DesiredBalance compute(
                    DesiredBalance previousDesiredBalance,
                    DesiredBalanceInput desiredBalanceInput,
                    Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
                    Predicate<DesiredBalanceInput> isFresh
                ) {
                    return super.compute(previousDesiredBalance, desiredBalanceInput, pendingDesiredBalanceMoves, isFresh);
                }

                @Override
                boolean hasEnoughIterations(int currentIteration) {
                    return currentIteration >= minIterations;
                }
            },
            reconcileAction,
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        );
        var allocationService = createAllocationService(desiredBalanceShardsAllocator, gatewayAllocator);
        allocationServiceRef.set(allocationService);

        var rerouteFinished = new CyclicBarrier(2);
        // A mock cluster state update task for creating an index
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
                return allocationService.reroute(
                    newState,
                    "test",
                    ActionTestUtils.assertNoFailureListener(response -> safeAwait(rerouteFinished))
                );
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }

        final var computationInterruptedMessage =
            "Desired balance computation for * interrupted * in order to not delay assignment of newly created index shards *";
        try {
            // Create a new index which is not ignored and therefore must be considered when a desired balance
            // computation takes longer than 2s.
            assertThat(desiredBalanceShardsAllocator.getStats().computationExecuted(), equalTo(0L));
            MockLog.assertThatLogger(() -> {
                clusterService.submitUnbatchedStateUpdateTask("test", new CreateIndexTask("index-1"));
                safeAwait(rerouteFinished);
                assertThat(clusterService.state().getRoutingTable().index("index-1").primaryShardsUnassigned(), equalTo(0));
            },
                DesiredBalanceComputer.class,
                new MockLog.SeenEventExpectation(
                    "Should log interrupted computation",
                    DesiredBalanceComputer.class.getCanonicalName(),
                    Level.INFO,
                    computationInterruptedMessage
                )
            );
            assertBusy(() -> assertFalse(desiredBalanceShardsAllocator.getStats().computationActive()));
            assertThat(desiredBalanceShardsAllocator.getStats().computationExecuted(), equalTo(2L));
            // The computation should not get interrupted when the newly created index shard stays unassigned.
            MockLog.assertThatLogger(() -> {
                clusterService.submitUnbatchedStateUpdateTask("test", new CreateIndexTask(ignoredIndexName));
                safeAwait(rerouteFinished);
                assertThat(clusterService.state().getRoutingTable().index(ignoredIndexName).primaryShardsUnassigned(), equalTo(1));
            },
                DesiredBalanceComputer.class,
                new MockLog.UnseenEventExpectation(
                    "Should log interrupted computation",
                    DesiredBalanceComputer.class.getCanonicalName(),
                    Level.INFO,
                    computationInterruptedMessage
                )
            );
            assertBusy(() -> assertFalse(desiredBalanceShardsAllocator.getStats().computationActive()));
            assertThat(desiredBalanceShardsAllocator.getStats().computationExecuted(), equalTo(3L));
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
            new DesiredBalanceComputer(clusterSettings, threadPool, shardsAllocator, TEST_ONLY_EXPLAINER) {
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
            reconcileAction,
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
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
            new DesiredBalanceComputer(clusterSettings, threadPool, shardsAllocator, TEST_ONLY_EXPLAINER) {
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
            reconcileAction,
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
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
            assertThat(desiredBalanceShardsAllocator.getDesiredBalance(), sameInstance(DesiredBalance.NOT_MASTER));
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

        var desiredBalanceComputer = new DesiredBalanceComputer(clusterSettings, threadPool, delegateAllocator, TEST_ONLY_EXPLAINER) {

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
            (reconcilerClusterState, rerouteStrategy) -> reconcilerClusterState,
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        );

        var service = createAllocationService(desiredBalanceShardsAllocator, createGatewayAllocator());

        try {
            // initial computation is based on DesiredBalance.INITIAL
            rerouteAndWait(service, clusterState, "initial-allocation");
            assertThat(desiredBalanceComputer.lastComputationInput.get(), equalTo(DesiredBalance.BECOME_MASTER_INITIAL));

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

    public void testSimulateStartedShardsWithinASingleClusterInfoPolling() {
        final var firstNode = newNode("node-1");
        final var secondNode = newNode("node-2");
        final var thirdNode = newNode("node-3");
        final DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().add(firstNode).add(secondNode).add(thirdNode);
        discoveryNodesBuilder.localNodeId(firstNode.getId()).masterNodeId(firstNode.getId());

        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(ProjectId.DEFAULT);
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        final var indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var shardId0 = new ShardId(indexMetadata.getIndex(), 0);
        final ShardRouting shardRouting0 = ShardRoutingHelper.moveToStarted(
            ShardRoutingHelper.initialize(
                ShardRouting.newUnassigned(
                    shardId0,
                    true,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "new index"),
                    ShardRouting.Role.DEFAULT
                ),
                firstNode.getId()
            )
        );

        final ShardId shardId1 = new ShardId(indexMetadata.getIndex(), 1);
        final ShardRouting shardRouting1 = ShardRoutingHelper.initialize(
            ShardRouting.newUnassigned(
                shardId1,
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "new index"),
                ShardRouting.Role.DEFAULT
            ),
            thirdNode.getId()
        );
        routingTableBuilder.add(IndexRoutingTable.builder(shardId0.getIndex()).addShard(shardRouting0).addShard(shardRouting1).build());
        projectBuilder.put(
            IndexMetadata.builder(indexMetadata).putInSyncAllocationIds(0, Set.of(shardRouting0.allocationId().getId())).build(),
            false
        );

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(Metadata.builder().put(projectBuilder))
            .putRoutingTable(ProjectId.DEFAULT, routingTableBuilder.build())
            .build();

        var threadPool = new TestThreadPool(getTestName());
        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);

        final var relocated = new AtomicBoolean(false);
        final AtomicReference<ClusterInfo> clusterInfoUsedByAllocator = new AtomicReference<>();
        var delegateAllocator = new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                allocation.routingNodes().setBalanceWeightStatsPerNode(Map.of());
                clusterInfoUsedByAllocator.set(allocation.clusterInfo());
                if (relocated.compareAndSet(false, true)) {
                    logger.info("--> relocating shard [{}]", shardId0);
                    final ShardRouting shardRouting = allocation.routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId0).primaryShard();
                    allocation.routingNodes().relocateShard(shardRouting, secondNode.getId(), 100, "test", allocation.changes());
                }
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };

        final var allocationServiceRef = new AtomicReference<AllocationService>();
        var clusterSettings = createBuiltInClusterSettings();
        var desiredBalanceComputer = new DesiredBalanceComputer(clusterSettings, threadPool, delegateAllocator, TEST_ONLY_EXPLAINER);
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            delegateAllocator,
            threadPool,
            clusterService,
            desiredBalanceComputer,
            (reconcilerClusterState, rerouteStrategy) -> allocationServiceRef.get()
                .executeWithRoutingAllocation(reconcilerClusterState, "reconcile-desired-balance", rerouteStrategy),
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        ) {
            @Override
            protected void reconcile(DesiredBalance desiredBalance, RoutingAllocation allocation) {
                logger.info("--> reconcile called");
                super.reconcile(desiredBalance, allocation);
            }
        };

        // A fixed ClusterInfo from one polling cycle with hotspot on first node
        final var zeroUsageStats = new ThreadPoolUsageStats(10, 0.0f, 0);
        final Map<String, NodeUsageStatsForThreadPools> initialThreadPoolStats = Map.of(
            firstNode.getId(),
            new NodeUsageStatsForThreadPools(firstNode.getId(), Map.of(ThreadPool.Names.WRITE, new ThreadPoolUsageStats(10, 10.0f, 30))),
            secondNode.getId(),
            new NodeUsageStatsForThreadPools(secondNode.getId(), Map.of(ThreadPool.Names.WRITE, zeroUsageStats)),
            thirdNode.getId(),
            new NodeUsageStatsForThreadPools(secondNode.getId(), Map.of(ThreadPool.Names.WRITE, zeroUsageStats))
        );

        final var clusterInfoRef = new AtomicReference<>(
            ClusterInfo.builder()
                .dataPath(Map.of(ClusterInfo.NodeAndShard.from(shardRouting0), "/data/path0"))
                .shardWriteLoads(Map.of(shardId0, 10.0d, shardId1, 10.0d))
                .nodeUsageStatsForThreadPools(initialThreadPoolStats)
                .build()
        );

        var service = new AllocationService(
            new AllocationDeciders(List.of()),
            createGatewayAllocator(),
            desiredBalanceShardsAllocator,
            clusterInfoRef::get,
            () -> SnapshotShardSizeInfo.EMPTY,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        allocationServiceRef.set(service);

        try {
            // 1. Initial reroute should produce a desired balance which moves the shard from first node to second node.
            rerouteAndWait(service, clusterState, "first-reroute");
            final DesiredBalance desiredBalance = desiredBalanceShardsAllocator.getDesiredBalance();
            assertNotNull(desiredBalance.assignments().get(shardId0));

            // When compute ends, the last ClusterInfo it uses is an updated one different from the initial one because the simulation
            // starts all initializing shards on their target nodes. Thread pool stats are updated to reflect the shard movements.
            final ClusterInfo updatedClusterInfo = clusterInfoUsedByAllocator.get();
            assertThat(updatedClusterInfo, not(equalTo(clusterInfoRef.get())));

            // First node has reduced utilization and zero latency since shard0 has moved away
            final var firstNodeUpdatedStats = updatedClusterInfo.getNodeUsageStatsForThreadPools()
                .get(firstNode.getId())
                .threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE);
            assertThat(
                firstNodeUpdatedStats.averageThreadPoolUtilization(),
                equalTo(
                    initialThreadPoolStats.get(firstNode.getId())
                        .threadPoolUsageStatsMap()
                        .get(ThreadPool.Names.WRITE)
                        .averageThreadPoolUtilization() - 1.0f
                )
            );
            assertThat(firstNodeUpdatedStats.maxThreadPoolQueueLatencyMillis(), equalTo(0L));

            // Second node has increased utilization since shard0 moved onto it. Latency does not change for incoming shards
            final var secondNodeUpdatedStats = updatedClusterInfo.getNodeUsageStatsForThreadPools()
                .get(secondNode.getId())
                .threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE);
            assertThat(secondNodeUpdatedStats.averageThreadPoolUtilization(), equalTo(1.0f));
            assertThat(secondNodeUpdatedStats.maxThreadPoolQueueLatencyMillis(), equalTo(0L));

            // Third node has increased utilization since shard1 started on it
            final var thirdNodeUpdatedStats = updatedClusterInfo.getNodeUsageStatsForThreadPools()
                .get(thirdNode.getId())
                .threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE);
            assertThat(thirdNodeUpdatedStats.averageThreadPoolUtilization(), equalTo(1.0f));

            // 2. Reroute again and the simulated ClusterInfo remains the same due to no new change
            rerouteAndWait(service, clusterService.state(), "reroute-with-desired-shard-movements");
            assertThat(clusterInfoUsedByAllocator.get(), equalTo(updatedClusterInfo));

            // 3. Wait until reconciliation is completed for the moved shard. This means shard0 is now relocating in the
            // actual cluster state, i.e. not just in the desired balance assignments.
            ClusterServiceUtils.addTemporaryStateListener(
                clusterService,
                state -> state.routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId0).primaryShard().relocating()
            );
            // Reroute again and the ClusterInfo simulation result remains unchanged and correctly accounts for all shard movements
            // (reconciled or not)
            rerouteAndWait(service, clusterService.state(), "reroute-with-reconciled-shard-movement");
            assertThat(clusterInfoUsedByAllocator.get(), equalTo(updatedClusterInfo));

            // 4. Actually start the relocating shard0 on the target node-2, i.e. action to honour the reconciliation result
            ClusterServiceUtils.setState(
                clusterService,
                service.applyStartedShards(
                    clusterService.state(),
                    List.of(
                        clusterService.state()
                            .routingTable(ProjectId.DEFAULT)
                            .shardRoutingTable(shardId0)
                            .primaryShard()
                            .getTargetRelocatingShard()
                    )
                )
            );
            assertThat(
                clusterService.state().routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId0).primaryShard().currentNodeId(),
                equalTo(secondNode.getId())
            );
            // The actually started shard0 is still accounted for when simulating ClusterInfo
            rerouteAndWait(service, clusterService.state(), "reroute-with-actual-relocating-shard-started-event");
            assertThat(clusterInfoUsedByAllocator.get(), equalTo(updatedClusterInfo));

            // 5. Also start the initializing shard1 on the target node-3
            ClusterServiceUtils.setState(
                clusterService,
                service.applyStartedShards(
                    clusterService.state(),
                    List.of(clusterService.state().routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId1).primaryShard())
                )
            );
            assertThat(
                clusterService.state().routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId1).primaryShard().currentNodeId(),
                equalTo(thirdNode.getId())
            );
            // The actually started shard1 is accounted for when simulating ClusterInfo
            rerouteAndWait(service, clusterService.state(), "reroute-with-actual-initializing-shard-started-event");
            assertThat(clusterInfoUsedByAllocator.get(), equalTo(updatedClusterInfo));

            // 6. A new ClusterInfo is polled
            clusterInfoRef.set(
                ClusterInfo.builder()
                    .dataPath(
                        Map.of(
                            new ClusterInfo.NodeAndShard(secondNode.getId(), shardId0),
                            "/data/path0",
                            new ClusterInfo.NodeAndShard(thirdNode.getId(), shardId1),
                            "/data/path1"
                        )
                    )
                    .shardWriteLoads(Map.of(shardId0, 10.0d, shardId1, 1.0d))
                    .nodeUsageStatsForThreadPools(
                        Map.of(
                            firstNode.getId(),
                            new NodeUsageStatsForThreadPools(
                                firstNode.getId(),
                                Map.of(ThreadPool.Names.WRITE, new ThreadPoolUsageStats(10, 5.0f, 30))
                            ),
                            secondNode.getId(),
                            new NodeUsageStatsForThreadPools(
                                secondNode.getId(),
                                Map.of(ThreadPool.Names.WRITE, new ThreadPoolUsageStats(10, 5.0f, 30))
                            ),
                            thirdNode.getId(),
                            new NodeUsageStatsForThreadPools(
                                secondNode.getId(),
                                Map.of(ThreadPool.Names.WRITE, new ThreadPoolUsageStats(10, 1.0f, 5))
                            )
                        )
                    )
                    .build()
            );
            // ClusterInfo is updated from the new polling and no adjustment is applied onto the new ClusterInfo due to no movement since
            rerouteAndWait(service, clusterService.state(), "reroute-after-new-cluster-info-polled");
            assertThat(clusterInfoUsedByAllocator.get(), equalTo(clusterInfoRef.get()));
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
        var desiredBalanceComputer = new DesiredBalanceComputer(
            createBuiltInClusterSettings(),
            threadPool,
            delegateAllocator,
            TEST_ONLY_EXPLAINER
        );
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            delegateAllocator,
            threadPool,
            clusterService,
            desiredBalanceComputer,
            (reconcilerClusterState, rerouteStrategy) -> reconcilerClusterState,
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        );

        var service = createAllocationService(desiredBalanceShardsAllocator, createGatewayAllocator());

        try {
            rerouteAndWait(service, clusterState, "initial-allocation");
            assertThat(desiredBalanceShardsAllocator.getDesiredBalance(), not(equalTo(DesiredBalance.BECOME_MASTER_INITIAL)));

            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.getNodes()).localNodeId(node1.getId()).masterNodeId(node2.getId()))
                .build();
            ClusterServiceUtils.setState(clusterService, clusterState);

            assertThat(
                "desired balance should be resetted on no longer master",
                desiredBalanceShardsAllocator.getDesiredBalance(),
                equalTo(DesiredBalance.NOT_MASTER)
            );
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testResetDesiredBalanceOnNodeShutdown() {
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

        final var resetCalled = new AtomicBoolean();
        var delegateAllocator = createShardsAllocator();
        var desiredBalanceComputer = new DesiredBalanceComputer(
            createBuiltInClusterSettings(),
            threadPool,
            delegateAllocator,
            TEST_ONLY_EXPLAINER
        );
        var desiredBalanceAllocator = new DesiredBalanceShardsAllocator(
            delegateAllocator,
            threadPool,
            clusterService,
            desiredBalanceComputer,
            (reconcilerClusterState, rerouteStrategy) -> reconcilerClusterState,
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        ) {
            @Override
            public void resetDesiredBalance() {
                resetCalled.set(true);
                super.resetDesiredBalance();
            }
        };

        var service = createAllocationService(desiredBalanceAllocator, createGatewayAllocator());

        try {
            rerouteAndWait(service, clusterState, "initial-allocation");
            assertThat(desiredBalanceAllocator.getDesiredBalance(), not(equalTo(DesiredBalance.BECOME_MASTER_INITIAL)));

            final var shutdownType = randomFrom(Type.SIGTERM, Type.REMOVE, Type.REPLACE);
            final var singleShutdownMetadataBuilder = SingleNodeShutdownMetadata.builder()
                .setNodeId(node2.getId())
                .setNodeEphemeralId(node2.getEphemeralId())
                .setReason("test")
                .setType(shutdownType)
                .setStartedAtMillis(randomNonNegativeLong());
            if (shutdownType.equals(Type.REPLACE)) {
                singleShutdownMetadataBuilder.setTargetNodeName(randomIdentifier());
            } else if (shutdownType.equals(Type.SIGTERM)) {
                singleShutdownMetadataBuilder.setGracePeriod(TimeValue.MAX_VALUE);
            }
            final var nodeShutdownMetadata = new NodesShutdownMetadata(Map.of(node2.getId(), singleShutdownMetadataBuilder.build()));
            // Add shutdown marker
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
                .build();
            assertTrue(desiredBalanceAllocator.getProcessedNodeShutdowns().isEmpty());
            rerouteAndWait(service, clusterState, "reroute-after-shutdown");
            assertTrue("desired balance reset should be called on node shutdown", resetCalled.get());
            assertThat(desiredBalanceAllocator.getProcessedNodeShutdowns(), equalTo(Set.of(node2.getId())));

            resetCalled.set(false);
            rerouteAndWait(service, clusterState, "random-reroute");
            assertFalse("desired balance reset should not be called again for processed shutdowns", resetCalled.get());
            assertThat(desiredBalanceAllocator.getProcessedNodeShutdowns(), equalTo(Set.of(node2.getId())));
            // Node may or may not have been removed
            final var removeNodeFromCluster = randomBoolean();
            if (removeNodeFromCluster) {
                clusterState = ClusterState.builder(clusterState)
                    .nodes(DiscoveryNodes.builder().add(node1).localNodeId(node1.getId()).masterNodeId(node1.getId()))
                    .build();
            }
            rerouteAndWait(service, clusterState, "random-reroute");
            assertFalse("desired balance reset should not be called again for processed shutdowns", resetCalled.get());
            // Remove the shutdown marker
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY))
                .build();
            rerouteAndWait(service, clusterState, "random-reroute");
            if (removeNodeFromCluster) {
                assertFalse("desired balance reset should not be called again for processed shutdowns", resetCalled.get());
            } else {
                assertTrue("desired balance reset should be called again for processed shutdowns", resetCalled.get());
            }
            assertTrue(desiredBalanceAllocator.getProcessedNodeShutdowns().isEmpty());

            resetCalled.set(false);
            rerouteAndWait(service, clusterState, "random-reroute");
            assertFalse("desired balance reset should not be called", resetCalled.get());
            assertThat(desiredBalanceAllocator.getProcessedNodeShutdowns(), empty());
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    public void testNotReconcileEagerlyForEmptyRoutingTable() {
        final var threadPool = new TestThreadPool(getTestName());
        final var clusterService = ClusterServiceUtils.createClusterService(ClusterState.EMPTY_STATE, threadPool);
        final var clusterSettings = createBuiltInClusterSettings();
        final var shardsAllocator = createShardsAllocator();
        final var reconciliationTaskSubmitted = new AtomicBoolean();
        final var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            shardsAllocator,
            threadPool,
            clusterService,
            new DesiredBalanceComputer(clusterSettings, TimeProviderUtils.create(() -> 1L), shardsAllocator, TEST_ONLY_EXPLAINER) {
                @Override
                public DesiredBalance compute(
                    DesiredBalance previousDesiredBalance,
                    DesiredBalanceInput desiredBalanceInput,
                    Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
                    Predicate<DesiredBalanceInput> isFresh
                ) {
                    assertThat(previousDesiredBalance, sameInstance(DesiredBalance.BECOME_MASTER_INITIAL));
                    return new DesiredBalance(desiredBalanceInput.index(), Map.of());
                }
            },
            (clusterState, rerouteStrategy) -> null,
            EMPTY_NODE_ALLOCATION_STATS,
            DesiredBalanceMetrics.NOOP,
            AllocationBalancingRoundMetrics.NOOP
        ) {

            private ActionListener<Void> lastListener;

            @Override
            public void allocate(RoutingAllocation allocation, ActionListener<Void> listener) {
                lastListener = listener;
                super.allocate(allocation, listener);
            }

            @Override
            protected void reconcile(DesiredBalance desiredBalance, RoutingAllocation allocation) {
                fail("should not call reconcile");
            }

            @Override
            protected void submitReconcileTask(DesiredBalance desiredBalance) {
                assertThat(desiredBalance.lastConvergedIndex(), equalTo(0L));
                reconciliationTaskSubmitted.set(true);
                lastListener.onResponse(null);
            }
        };
        assertThat(desiredBalanceShardsAllocator.getDesiredBalance(), sameInstance(DesiredBalance.NOT_MASTER));
        try {
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            desiredBalanceShardsAllocator.allocate(
                new RoutingAllocation(
                    new AllocationDeciders(Collections.emptyList()),
                    clusterService.state(),
                    null,
                    null,
                    randomNonNegativeLong()
                ),
                future
            );
            safeGet(future);
            assertThat(desiredBalanceShardsAllocator.getStats().computationSubmitted(), equalTo(1L));
            assertThat(desiredBalanceShardsAllocator.getStats().computationExecuted(), equalTo(1L));
            assertThat(reconciliationTaskSubmitted.get(), is(true));
            assertThat(desiredBalanceShardsAllocator.getDesiredBalance().lastConvergedIndex(), equalTo(0L));
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

    private static void initialize(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        ExistingShardsAllocator.UnassignedAllocationHandler handler
    ) {
        handler.initialize(allocation.nodes().getLocalNodeId(), null, 0L, allocation.changes());
    }

    /**
     * A helper interface to simplify creating a GatewayAllocator in the tests by only requiring
     * an implementation for {@link org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator#allocateUnassigned}.
     */
    interface AllocateUnassignedHandler {
        void handle(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
        );
    }

    /**
     * Creates an implementation of GatewayAllocator that delegates its logic for allocating unassigned shards to the provided handler.
     */
    private static GatewayAllocator createGatewayAllocator(AllocateUnassignedHandler allocateUnassigned) {
        return new GatewayAllocator() {

            @Override
            public void beforeAllocation(RoutingAllocation allocation) {}

            @Override
            public void allocateUnassigned(
                ShardRouting shardRouting,
                RoutingAllocation allocation,
                UnassignedAllocationHandler unassignedAllocationHandler
            ) {
                allocateUnassigned.handle(shardRouting, allocation, unassignedAllocationHandler);
            }

            @Override
            public void afterPrimariesBeforeReplicas(RoutingAllocation allocation, Predicate<ShardRouting> isRelevantShardPredicate) {}
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
                allocation.routingNodes().setBalanceWeightStatsPerNode(Map.of());
            }

            @Override
            public ShardAllocationDecision explainShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };
    }

    private static void rerouteAndWait(AllocationService service, ClusterState clusterState, String reason) {
        safeAwait((ActionListener<Void> listener) -> service.reroute(clusterState, reason, listener));
    }
}
