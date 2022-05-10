/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class DesiredBalanceShardsAllocatorTests extends ESTestCase {

    private static final String TEST_INDEX = "test-index";

    public void testGatewayAllocatorPreemptsAllocation() {
        testAllocate(GatewayAllocatorBehaviour.ALLOCATE);
    }

    public void testGatewayAllocatorStillFetching() {
        testAllocate(GatewayAllocatorBehaviour.STILL_FETCHING);
    }

    public void testGatewayAllocatorDoesNothing() {
        testAllocate(GatewayAllocatorBehaviour.DO_NOTHING);
    }

    private enum GatewayAllocatorBehaviour {
        DO_NOTHING,
        STILL_FETCHING,
        ALLOCATE,
    }

    private static class TestRerouteService implements RerouteService {
        private boolean expectReroute;

        @Override
        public void reroute(String reason, Priority priority, ActionListener<ClusterState> listener) {
            assertTrue("unexpected reroute", expectReroute);
            expectReroute = false;
            listener.onResponse(null);
        }

        public void setExpectReroute() {
            assertFalse("already expecting a reroute", expectReroute);
            expectReroute = true;
        }

        public void assertNoPendingReroute() {
            assertFalse("no reroute occurred", expectReroute);
        }
    }

    private static class TestActionListener implements ActionListener<Void> {

        private volatile boolean wasCalled = false;

        @Override
        public void onResponse(Void unused) {
            wasCalled = true;
        }

        @Override
        public void onFailure(Exception e) {
            fail("should not be called in test");
        }
    }

    private static void testAllocate(GatewayAllocatorBehaviour gatewayAllocatorBehaviour) {
        final var rerouteService = new TestRerouteService();
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
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
        }, threadPool, () -> rerouteService);

        final var fetchingShardData = new AtomicBoolean(gatewayAllocatorBehaviour == GatewayAllocatorBehaviour.STILL_FETCHING);
        final var allocationService = new AllocationService(new AllocationDeciders(List.of()), new GatewayAllocator() {

            @Override
            public void beforeAllocation(RoutingAllocation allocation) {}

            @Override
            public void allocateUnassigned(
                ShardRouting shardRouting,
                RoutingAllocation allocation,
                UnassignedAllocationHandler unassignedAllocationHandler
            ) {
                switch (gatewayAllocatorBehaviour) {
                    case DO_NOTHING -> {}
                    case STILL_FETCHING -> {
                        if (fetchingShardData.get()) {
                            unassignedAllocationHandler.removeAndIgnore(
                                UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA,
                                allocation.changes()
                            );
                        }
                    }
                    case ALLOCATE -> unassignedAllocationHandler.initialize(
                        allocation.nodes().getLocalNodeId(),
                        null,
                        0L,
                        allocation.changes()
                    );
                }

            }

            @Override
            public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}
        }, desiredBalanceShardsAllocator, () -> ClusterInfo.EMPTY, () -> SnapshotShardSizeInfo.EMPTY);

        final var discoveryNode = createDiscoveryNode();
        final var indexMetadata = createIndex(TEST_INDEX);
        var clusterState = createClusterState(discoveryNode, indexMetadata);

        var listener = new TestActionListener();

        switch (gatewayAllocatorBehaviour) {
            case DO_NOTHING -> {
                // first reroute does nothing synchronously but triggers a desired balance computation which leads to a further reroute
                assertSame(clusterState, allocationService.reroute(clusterState, "test", listener));
                rerouteService.setExpectReroute();
                deterministicTaskQueue.runAllTasks();
                rerouteService.assertNoPendingReroute();
                final var shardRouting = clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
                assertFalse(shardRouting.assignedToNode());
                assertThat(shardRouting.unassignedInfo().getLastAllocationStatus(), equalTo(UnassignedInfo.AllocationStatus.NO_ATTEMPT));
                assertTrue(listener.wasCalled);
            }
            case STILL_FETCHING -> {
                // first reroute will allocate nothing if the gateway allocator is still in charge
                clusterState = allocationService.reroute(clusterState, "test", listener);
                rerouteService.setExpectReroute();
                assertTrue(deterministicTaskQueue.hasRunnableTasks());
                deterministicTaskQueue.runAllTasks();
                rerouteService.assertNoPendingReroute();
                final var shardRouting = clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
                assertFalse(shardRouting.assignedToNode());
                assertThat(
                    shardRouting.unassignedInfo().getLastAllocationStatus(),
                    equalTo(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA)
                );
                fetchingShardData.set(false);
                assertTrue(listener.wasCalled);
            }
            case ALLOCATE -> {
                // first reroute will allocate according to the gateway allocator
                clusterState = allocationService.reroute(clusterState, "test", listener);
                rerouteService.setExpectReroute();
                assertTrue(deterministicTaskQueue.hasRunnableTasks());
                deterministicTaskQueue.runAllTasks();
                rerouteService.assertNoPendingReroute();
                final var shardRouting = clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
                assertTrue(shardRouting.assignedToNode());
                assertTrue(listener.wasCalled);
            }
        }

        // next reroute picks up the new desired balance, reconciles the cluster state by allocating the shard, but needs no extra reroute
        clusterState = allocationService.reroute(clusterState, "test");
        deterministicTaskQueue.runAllTasks();
        rerouteService.assertNoPendingReroute();
        assertTrue(clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard().assignedToNode());

        // another reroute does nothing
        assertSame(clusterState, allocationService.reroute(clusterState, "test"));
        deterministicTaskQueue.runAllTasks();
        rerouteService.assertNoPendingReroute();

        // when the shard is started further reroutes still do nothing
        clusterState = allocationService.applyStartedShards(
            clusterState,
            List.of(clusterState.routingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard())
        );
        assertSame(clusterState, allocationService.reroute(clusterState, "test"));
        deterministicTaskQueue.runAllTasks();
        rerouteService.assertNoPendingReroute();
    }

    public void testCallListenersOnlyAfterProducingFreshInput() {

        var threadPool = new TestThreadPool(getTestName());

        var secondInputSubmitted = new CountDownLatch(1);
        var computationStarted = new CountDownLatch(1);
        var listenersCalled = new AtomicInteger(0);
        var reroutesCalled = new AtomicInteger(0);

        RerouteService rerouteService = (r, p, l) -> {
            reroutesCalled.incrementAndGet();
            l.onResponse(null);
        };

        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {

                try {
                    computationStarted.countDown();
                    assertTrue("Should have submitted the second input in time", secondInputSubmitted.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError("Should have submitted the second input");
                }

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
        }, threadPool, () -> rerouteService);

        var discoveryNode = createDiscoveryNode();
        var index1 = createIndex(UUIDs.randomBase64UUID());
        var index2 = createIndex(UUIDs.randomBase64UUID());

        var listener = ActionListener.<Void>wrap(response -> {
            assertThat("Should execute listeners only after both reroutes are completed", reroutesCalled.get(), equalTo(2));
            listenersCalled.incrementAndGet();
        }, exception -> { throw new AssertionError("Should not fail in test"); });

        desiredBalanceShardsAllocator.allocate(createAllocationFrom(createClusterState(discoveryNode, index1)), listener);

        try {
            assertTrue("Should submit second computation when first one has started", computationStarted.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new AssertionError("Should have submitted the second input");
        }

        desiredBalanceShardsAllocator.allocate(createAllocationFrom(createClusterState(discoveryNode, index2)), listener);

        secondInputSubmitted.countDown();

        terminate(threadPool);
        assertThat(listenersCalled.get(), equalTo(2));
    }

    public void testFailListenersOnNoLongerMasterException() {

        var threadPool = new TestThreadPool(getTestName());

        var listenersCalled = new AtomicInteger(0);

        RerouteService rerouteService = (r, p, l) -> l.onFailure(new NotMasterException("no longer master"));

        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(new ShardsAllocator() {
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
        }, threadPool, () -> rerouteService);

        var discoveryNode = createDiscoveryNode();
        var index = createIndex(UUIDs.randomBase64UUID());

        var listener = ActionListener.<Void>wrap(
            response -> { throw new AssertionError("Should not complete in test"); },
            failure -> listenersCalled.incrementAndGet()
        );
        desiredBalanceShardsAllocator.allocate(createAllocationFrom(createClusterState(discoveryNode, index)), listener);

        terminate(threadPool);
        assertThat(listenersCalled.get(), equalTo(1));
    }

    public void testConcurrency() throws Exception {

        var createdIndices = new CopyOnWriteArraySet<String>();
        var allocatedIndices = new CopyOnWriteArraySet<String>();
        var reroutedIndices = new CopyOnWriteArraySet<String>();

        var threadPool = new TestThreadPool(getTestName());
        RerouteService rerouteService = (r, p, l) -> threadPool.executor(ThreadPool.Names.CLUSTER_COORDINATION).submit(() -> {
            reroutedIndices.addAll(createdIndices);
            l.onResponse(null);
        });
        var allocator = new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {// TODO attempt to count up-to-date allocations
                final var dataNodeId = allocation.nodes().getDataNodes().values().iterator().next().getId();
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    String indexName = unassignedIterator.next().getIndexName();
                    allocatedIndices.add(indexName);
                    unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        };
        var desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(allocator, threadPool, () -> rerouteService);

        var discoveryNode = createDiscoveryNode();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).masterNodeId(discoveryNode.getId()))
            .build();

        var indexNameGenerator = new AtomicInteger();
        var listenersCalled = new AtomicInteger();

        var iterations = 1_000;
        for (int i = 0; i < iterations; i++) {
            boolean addNewIndex = i == 0 || randomInt(9) == 0;
            if (addNewIndex) {
                var indexName = "index-" + indexNameGenerator.incrementAndGet();
                var indexMetadata = createIndex(indexName);
                clusterState = ClusterState.builder(clusterState)
                    .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true))
                    .routingTable(RoutingTable.builder(clusterState.routingTable()).addAsNew(indexMetadata))
                    .build();
                createdIndices.add(indexName);
            }
            var indexName = "index-" + indexNameGenerator;
            desiredBalanceShardsAllocator.allocate(createAllocationFrom(clusterState), ActionListener.wrap(() -> {
                assertThat("Listener should be called after index is rerouted", reroutedIndices, hasItem(indexName));
                listenersCalled.incrementAndGet();
            }));
        }

        assertBusy(() -> {
            assertThat(createdIndices.size(), equalTo(indexNameGenerator.get()));
            assertThat(allocatedIndices.size(), equalTo(indexNameGenerator.get()));
            assertThat(reroutedIndices.size(), equalTo(indexNameGenerator.get()));
            assertThat(listenersCalled.get(), equalTo(iterations));
        });

        terminate(threadPool);
    }

    private static RoutingAllocation createAllocationFrom(ClusterState clusterState) {
        return new RoutingAllocation(
            new AllocationDeciders(List.of()),
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private static ClusterState createClusterState(DiscoveryNode discoveryNode, IndexMetadata indexMetadata) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).masterNodeId(discoveryNode.getId()))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();
    }

    private static DiscoveryNode createDiscoveryNode() {
        var transportAddress = buildNewFakeTransportAddress();
        return new DiscoveryNode(
            "node-0",
            "node-0",
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
