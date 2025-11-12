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
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.mockito.invocation.InvocationOnMock;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class BalancedShardsAllocatorInvalidWeightsTests extends ESTestCase {

    public void testBalanceIsSkippedWhenInvalidWeightsAreEncounteredDuringSorting() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertionsAndRemoveLogRateLimiting()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                BalancerSettings.DEFAULT,
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final ClusterState clusterState = makeUnbalanced(
                ClusterStateCreationUtils.state(3, new String[] { "one", "two", "three" }, 1),
                "node_0"
            );
            final RoutingAllocation allocation = new RoutingAllocation(
                new AllocationDeciders(List.of()),
                clusterState.getRoutingNodes().mutableCopy(),
                clusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );
            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);
            assertInvalidWeightsMessageLogged(() -> allocator.allocate(allocation));

            // There should have been no changes
            assertFalse(allocation.routingNodesChanged());
        }
    }

    public void testNodesBalancingIsAbortedWhenWeFirstSeeAnInvalidWeight() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertionsAndRemoveLogRateLimiting()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                BalancerSettings.DEFAULT,
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final AllocationDecider allocationDecider = spy(AllocationDecider.class);
            final ClusterState clusterState = makeUnbalanced(
                ClusterStateCreationUtils.state(3, new String[] { "one", "two", "three" }, randomIntBetween(1, 2)),
                "node_0"
            );
            final RoutingAllocation allocation = new RoutingAllocation(
                new AllocationDeciders(List.of(allocationDecider)),
                clusterState.getRoutingNodes().mutableCopy(),
                clusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );

            doAnswer(iom -> {
                if (allocation.routingNodes().getRelocatingShardCount() > 0) {
                    return randomFrom(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN);
                }
                return iom.callRealMethod();
            }).when(balancingWeightsFactory.getWeightFunction()).calculateNodeWeightWithIndex(any(), any(), any());
            assertInvalidWeightsMessageLogged(() -> allocator.allocate(allocation));
            // We should have aborted balancing after that first move
            assertEquals(1, allocation.routingNodes().getRelocatingShardCount());
        }
    }

    public void testMoveShardsIsSkippedWhenInvalidWeightsAreEncounteredDuringSorting() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertionsAndRemoveLogRateLimiting()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                BalancerSettings.DEFAULT,
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final AllocationDecider allocationDecider = spy(AllocationDecider.class);
            final ClusterState clusterState = ClusterStateCreationUtils.state(3, new String[] { "one", "two", "three" }, 1);
            // Allocator will try and move shards off of node_2
            when(allocationDecider.canRemain(any(), any(), any(), any())).thenAnswer(iom -> {
                RoutingNode routingNode = iom.getArgument(2);
                if (routingNode.node().getId().equals("node_2")) {
                    return Decision.NO;
                }
                return Decision.YES;
            });
            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);
            final RoutingAllocation allocation = new RoutingAllocation(
                new AllocationDeciders(List.of(allocationDecider)),
                clusterState.getRoutingNodes().mutableCopy(),
                clusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );
            assertInvalidWeightsMessageLogged(() -> allocator.allocate(allocation));
            // No shards should have moved
            assertFalse(allocation.routingNodesChanged());
        }
    }

    public void testUnallocatedShardsAreStillAllocatedWhenSomeOrAllNodesHaveInvalidWeights() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertionsAndRemoveLogRateLimiting()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                BalancerSettings.DEFAULT,
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final ClusterState clusterState = failAllShards(ClusterStateCreationUtils.state(3, new String[] { "one", "two", "three" }, 1));

            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);
            final RoutingAllocation allocation = new RoutingAllocation(
                new AllocationDeciders(List.of()),
                clusterState.getRoutingNodes().mutableCopy(),
                clusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );
            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);
            assertInvalidWeightsMessageLogged(() -> allocator.allocate(allocation));
            // No shards should be left unassigned
            assertFalse(allocation.routingNodes().hasUnassignedShards());
        }
    }

    private void assertInvalidWeightsMessageLogged(Runnable runnable) {
        assertThatLogger(
            runnable,
            BalancedShardsAllocator.class,
            new MockLog.SeenEventExpectation(
                "invalid weights returned",
                BalancedShardsAllocator.class.getName(),
                Level.ERROR,
                "Weight function returned invalid weight node=*"
            )
        );
    }

    /**
     * Fail all the shards, this should make them all unassigned
     */
    private ClusterState failAllShards(ClusterState clusterState) {
        RoutingNodes routingNodes = clusterState.getRoutingNodes().mutableCopy();
        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                routingNodes.failShard(
                    shardRouting,
                    new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "test"),
                    RoutingChangesObserver.NOOP
                );
            }
        }
        return ClusterState.builder(clusterState)
            .routingTable(clusterState.globalRoutingTable().rebuild(routingNodes, clusterState.metadata()))
            .build();
    }

    /**
     * Move all the shards to the specified node, the balancer should make some movement on a routing table
     * in this state
     */
    private ClusterState makeUnbalanced(ClusterState clusterState, String nodeId) {
        final var routingNodes = clusterState.getRoutingNodes().mutableCopy();
        final var targetNode = Objects.requireNonNull(clusterState.nodes().get(nodeId), "Unknown node specified: " + nodeId);
        for (RoutingNode routingNode : routingNodes) {
            if (targetNode.equals(routingNode.node())) {
                continue;
            }
            for (ShardRouting shardRouting : routingNode.copyShards()) {
                Tuple<ShardRouting, ShardRouting> test = routingNodes.relocateShard(
                    shardRouting,
                    targetNode.getId(),
                    0L,
                    "test",
                    RoutingChangesObserver.NOOP
                );
                routingNodes.startShard(test.v2(), RoutingChangesObserver.NOOP, 0L);
            }
        }
        return ClusterState.builder(clusterState)
            .routingTable(clusterState.globalRoutingTable().rebuild(routingNodes, clusterState.metadata()))
            .build();
    }

    private static class InvalidWeightsBalancingWeightsFactory implements BalancingWeightsFactory {

        private final WeightFunction weightFunction;
        private Set<DiscoveryNode> nodesToReturnInvalidWeightsFor = Set.of();

        InvalidWeightsBalancingWeightsFactory() {
            weightFunction = spy(new WeightFunction(1.0f, 1.0f, 1.0f, 1.0f));
            doAnswer(this::calculateWeightWithIndex).when(weightFunction).calculateNodeWeightWithIndex(any(), any(), any());
        }

        private Object calculateWeightWithIndex(InvocationOnMock invocationOnMock) throws Throwable {
            BalancedShardsAllocator.ModelNode modelNode = invocationOnMock.getArgument(1);

            if (nodesToReturnInvalidWeightsFor.contains(modelNode.getRoutingNode().node())) {
                return randomFrom(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN);
            }
            return invocationOnMock.callRealMethod();
        }

        /**
         * Make one or more of the nodes return invalid weights
         */
        public void returnInvalidWeightsForRandomNodes(ClusterState clusterState) {
            final var allNodes = clusterState.nodes().getAllNodes();
            nodesToReturnInvalidWeightsFor = Set.copyOf(randomSubsetOf(randomIntBetween(1, allNodes.size()), allNodes));
        }

        public WeightFunction getWeightFunction() {
            return weightFunction;
        }

        @Override
        public BalancingWeights create() {
            return new InvalidWeightsBalancingWeights();
        }

        private class InvalidWeightsBalancingWeights implements BalancingWeights {

            @Override
            public WeightFunction weightFunctionForShard(ShardRouting shard) {
                return weightFunction;
            }

            @Override
            public WeightFunction weightFunctionForNode(RoutingNode node) {
                return weightFunction;
            }

            @Override
            public NodeSorters createNodeSorters(
                BalancedShardsAllocator.ModelNode[] modelNodes,
                BalancedShardsAllocator.Balancer balancer
            ) {
                final var nodeSorters = List.of(new BalancedShardsAllocator.NodeSorter(modelNodes, weightFunction, balancer));
                return new NodeSorters() {

                    @Override
                    public Iterator<BalancedShardsAllocator.NodeSorter> iterator() {
                        return nodeSorters.iterator();
                    }

                    @Override
                    public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
                        return nodeSorters.getFirst();
                    }
                };
            }

            @Override
            public boolean diskUsageIgnored() {
                return true;
            }
        }
    }
}
