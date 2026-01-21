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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.mockito.invocation.InvocationOnMock;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Tests for how the balanced shards allocator will behave when the weight function
 * starts returning invalid (non-finite) values. This shouldn't happen in the absence
 * of bugs.
 */
public class BalancedShardsAllocatorInvalidWeightsTests extends ESTestCase {

    public void testBalanceWillBePerformedForAnyNodesReturningValidWeights() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                createBalancerSettings(),
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final int numberOfNodes = randomIntBetween(3, 5);
            final var originalState = ClusterStateCreationUtils.state(numberOfNodes, new String[] { "one", "two", "three" }, 1);
            final var nodeToPutAllShardsOn = randomFrom(originalState.nodes().getAllNodes());
            final var unbalancedClusterState = moveAllShardsToNode(originalState, nodeToPutAllShardsOn);
            final var allocation = new RoutingAllocation(
                new AllocationDeciders(List.of()),
                unbalancedClusterState.getRoutingNodes().mutableCopy(),
                unbalancedClusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );
            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(unbalancedClusterState);
            assertInvalidWeightsMessageIsLogged(() -> allocator.allocate(allocation));

            final var nodeIdsReturningInvalidWeights = balancingWeightsFactory.nodeIdsReturningInvalidWeights();
            if (nodeIdsReturningInvalidWeights.contains(nodeToPutAllShardsOn.getId())) {
                // The node with all the shards is returning invalid weights, we can't balance
                assertFalse(allocation.routingNodesChanged());
            } else if (nodeIdsReturningInvalidWeights.size() == numberOfNodes - 1) {
                // All the other nodes are returning invalid weights, we can't balance
                assertFalse(allocation.routingNodesChanged());
            } else {
                // Some balancing should have been done
                assertTrue(allocation.routingNodesChanged());
            }
        }
    }

    public void testShardThatNeedsMovingWillAlwaysBeMoved() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                createBalancerSettings(),
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final int numberOfNodes = randomIntBetween(3, 5);
            final var clusterState = ClusterStateCreationUtils.state(randomIdentifier(), numberOfNodes, numberOfNodes);
            final var negativeDecision = randomFrom(Decision.NO, Decision.NOT_PREFERRED);
            final var nodeToMoveShardOff = randomFrom(
                clusterState.nodes()
                    .getAllNodes()
                    .stream()
                    .filter(node -> clusterState.getRoutingNodes().node(node.getId()).isEmpty() == false)
                    .toList()
            );
            final var allocationDecider = new AllocationDecider() {
                @Override
                public Decision canRemain(
                    IndexMetadata indexMetadata,
                    ShardRouting shardRouting,
                    RoutingNode node,
                    RoutingAllocation allocation
                ) {
                    if (nodeToMoveShardOff.equals(node.node())) {
                        return negativeDecision;
                    }
                    return Decision.YES;
                }
            };

            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);
            final var allocation = new RoutingAllocation(
                new AllocationDeciders(List.of(allocationDecider)),
                clusterState.getRoutingNodes().mutableCopy(),
                clusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );
            assertInvalidWeightsMessageIsLogged(() -> allocator.allocate(allocation));
            // A shard on the nominated node should have been moved (we stop after 1 move by default)
            assertEquals(1, allocation.routingNodes().getRelocatingShardCount());
            final var relocatingShardsOnNominatedNode = allocation.routingNodes()
                .node(nodeToMoveShardOff.getId())
                .shardsWithState(ShardRoutingState.RELOCATING)
                .toList();
            assertEquals(1, relocatingShardsOnNominatedNode.size());
            final var relocatingShard = relocatingShardsOnNominatedNode.getFirst();
            // It should be moved to a node returning a valid weight, if there are any
            final boolean allOtherNodesAreReturningInvalidWeights = Sets.difference(
                balancingWeightsFactory.nodeIdsReturningInvalidWeights(),
                Set.of(nodeToMoveShardOff.getId())
            ).size() + 1 == numberOfNodes;
            assertTrue(
                balancingWeightsFactory.nodeIsReturningInvalidWeights(relocatingShard.relocatingNodeId()) == false
                    || allOtherNodesAreReturningInvalidWeights
            );
        }
    }

    public void testUnallocatedShardsAreStillAllocatedWhenOneOrMoreNodesReturnInvalidWeights() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                createBalancerSettings(),
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final ClusterState clusterState = failAllShards(ClusterStateCreationUtils.state(3, new String[] { "one", "two", "three" }, 1));

            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);
            final var allocation = new RoutingAllocation(
                new AllocationDeciders(List.of()),
                clusterState.getRoutingNodes().mutableCopy(),
                clusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );
            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);
            assertInvalidWeightsMessageIsLogged(() -> allocator.allocate(allocation));
            // No shards should be left unassigned
            assertFalse(allocation.routingNodes().hasUnassignedShards());
        }
    }

    public void testExplainRebalanceExcludesNodesReturningInvalidWeights() {
        try (var ignored = BalancedShardsAllocator.disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new BalancedShardsAllocator(
                createBalancerSettings(),
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final int numberOfNodes = randomIntBetween(3, 5);
            final var clusterState = ClusterStateCreationUtils.state(numberOfNodes, new String[] { "one", "two", "three" }, 1);
            balancingWeightsFactory.returnInvalidWeightsForRandomNodes(clusterState);

            final var allocation = new RoutingAllocation(
                new AllocationDeciders(List.of()),
                clusterState.getRoutingNodes().mutableCopy(),
                clusterState,
                ClusterInfo.EMPTY,
                null,
                System.nanoTime()
            );

            assertInvalidWeightsMessageIsLogged(() -> {
                final var shard = randomFrom(clusterState.routingTable(ProjectId.DEFAULT).allShards().collect(Collectors.toSet()));
                final var shardAllocationDecision = allocator.explainShardAllocation(shard, allocation);
                final boolean currentNodeReturningInvalidWeight = balancingWeightsFactory.nodeIsReturningInvalidWeights(
                    shard.currentNodeId()
                );
                final boolean allOtherNodesReturningInvalidWeights = Sets.difference(
                    balancingWeightsFactory.nodeIdsReturningInvalidWeights(),
                    Set.of(shard.currentNodeId())
                ).size() + 1 == numberOfNodes;
                // If the current node and/or all other nodes are returning invalid weights, we can't return a decision
                assertTrue(
                    "Decision: "
                        + shardAllocationDecision
                        + ", currentNodeId: "
                        + shard.currentNodeId()
                        + ", invalidNodes: "
                        + balancingWeightsFactory.nodeIdsReturningInvalidWeights()
                        + ", nodes: "
                        + numberOfNodes,
                    shardAllocationDecision.isDecisionTaken() || allOtherNodesReturningInvalidWeights || currentNodeReturningInvalidWeight
                );
                // If we did return results, they should all be from nodes returning valid weights
                if (shardAllocationDecision.isDecisionTaken()) {
                    assertTrue(
                        shardAllocationDecision.getMoveDecision()
                            .getNodeDecisions()
                            .stream()
                            .noneMatch(result -> balancingWeightsFactory.nodeIsReturningInvalidWeights(result.getNode().getId()))
                    );
                }
            });
        }
    }

    /**
     * Create balancer settings with invalid weight log rate limiting turned off
     */
    private BalancerSettings createBalancerSettings() {
        return new BalancerSettings(
            Settings.builder().put(BalancedShardsAllocator.INVALID_WEIGHTS_MINIMUM_LOG_INTERVAL.getKey(), TimeValue.ZERO).build()
        );
    }

    private void assertInvalidWeightsMessageIsLogged(Runnable runnable) {
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
        final var routingNodes = clusterState.getRoutingNodes().mutableCopy();
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
    private ClusterState moveAllShardsToNode(ClusterState clusterState, DiscoveryNode targetNode) {
        final var routingNodes = clusterState.getRoutingNodes().mutableCopy();
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

        public boolean nodeIsReturningInvalidWeights(String nodeId) {
            return nodesToReturnInvalidWeightsFor.stream().anyMatch(node -> node.getId().equals(nodeId));
        }

        public Set<String> nodeIdsReturningInvalidWeights() {
            return nodesToReturnInvalidWeightsFor.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
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
                final var nodeSorters = List.of(
                    new BalancedShardsAllocator.NodeSorter(modelNodes, weightFunction, balancer, BalancerSettings.DEFAULT.getThreshold())
                );
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
