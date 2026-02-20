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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.mockito.invocation.InvocationOnMock;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.allocator.PreDesiredBalanceShardsAllocator.disableInvalidWeightsAssertions;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * THESE TESTS SHOULD NOT HAVE FURTHER DEVELOPMENT!
 *
 * Parallels the {@link BalancedShardsAllocatorInvalidWeightsTests} tests except using the {@link PreDesiredBalanceShardsAllocator}.
 */
public class PreDesiredBalanceShardsAllocatorInvalidWeightsTests extends ESTestCase {

    public void testBalanceWillBePerformedForAnyNodesReturningValidWeights() {
        try (var ignored = disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new PreDesiredBalanceShardsAllocator(
                BalancedShardsAllocatorInvalidWeightsTests.createBalancerSettings(),
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final int numberOfNodes = randomIntBetween(3, 5);
            final var originalState = ClusterStateCreationUtils.state(numberOfNodes, new String[] { "one", "two", "three" }, 1);
            final var nodeToPutAllShardsOn = randomFrom(originalState.nodes().getAllNodes());
            final var unbalancedClusterState = BalancedShardsAllocatorInvalidWeightsTests.moveAllShardsToNode(
                originalState,
                nodeToPutAllShardsOn
            );
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
                assertFalse(allocation.routingNodesChanged());
            } else if (nodeIdsReturningInvalidWeights.size() == numberOfNodes - 1) {
                assertFalse(allocation.routingNodesChanged());
            } else {
                assertTrue(allocation.routingNodesChanged());
            }
        }
    }

    public void testShardThatNeedsMovingWillAlwaysBeMoved() {
        try (var ignored = disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new PreDesiredBalanceShardsAllocator(
                BalancedShardsAllocatorInvalidWeightsTests.createBalancerSettings(),
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
            assertEquals(1, allocation.routingNodes().getRelocatingShardCount());
            final var relocatingShardsOnNominatedNode = allocation.routingNodes()
                .node(nodeToMoveShardOff.getId())
                .shardsWithState(ShardRoutingState.RELOCATING)
                .toList();
            assertEquals(1, relocatingShardsOnNominatedNode.size());
            final var relocatingShard = relocatingShardsOnNominatedNode.getFirst();
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
        try (var ignored = disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new PreDesiredBalanceShardsAllocator(
                BalancedShardsAllocatorInvalidWeightsTests.createBalancerSettings(),
                WriteLoadForecaster.DEFAULT,
                balancingWeightsFactory
            );

            final ClusterState clusterState = BalancedShardsAllocatorInvalidWeightsTests.failAllShards(
                ClusterStateCreationUtils.state(3, new String[] { "one", "two", "three" }, 1)
            );

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
            assertFalse(allocation.routingNodes().hasUnassignedShards());
        }
    }

    public void testExplainRebalanceExcludesNodesReturningInvalidWeights() {
        try (var ignored = disableInvalidWeightsAssertions()) {
            final var balancingWeightsFactory = new InvalidWeightsBalancingWeightsFactory();
            final var allocator = new PreDesiredBalanceShardsAllocator(
                BalancedShardsAllocatorInvalidWeightsTests.createBalancerSettings(),
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

    private void assertInvalidWeightsMessageIsLogged(Runnable runnable) {
        assertThatLogger(
            runnable,
            PreDesiredBalanceShardsAllocator.class,
            new MockLog.SeenEventExpectation(
                "invalid weights returned",
                PreDesiredBalanceShardsAllocator.class.getName(),
                Level.ERROR,
                "Weight function returned invalid weight node=*"
            )
        );
    }

    private static class InvalidWeightsBalancingWeightsFactory
        implements
            PreDesiredBalanceShardsAllocator.PreDesiredBalancingWeightsFactory {

        private final PreDesiredBalanceShardsAllocator.LocalWeightFunction weightFunction;
        private Set<DiscoveryNode> nodesToReturnInvalidWeightsFor = Set.of();

        InvalidWeightsBalancingWeightsFactory() {
            weightFunction = spy(new PreDesiredBalanceShardsAllocator.LocalWeightFunction(1.0f, 1.0f, 1.0f, 1.0f));
            doAnswer(this::calculateWeightWithIndex).when(weightFunction).calculateNodeWeightWithIndex(any(), any(), any());
        }

        private Object calculateWeightWithIndex(InvocationOnMock invocationOnMock) throws Throwable {
            PreDesiredBalanceShardsAllocator.ModelNode modelNode = invocationOnMock.getArgument(1);

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
        public PreDesiredBalanceShardsAllocator.PreDesiredBalancingWeights create() {
            return new InvalidWeightsBalancingWeights();
        }

        class InvalidWeightsBalancingWeights implements PreDesiredBalanceShardsAllocator.PreDesiredBalancingWeights {

            @Override
            public PreDesiredBalanceShardsAllocator.LocalWeightFunction weightFunctionForShard(ShardRouting shard) {
                return weightFunction;
            }

            @Override
            public PreDesiredBalanceShardsAllocator.LocalWeightFunction weightFunctionForNode(RoutingNode node) {
                return weightFunction;
            }

            @Override
            public PreDesiredBalanceShardsAllocator.PreDesiredNodeSorters createNodeSorters(
                PreDesiredBalanceShardsAllocator.ModelNode[] modelNodes,
                PreDesiredBalanceShardsAllocator.Balancer balancer
            ) {
                final var nodeSorters = List.of(
                    new PreDesiredBalanceShardsAllocator.NodeSorter(
                        modelNodes,
                        weightFunction,
                        balancer,
                        BalancerSettings.DEFAULT.getThreshold()
                    )
                );
                return new PreDesiredBalanceShardsAllocator.PreDesiredNodeSorters() {

                    @Override
                    public Iterator<PreDesiredBalanceShardsAllocator.NodeSorter> iterator() {
                        return nodeSorters.iterator();
                    }

                    @Override
                    public PreDesiredBalanceShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
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
