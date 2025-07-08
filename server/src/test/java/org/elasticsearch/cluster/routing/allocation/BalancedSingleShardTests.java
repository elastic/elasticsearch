/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for balancing a single shard.
 */
public class BalancedSingleShardTests extends ESAllocationTestCase {

    public void testRebalanceNonStartedShardNotAllowed() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state(
            "idx",
            randomBoolean(),
            randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.UNASSIGNED, ShardRoutingState.RELOCATING)
        );
        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();

        ShardRouting shard = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        MoveDecision rebalanceDecision = allocator.decideShardAllocation(
            shard,
            newRoutingAllocation(new AllocationDeciders(Collections.emptyList()), clusterState)
        ).getMoveDecision();
        assertSame(MoveDecision.NOT_TAKEN, rebalanceDecision);
    }

    public void testRebalanceNotAllowedDuringPendingAsyncFetch() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();

        ShardRouting shard = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        RoutingAllocation routingAllocation = newRoutingAllocation(new AllocationDeciders(Collections.emptyList()), clusterState);
        routingAllocation.setHasPendingAsyncFetch();
        MoveDecision rebalanceDecision = allocator.decideShardAllocation(shard, routingAllocation).getMoveDecision();
        assertNotNull(rebalanceDecision.getClusterRebalanceDecision());
        assertEquals(AllocationDecision.AWAITING_INFO, rebalanceDecision.getAllocationDecision());
        assertThat(rebalanceDecision.getExplanation(), equalTo(Explanations.Rebalance.AWAITING_INFO));
        assertEquals(clusterState.nodes().getSize() - 1, rebalanceDecision.getNodeDecisions().size());
        assertNull(rebalanceDecision.getTargetNode());

        assertAssignedNodeRemainsSame(allocator, routingAllocation, shard);
    }

    public void testRebalancingNotAllowedDueToCanRebalance() {
        final Decision canRebalanceDecision = randomFrom(Decision.NO, Decision.THROTTLE);
        AllocationDecider noRebalanceDecider = new AllocationDecider() {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return allocation.decision(canRebalanceDecision, "TEST", "foobar");
            }
        };
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();

        ShardRouting shard = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        RoutingAllocation routingAllocation = newRoutingAllocation(
            new AllocationDeciders(Collections.singleton(noRebalanceDecider)),
            clusterState
        );
        MoveDecision rebalanceDecision = allocator.decideShardAllocation(shard, routingAllocation).getMoveDecision();
        assertEquals(canRebalanceDecision.type(), rebalanceDecision.getClusterRebalanceDecision().type());
        assertEquals(AllocationDecision.fromDecisionType(canRebalanceDecision.type()), rebalanceDecision.getAllocationDecision());
        assertThat(
            rebalanceDecision.getExplanation(),
            containsString(
                canRebalanceDecision.type() == Type.THROTTLE
                    ? Explanations.Rebalance.CLUSTER_THROTTLE
                    : Explanations.Rebalance.CANNOT_REBALANCE_CANNOT_ALLOCATE
            )
        );
        assertNotNull(rebalanceDecision.getNodeDecisions());
        assertNull(rebalanceDecision.getTargetNode());
        assertEquals(1, rebalanceDecision.getClusterRebalanceDecision().getDecisions().size());
        for (Decision subDecision : rebalanceDecision.getClusterRebalanceDecision().getDecisions()) {
            assertEquals("foobar", subDecision.getExplanation());
        }

        assertAssignedNodeRemainsSame(allocator, routingAllocation, shard);
    }

    public void testRebalancePossible() {
        AllocationDecider canAllocateDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        Tuple<ClusterState, MoveDecision> rebalance = setupStateAndRebalance(canAllocateDecider, Settings.EMPTY, true);
        ClusterState clusterState = rebalance.v1();
        MoveDecision rebalanceDecision = rebalance.v2();
        assertEquals(Type.YES, rebalanceDecision.getClusterRebalanceDecision().type());
        assertNotNull(rebalanceDecision.getExplanation());
        assertEquals(clusterState.nodes().getSize() - 1, rebalanceDecision.getNodeDecisions().size());
    }

    public void testRebalancingNotAllowedDueToCanAllocate() {
        AllocationDecider canAllocateDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.NO;
            }
        };
        Tuple<ClusterState, MoveDecision> rebalance = setupStateAndRebalance(canAllocateDecider, Settings.EMPTY, false);
        ClusterState clusterState = rebalance.v1();
        MoveDecision rebalanceDecision = rebalance.v2();
        assertEquals(Type.YES, rebalanceDecision.getClusterRebalanceDecision().type());
        assertEquals(AllocationDecision.NO, rebalanceDecision.getAllocationDecision());
        assertThat(rebalanceDecision.getExplanation(), equalTo(Explanations.Rebalance.ALREADY_BALANCED));
        assertEquals(clusterState.nodes().getSize() - 1, rebalanceDecision.getNodeDecisions().size());
        assertNull(rebalanceDecision.getTargetNode());
        int prevRanking = 0;
        for (NodeAllocationResult result : rebalanceDecision.getNodeDecisions()) {
            assertThat(result.getWeightRanking(), greaterThanOrEqualTo(prevRanking));
            prevRanking = result.getWeightRanking();
        }
    }

    public void testDontBalanceShardWhenThresholdNotMet() {
        AllocationDecider canAllocateDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        // ridiculously high threshold setting so we won't rebalance
        Settings balancerSettings = Settings.builder().put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 1000f).build();
        Tuple<ClusterState, MoveDecision> rebalance = setupStateAndRebalance(canAllocateDecider, balancerSettings, false);
        ClusterState clusterState = rebalance.v1();
        MoveDecision rebalanceDecision = rebalance.v2();
        assertEquals(Type.YES, rebalanceDecision.getClusterRebalanceDecision().type());
        assertEquals(AllocationDecision.NO, rebalanceDecision.getAllocationDecision());
        assertNotNull(rebalanceDecision.getExplanation());
        assertEquals(clusterState.nodes().getSize() - 1, rebalanceDecision.getNodeDecisions().size());
        assertNull(rebalanceDecision.getTargetNode());
        int prevRanking = 0;
        for (NodeAllocationResult result : rebalanceDecision.getNodeDecisions()) {
            assertThat(result.getWeightRanking(), greaterThanOrEqualTo(prevRanking));
            prevRanking = result.getWeightRanking();
        }
    }

    public void testSingleShardBalanceProducesSameResultsAsBalanceStep() {
        final String[] indices = { "idx1", "idx2" };
        // Create a cluster state with 2 indices, each with 1 started primary shard, and only
        // one node initially so that all primary shards get allocated to the same node. We are only
        // using 2 indices (i.e. 2 total primary shards) because if we have any more than 2 started shards
        // in the routing table, then we have no guarantees about the order in which the 3 or more shards
        // are selected to be rebalanced to the new node, and hence the node to which they are rebalanced
        // is not deterministic. Using only two shards guarantees that only one of those two shards will
        // be rebalanced, and so we pick the one that was chosen to be rebalanced and execute the single-shard
        // rebalance step on it to make sure it gets assigned to the same node.
        ClusterState clusterState = ClusterStateCreationUtils.state(1, indices, 1);

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));

        // add new nodes so one of the primaries can be rebalanced
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
        int numAddedNodes = randomIntBetween(1, 5);
        // randomly select a subset of the newly added nodes to set filter allocation on (but not all)
        int excludeNodesSize = randomIntBetween(0, numAddedNodes - 1);
        final Set<String> excludeNodes = new HashSet<>();
        for (int i = 0; i < numAddedNodes; i++) {
            DiscoveryNode discoveryNode = newNode(randomAlphaOfLength(7));
            nodesBuilder.add(discoveryNode);
            if (i < excludeNodesSize) {
                excludeNodes.add(discoveryNode.getId());
            }
        }
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();

        AllocationDecider allocationDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                if (excludeNodes.contains(node.nodeId())) {
                    return Decision.NO;
                }
                return Decision.YES;
            }
        };
        AllocationDecider rebalanceDecider = new AllocationDecider() {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        List<AllocationDecider> allocationDeciders = Arrays.asList(rebalanceDecider, allocationDecider);
        RoutingAllocation routingAllocation = newRoutingAllocation(new AllocationDeciders(allocationDeciders), clusterState);
        // allocate and get the node that is now relocating
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        allocator.allocate(routingAllocation);
        ShardRouting shardToRebalance = null;
        for (RoutingNode routingNode : routingAllocation.routingNodes()) {
            List<ShardRouting> relocatingShards = routingNode.shardsWithState(ShardRoutingState.RELOCATING).toList();
            if (relocatingShards.size() > 0) {
                shardToRebalance = randomFrom(relocatingShards);
                break;
            }
        }

        routingAllocation = newRoutingAllocation(new AllocationDeciders(allocationDeciders), clusterState);
        routingAllocation.debugDecision(true);
        ShardRouting shard = clusterState.getRoutingNodes().activePrimary(shardToRebalance.shardId());
        MoveDecision rebalanceDecision = allocator.decideShardAllocation(shard, routingAllocation).getMoveDecision();
        assertEquals(shardToRebalance.relocatingNodeId(), rebalanceDecision.getTargetNode().getId());
        // make sure all excluded nodes returned a NO decision
        for (NodeAllocationResult nodeResult : rebalanceDecision.getNodeDecisions()) {
            if (excludeNodes.contains(nodeResult.getNode().getId())) {
                assertEquals(Type.NO, nodeResult.getCanAllocateDecision().type());
            }
        }
    }

    public void testNodeDecisionsRanking() {
        // only one shard, so moving it will not create a better balance anywhere, so all node decisions should
        // return the same ranking as the current node
        ClusterState clusterState = ClusterStateCreationUtils.state(randomIntBetween(1, 10), new String[] { "idx" }, 1);
        ShardRouting shardToRebalance = clusterState.routingTable().index("idx").shardsWithState(ShardRoutingState.STARTED).get(0);
        MoveDecision decision = executeRebalanceFor(shardToRebalance, clusterState, emptySet());
        int currentRanking = decision.getCurrentNodeRanking();
        assertEquals(1, currentRanking);
        for (NodeAllocationResult result : decision.getNodeDecisions()) {
            assertEquals(1, result.getWeightRanking());
        }

        // start off with one node and several shards assigned to that node, then add a few nodes to the cluster,
        // each of these new nodes should have a better ranking than the current, given a low enough threshold
        clusterState = ClusterStateCreationUtils.state(1, new String[] { "idx" }, randomIntBetween(2, 10));
        shardToRebalance = clusterState.routingTable().index("idx").shardsWithState(ShardRoutingState.STARTED).get(0);
        clusterState = addNodesToClusterState(clusterState, randomIntBetween(1, 10));
        decision = executeRebalanceFor(shardToRebalance, clusterState, emptySet());
        for (NodeAllocationResult result : decision.getNodeDecisions()) {
            assertThat(result.getWeightRanking(), lessThan(decision.getCurrentNodeRanking()));
        }

        // start off with 3 nodes and 7 shards, so that one of the 3 nodes will have 3 shards assigned, the remaining 2
        // nodes will have 2 shard each. then, add another node. pick a shard on one of the nodes that has only 2 shard
        // to rebalance. the new node should have the best ranking (because it has no shards), followed by the node currently
        // holding the shard as well as the other node with only 2 shards (they should have the same ranking), followed by the
        // node with 3 shards which will have the lowest ranking.
        clusterState = ClusterStateCreationUtils.state(3, new String[] { "idx" }, 7);
        shardToRebalance = null;
        Set<String> nodesWithTwoShards = new HashSet<>();
        String nodeWithThreeShards = null;
        for (RoutingNode node : clusterState.getRoutingNodes()) {
            if (node.numberOfShardsWithState(ShardRoutingState.STARTED) == 2) {
                nodesWithTwoShards.add(node.nodeId());
                if (shardToRebalance == null) {
                    shardToRebalance = node.shardsWithState(ShardRoutingState.STARTED).findFirst().get();
                }
            } else {
                assertEquals(3, node.numberOfShardsWithState(ShardRoutingState.STARTED));
                assertNull(nodeWithThreeShards); // should only have one of these
                nodeWithThreeShards = node.nodeId();
            }
        }
        clusterState = addNodesToClusterState(clusterState, 1);
        decision = executeRebalanceFor(shardToRebalance, clusterState, emptySet());
        for (NodeAllocationResult result : decision.getNodeDecisions()) {
            if (result.getWeightRanking() < decision.getCurrentNodeRanking()) {
                // highest ranked node should not be any of the initial nodes
                assertFalse(nodesWithTwoShards.contains(result.getNode().getId()));
                assertNotEquals(nodeWithThreeShards, result.getNode().getId());
            } else if (result.getWeightRanking() > decision.getCurrentNodeRanking()) {
                // worst ranked should be the node with two shards
                assertEquals(nodeWithThreeShards, result.getNode().getId());
            } else {
                assertTrue(nodesWithTwoShards.contains(result.getNode().getId()));
            }
        }
    }

    private MoveDecision executeRebalanceFor(
        final ShardRouting shardRouting,
        final ClusterState clusterState,
        final Set<String> noDecisionNodes
    ) {
        AllocationDecider allocationDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                if (noDecisionNodes.contains(node.nodeId())) {
                    return Decision.NO;
                }
                return Decision.YES;
            }
        };
        AllocationDecider rebalanceDecider = new AllocationDecider() {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        RoutingAllocation routingAllocation = newRoutingAllocation(
            new AllocationDeciders(Arrays.asList(allocationDecider, rebalanceDecider)),
            clusterState
        );

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();

        return allocator.decideShardAllocation(shardRouting, routingAllocation).getMoveDecision();
    }

    private ClusterState addNodesToClusterState(ClusterState clusterState, int numNodesToAdd) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
        for (int i = 0; i < numNodesToAdd; i++) {
            DiscoveryNode discoveryNode = newNode(randomAlphaOfLength(7));
            nodesBuilder.add(discoveryNode);
        }
        return ClusterState.builder(clusterState).nodes(nodesBuilder).build();
    }

    private Tuple<ClusterState, MoveDecision> setupStateAndRebalance(
        AllocationDecider allocationDecider,
        Settings balancerSettings,
        boolean rebalanceExpected
    ) {
        AllocationDecider rebalanceDecider = new AllocationDecider() {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        List<AllocationDecider> allocationDeciders = Arrays.asList(rebalanceDecider, allocationDecider);
        final int numShards = randomIntBetween(8, 13);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(balancerSettings);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", 2, numShards);
        // add a new node so shards can be rebalanced there
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
        nodesBuilder.add(newNode(randomAlphaOfLength(7)));
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();

        ShardRouting shard = clusterState.routingTable(projectId).index("idx").shard(0).primaryShard();
        RoutingAllocation routingAllocation = newRoutingAllocation(new AllocationDeciders(allocationDeciders), clusterState);
        MoveDecision rebalanceDecision = allocator.decideShardAllocation(shard, routingAllocation).getMoveDecision();

        if (rebalanceExpected == false) {
            assertAssignedNodeRemainsSame(allocator, routingAllocation, shard);
        }

        return Tuple.tuple(clusterState, rebalanceDecision);
    }

    private RoutingAllocation newRoutingAllocation(AllocationDeciders deciders, ClusterState state) {
        RoutingAllocation allocation = new RoutingAllocation(
            deciders,
            state.mutableRoutingNodes(),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        return allocation;
    }

    private void assertAssignedNodeRemainsSame(
        BalancedShardsAllocator allocator,
        RoutingAllocation routingAllocation,
        ShardRouting originalRouting
    ) {
        allocator.allocate(routingAllocation);
        RoutingNodes routingNodes = routingAllocation.routingNodes();
        // make sure the previous node id is the same as the current one after rerouting
        assertEquals(originalRouting.currentNodeId(), routingNodes.activePrimary(originalRouting.shardId()).currentNodeId());
    }
}
