/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.Balancer;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for balancing a single shard, see {@link Balancer#decideRebalance(ShardRouting)}.
 */
public class BalancedSingleShardTests extends ESAllocationTestCase {

    public void testRebalanceNonStartedShardNotAllowed() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(),
            randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.UNASSIGNED, ShardRoutingState.RELOCATING));
        ShardRouting shard = clusterState.routingTable().index("idx").shard(0).primaryShard();
        RebalanceDecision rebalanceDecision = allocator.decideRebalance(shard, newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, Collections.emptyList()), clusterState));
        assertSame(RebalanceDecision.NOT_TAKEN, rebalanceDecision);
    }

    public void testRebalanceNotAllowedDuringPendingAsyncFetch() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        ShardRouting shard = clusterState.routingTable().index("idx").shard(0).primaryShard();
        RoutingAllocation routingAllocation = newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, Collections.emptyList()), clusterState);
        routingAllocation.setHasPendingAsyncFetch();
        RebalanceDecision rebalanceDecision = allocator.decideRebalance(shard, routingAllocation);
        assertNotNull(rebalanceDecision.getCanRebalanceDecision());
        assertEquals(Type.NO, rebalanceDecision.getFinalDecisionType());
        assertThat(rebalanceDecision.getFinalExplanation(), startsWith("cannot rebalance due to in-flight shard store fetches"));
        assertNull(rebalanceDecision.getNodeDecisions());
        assertNull(rebalanceDecision.getAssignedNodeId());

        assertAssignedNodeRemainsSame(allocator, routingAllocation, shard);
    }

    public void testRebalancingNotAllowedDueToCanRebalance() {
        final Decision canRebalanceDecision = randomFrom(Decision.NO, Decision.THROTTLE);
        AllocationDecider noRebalanceDecider = new AllocationDecider(Settings.EMPTY) {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return allocation.decision(canRebalanceDecision, "TEST", "foobar");
            }
        };
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        ShardRouting shard = clusterState.routingTable().index("idx").shard(0).primaryShard();
        RoutingAllocation routingAllocation =  newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, Collections.singleton(noRebalanceDecider)), clusterState);
        RebalanceDecision rebalanceDecision = allocator.decideRebalance(shard, routingAllocation);
        assertEquals(canRebalanceDecision.type(), rebalanceDecision.getCanRebalanceDecision().type());
        assertEquals(canRebalanceDecision.type(), rebalanceDecision.getFinalDecisionType());
        assertEquals("rebalancing is not allowed", rebalanceDecision.getFinalExplanation());
        assertNotNull(rebalanceDecision.getNodeDecisions());
        assertNull(rebalanceDecision.getAssignedNodeId());
        assertEquals(1, rebalanceDecision.getCanRebalanceDecision().getDecisions().size());
        for (Decision subDecision : rebalanceDecision.getCanRebalanceDecision().getDecisions()) {
            assertEquals("foobar", ((Decision.Single) subDecision).getExplanation());
        }

        assertAssignedNodeRemainsSame(allocator, routingAllocation, shard);
    }

    public void testRebalancePossible() {
        AllocationDecider canAllocateDecider = new AllocationDecider(Settings.EMPTY) {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        Tuple<ClusterState, RebalanceDecision> rebalance = setupStateAndRebalance(canAllocateDecider, Settings.EMPTY, true);
        ClusterState clusterState = rebalance.v1();
        RebalanceDecision rebalanceDecision = rebalance.v2();
        assertEquals(Type.YES, rebalanceDecision.getCanRebalanceDecision().type());
        assertNotNull(rebalanceDecision.getFinalExplanation());
        assertEquals(clusterState.nodes().getSize() - 1, rebalanceDecision.getNodeDecisions().size());
    }

    public void testRebalancingNotAllowedDueToCanAllocate() {
        AllocationDecider canAllocateDecider = new AllocationDecider(Settings.EMPTY) {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.NO;
            }
        };
        Tuple<ClusterState, RebalanceDecision> rebalance = setupStateAndRebalance(canAllocateDecider, Settings.EMPTY, false);
        ClusterState clusterState = rebalance.v1();
        RebalanceDecision rebalanceDecision = rebalance.v2();
        assertEquals(Type.YES, rebalanceDecision.getCanRebalanceDecision().type());
        assertEquals(Type.NO, rebalanceDecision.getFinalDecisionType());
        assertThat(rebalanceDecision.getFinalExplanation(),
            startsWith("cannot rebalance shard, no other node exists that would form a more balanced"));
        assertEquals(clusterState.nodes().getSize() - 1, rebalanceDecision.getNodeDecisions().size());
        assertNull(rebalanceDecision.getAssignedNodeId());
    }

    public void testDontBalanceShardWhenThresholdNotMet() {
        AllocationDecider canAllocateDecider = new AllocationDecider(Settings.EMPTY) {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        // ridiculously high threshold setting so we won't rebalance
        Settings balancerSettings = Settings.builder().put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 1000f).build();
        Tuple<ClusterState, RebalanceDecision> rebalance = setupStateAndRebalance(canAllocateDecider, balancerSettings, false);
        ClusterState clusterState = rebalance.v1();
        RebalanceDecision rebalanceDecision = rebalance.v2();
        assertEquals(Type.YES, rebalanceDecision.getCanRebalanceDecision().type());
        assertEquals(Type.NO, rebalanceDecision.getFinalDecisionType());
        assertNotNull(rebalanceDecision.getFinalExplanation());
        assertEquals(clusterState.nodes().getSize() - 1, rebalanceDecision.getNodeDecisions().size());
        assertNull(rebalanceDecision.getAssignedNodeId());
    }

    public void testSingleShardBalanceProducesSameResultsAsBalanceStep() {
        final String[] indices = { "idx1", "idx2" };
        // Create a cluster state with 2 indices, each with 1 started primary shard, and only
        // one node initially so that all primary shards get allocated to the same node.  We are only
        // using 2 indices (i.e. 2 total primary shards) because if we have any more than 2 started shards
        // in the routing table, then we have no guarantees about the order in which the 3 or more shards
        // are selected to be rebalanced to the new node, and hence the node to which they are rebalanced
        // is not deterministic.  Using only two shards guarantees that only one of those two shards will
        // be rebalanced, and so we pick the one that was chosen to be rebalanced and execute the single-shard
        // rebalance step on it to make sure it gets assigned to the same node.
        ClusterState clusterState = ClusterStateCreationUtils.state(1, indices, 1);
        // add new nodes so one of the primaries can be rebalanced
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
        int numAddedNodes = randomIntBetween(1, 5);
        // randomly select a subset of the newly added nodes to set filter allocation on (but not all)
        int excludeNodesSize = randomIntBetween(0, numAddedNodes - 1);
        final Set<String> excludeNodes = new HashSet<>();
        for (int i = 0; i < numAddedNodes; i++) {
            DiscoveryNode discoveryNode = newNode(randomAsciiOfLength(7));
            nodesBuilder.add(discoveryNode);
            if (i < excludeNodesSize) {
                excludeNodes.add(discoveryNode.getId());
            }
        }
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();

        AllocationDecider allocationDecider = new AllocationDecider(Settings.EMPTY) {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                if (excludeNodes.contains(node.nodeId())) {
                    return Decision.NO;
                }
                return Decision.YES;
            }
        };
        AllocationDecider rebalanceDecider = new AllocationDecider(Settings.EMPTY) {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }
        };
        List<AllocationDecider> allocationDeciders = Arrays.asList(rebalanceDecider, allocationDecider);
        RoutingAllocation routingAllocation = newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, allocationDeciders), clusterState);
        // allocate and get the node that is now relocating
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        allocator.allocate(routingAllocation);
        ShardRouting shardToRebalance = null;
        for (RoutingNode routingNode : routingAllocation.routingNodes()) {
            List<ShardRouting> relocatingShards = routingNode.shardsWithState(ShardRoutingState.RELOCATING);
            if (relocatingShards.size() > 0) {
                shardToRebalance = randomFrom(relocatingShards);
                break;
            }
        }

        routingAllocation = newRoutingAllocation(new AllocationDeciders(Settings.EMPTY, allocationDeciders), clusterState);
        routingAllocation.debugDecision(true);
        ShardRouting shard = clusterState.getRoutingNodes().activePrimary(shardToRebalance.shardId());
        RebalanceDecision rebalanceDecision = allocator.decideRebalance(shard, routingAllocation);
        assertEquals(shardToRebalance.relocatingNodeId(), rebalanceDecision.getAssignedNodeId());
        // make sure all excluded nodes returned a NO decision
        for (String exludedNode : excludeNodes) {
            NodeRebalanceResult nodeRebalanceResult = rebalanceDecision.getNodeDecisions().get(exludedNode);
            assertEquals(Type.NO, nodeRebalanceResult.getCanAllocateDecision().type());
        }
    }

    private Tuple<ClusterState, RebalanceDecision> setupStateAndRebalance(AllocationDecider allocationDecider,
                                                                          Settings balancerSettings,
                                                                          boolean rebalanceExpected) {
        AllocationDecider rebalanceDecider = new AllocationDecider(Settings.EMPTY) {
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
        nodesBuilder.add(newNode(randomAsciiOfLength(7)));
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
        ShardRouting shard = clusterState.routingTable().index("idx").shard(0).primaryShard();
        RoutingAllocation routingAllocation = newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, allocationDeciders), clusterState);
        RebalanceDecision rebalanceDecision = allocator.decideRebalance(shard, routingAllocation);

        if (rebalanceExpected == false) {
            assertAssignedNodeRemainsSame(allocator, routingAllocation, shard);
        }

        return Tuple.tuple(clusterState, rebalanceDecision);
    }

    private RoutingAllocation newRoutingAllocation(AllocationDeciders deciders, ClusterState state) {
        RoutingAllocation allocation = new RoutingAllocation(
            deciders, new RoutingNodes(state, false), state, ClusterInfo.EMPTY, System.nanoTime(), false
        );
        allocation.debugDecision(true);
        return allocation;
    }

    private void assertAssignedNodeRemainsSame(BalancedShardsAllocator allocator, RoutingAllocation routingAllocation,
                                               ShardRouting originalRouting) {
        allocator.allocate(routingAllocation);
        RoutingNodes routingNodes = routingAllocation.routingNodes();
        // make sure the previous node id is the same as the current one after rerouting
        assertEquals(originalRouting.currentNodeId(), routingNodes.activePrimary(originalRouting.shardId()).currentNodeId());
    }
}
