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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.Balancer;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.RebalanceDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;

/**
 * Tests for balancing a single shard, see {@link Balancer#balanceShard(ShardRouting)}.
 */
public class BalancedSingleShardTests extends ESAllocationTestCase {

    public void testRebalanceNonStartedShardNotAllowed() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(),
            randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.UNASSIGNED));
        ShardRouting unassignedShard = clusterState.routingTable().index("idx").shard(0).primaryShard();
        RebalanceDecision rebalanceDecision = allocator.rebalanceShard(unassignedShard, newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, Collections.emptyList()), clusterState));
        assertSame(RebalanceDecision.NOT_TAKEN, rebalanceDecision);
    }

    public void testRebalanceNotAllowedDuringPendingAsyncFetch() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        ShardRouting unassignedShard = clusterState.routingTable().index("idx").shard(0).primaryShard();
        RoutingAllocation routingAllocation = newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, Collections.emptyList()), clusterState);
        routingAllocation.setHasPendingAsyncFetch();
        RebalanceDecision rebalanceDecision = allocator.rebalanceShard(unassignedShard, routingAllocation);
        assertNotNull(rebalanceDecision.getCanRebalanceDecision());
        assertEquals(Type.NO, rebalanceDecision.getFinalDecisionType());
        assertEquals("cannot rebalance due to in-flight shard store fetches", rebalanceDecision.getFinalExplanation());
        assertNull(rebalanceDecision.getNodeDecisions());
        assertNull(rebalanceDecision.getAssignedNodeId());
    }

    public void testRebalanceNotAllowed() {
        AllocationDecider noRebalanceDecider = new AllocationDecider(Settings.EMPTY) {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.NO;
            }
        };
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        ShardRouting unassignedShard = clusterState.routingTable().index("idx").shard(0).primaryShard();
        RebalanceDecision rebalanceDecision = allocator.rebalanceShard(unassignedShard, newRoutingAllocation(
            new AllocationDeciders(Settings.EMPTY, Collections.singleton(noRebalanceDecider)), clusterState));
        assertNotEquals(Type.YES, rebalanceDecision.getCanRebalanceDecision().type());
        assertEquals(Type.NO, rebalanceDecision.getFinalDecisionType());
        assertEquals("rebalancing is not allowed", rebalanceDecision.getFinalExplanation());
        assertNull(rebalanceDecision.getNodeDecisions());
        assertNull(rebalanceDecision.getAssignedNodeId());
    }

    public void testBalanceShard() {
        //TODO:
    }

    public void testDontBalanceShard() {
        //TODO:
    }

    private RoutingAllocation newRoutingAllocation(AllocationDeciders deciders, ClusterState state) {
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, ClusterInfo.EMPTY, System.nanoTime(), false);
    }
}
