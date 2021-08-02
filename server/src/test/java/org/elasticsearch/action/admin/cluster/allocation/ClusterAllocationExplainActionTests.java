/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.time.Instant;
import java.util.Collections;
import java.util.Locale;

import static org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction.findShardToExplain;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for the {@link TransportClusterAllocationExplainAction} class.
 */
public class ClusterAllocationExplainActionTests extends ESTestCase {

    private static final AllocationDeciders NOOP_DECIDERS = new AllocationDeciders(Collections.emptyList());

    public void testInitializingOrRelocatingShardExplanation() throws Exception {
        ShardRoutingState shardRoutingState = randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), shardRoutingState);
        ShardRouting shard = clusterState.getRoutingTable().index("idx").shard(0).primaryShard();
        RoutingAllocation allocation = new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            clusterState.getRoutingNodes(), clusterState, null, null, System.nanoTime());
        ClusterAllocationExplanation cae = TransportClusterAllocationExplainAction.explainShard(
            shard,
            allocation,
            null,
            randomBoolean(),
            true,
            new AllocationService(null, new TestGatewayAllocator(), new ShardsAllocator() {
                @Override
                public void allocate(RoutingAllocation allocation) {
                    // no-op
                }

                @Override
                public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    if (shard.initializing() || shard.relocating()) {
                        return ShardAllocationDecision.NOT_TAKEN;
                    } else {
                        throw new UnsupportedOperationException("cannot explain");
                    }
                }
            }, null, null));

        assertEquals(shard.currentNodeId(), cae.getCurrentNode().getId());
        assertTrue(cae.isSpecificShard());
        assertFalse(cae.getShardAllocationDecision().isDecisionTaken());
        assertFalse(cae.getShardAllocationDecision().getAllocateDecision().isDecisionTaken());
        assertFalse(cae.getShardAllocationDecision().getMoveDecision().isDecisionTaken());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        cae.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String explanation;
        if (shardRoutingState == ShardRoutingState.RELOCATING) {
            explanation = "the shard is in the process of relocating from node [] to node [], wait until " +
                              "relocation has completed";
        } else {
            explanation = "the shard is in the process of initializing on node [], " +
                              "wait until initialization has completed";
        }
        assertEquals("{\"index\":\"idx\",\"shard\":0,\"primary\":true,\"current_state\":\"" +
                         shardRoutingState.toString().toLowerCase(Locale.ROOT) + "\"" +
                        (shard.unassignedInfo() != null ?
                            ",\"unassigned_info\":{"
                                + "\"reason\":\"" + shard.unassignedInfo().getReason() + "\","
                                + "\"at\":\""+ UnassignedInfo.DATE_TIME_FORMATTER.format(
                                    Instant.ofEpochMilli(shard.unassignedInfo().getUnassignedTimeInMillis())) + "\","
                                + "\"last_allocation_status\":\"" + AllocationDecision.fromAllocationStatus(
                                    shard.unassignedInfo().getLastAllocationStatus()) + "\"}"
                            : "")
                        + ",\"current_node\":" +
                         "{\"id\":\"" + cae.getCurrentNode().getId() + "\",\"name\":\"" + cae.getCurrentNode().getName() +
                         "\",\"transport_address\":\"" + cae.getCurrentNode().getAddress() +
                         "\"},\"explanation\":\"" + explanation + "\"}", Strings.toString(builder));
    }

    public void testFindAnyUnassignedShardToExplain() {
        // find unassigned primary
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.UNASSIGNED);
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest();
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).primaryShard(), shard);

        // find unassigned replica
        clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED);
        request = new ClusterAllocationExplainRequest();
        shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).replicaShards().get(0), shard);

        // no unassigned shard to explain
        final ClusterState allStartedClusterState = ClusterStateCreationUtils.state("idx", randomBoolean(),
            ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        final ClusterAllocationExplainRequest anyUnassignedShardsRequest = new ClusterAllocationExplainRequest();
        assertThat(expectThrows(
            IllegalArgumentException.class,
            () -> findShardToExplain(anyUnassignedShardsRequest, routingAllocation(allStartedClusterState))).getMessage(),
            allOf(
                // no point in asserting the precise wording of the message into this test, but we care that it contains these bits:
                containsString("No shard was specified in the request"),
                containsString("specify the target shard in the request")));
    }

    public void testFindPrimaryShardToExplain() {
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), randomFrom(ShardRoutingState.values()));
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest("idx", 0, true, null);
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).primaryShard(), shard);
    }

    public void testFindAnyReplicaToExplain() {
        // prefer unassigned replicas to started replicas
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED,
            ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED);
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest("idx", 0, false, null);
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).replicaShards()
                         .stream().filter(ShardRouting::unassigned).findFirst().get(), shard);

        // prefer started replicas to initializing/relocating replicas
        clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED,
            randomFrom(ShardRoutingState.RELOCATING, ShardRoutingState.INITIALIZING), ShardRoutingState.STARTED);
        request = new ClusterAllocationExplainRequest("idx", 0, false, null);
        shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).replicaShards()
                         .stream().filter(ShardRouting::started).findFirst().get(), shard);
    }

    public void testFindShardAssignedToNode() {
        // find shard with given node
        final boolean primary = randomBoolean();
        ShardRoutingState[] replicaStates = new ShardRoutingState[0];
        if (primary == false) {
            replicaStates = new ShardRoutingState[] { ShardRoutingState.STARTED };
        }
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED, replicaStates);
        ShardRouting shardToExplain = primary ? clusterState.getRoutingTable().index("idx").shard(0).primaryShard() :
                                          clusterState.getRoutingTable().index("idx").shard(0).replicaShards().get(0);
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest("idx", 0, primary, shardToExplain.currentNodeId());
        RoutingAllocation allocation = routingAllocation(clusterState);
        ShardRouting foundShard = findShardToExplain(request, allocation);
        assertEquals(shardToExplain, foundShard);

        // shard is not assigned to given node
        String explainNode = null;
        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            if (routingNode.nodeId().equals(shardToExplain.currentNodeId()) == false) {
                explainNode = routingNode.nodeId();
                break;
            }
        }
        final ClusterAllocationExplainRequest failingRequest = new ClusterAllocationExplainRequest("idx", 0, primary, explainNode);
        expectThrows(IllegalArgumentException.class, () -> findShardToExplain(failingRequest, allocation));
    }

    private static RoutingAllocation routingAllocation(ClusterState clusterState) {
        return new RoutingAllocation(NOOP_DECIDERS, clusterState.getRoutingNodes(), clusterState, null, null, System.nanoTime());
    }
}
