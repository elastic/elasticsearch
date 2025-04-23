/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
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
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction.findShardToExplain;
import static org.hamcrest.Matchers.aMapWithSize;
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

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();

        ShardRouting shard = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            clusterState,
            null,
            null,
            System.nanoTime()
        );
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
            }, null, null, TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
        );

        assertEquals(shard.currentNodeId(), cae.getCurrentNode().getId());
        assertTrue(cae.isSpecificShard());
        assertFalse(cae.getShardAllocationDecision().isDecisionTaken());
        assertFalse(cae.getShardAllocationDecision().getAllocateDecision().isDecisionTaken());
        assertFalse(cae.getShardAllocationDecision().getMoveDecision().isDecisionTaken());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        ChunkedToXContent.wrapAsToXContent(cae).toXContent(builder, ToXContent.EMPTY_PARAMS);
        String explanation;
        if (shardRoutingState == ShardRoutingState.RELOCATING) {
            explanation = "the shard is in the process of relocating from node [] to node [], wait until " + "relocation has completed";
        } else {
            explanation = "the shard is in the process of initializing on node [], " + "wait until initialization has completed";
        }
        Object[] args = new Object[] {
            shardRoutingState.toString().toLowerCase(Locale.ROOT),
            shard.unassignedInfo() != null
                ? Strings.format(
                    """
                        ,"unassigned_info": {"reason": "%s", "at": "%s", "last_allocation_status": "%s"}
                        """,
                    shard.unassignedInfo().reason(),
                    UnassignedInfo.DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(shard.unassignedInfo().unassignedTimeMillis())),
                    AllocationDecision.fromAllocationStatus(shard.unassignedInfo().lastAllocationStatus())
                )
                : "",
            cae.getCurrentNode().getId(),
            cae.getCurrentNode().getName(),
            cae.getCurrentNode().getAddress(),
            cae.getCurrentNode().getRoles().stream().map(r -> '"' + r.roleName() + '"').collect(Collectors.joining(", ", "[", "]")),
            explanation };
        assertEquals(XContentHelper.stripWhitespace(Strings.format("""
            {
              "index": "idx",
              "shard": 0,
              "primary": true,
              "current_state": "%s"
              %s,
              "current_node": {
                "id": "%s",
                "name": "%s",
                "transport_address": "%s",
                "roles": %s
              },
              "explanation": "%s"
            }""", args)), Strings.toString(builder));
    }

    public void testFindAnyUnassignedShardToExplain() {
        // find unassigned primary
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.UNASSIGNED);
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        Set<ProjectId> projectIds = clusterState.metadata().projects().keySet();
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState), projectIds);
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).primaryShard(), shard);

        // find unassigned replica
        clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED);
        request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        shard = findShardToExplain(request, routingAllocation(clusterState), projectIds);
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).replicaShards().get(0), shard);

        // prefer unassigned primary to replica
        clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(new String[] { "idx1", "idx2" }, 1, 1);
        final String redIndex = randomBoolean() ? "idx1" : "idx2";
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());
        for (final IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            final IndexRoutingTable.Builder indexBuilder = IndexRoutingTable.builder(indexRoutingTable.getIndex());
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
                final IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(indexShardRoutingTable.shardId());
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                    if (shardRouting.primary() == false || indexRoutingTable.getIndex().getName().equals(redIndex)) {
                        // move all replicas and one primary to unassigned
                        shardBuilder.addShard(
                            shardRouting.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "test"))
                        );
                    } else {
                        shardBuilder.addShard(shardRouting);
                    }
                }
                indexBuilder.addIndexShard(shardBuilder);
            }
            routingTableBuilder.add(indexBuilder);
        }
        clusterState = ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();
        request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        shard = findShardToExplain(request, routingAllocation(clusterState), projectIds);
        assertEquals(clusterState.getRoutingTable().index(redIndex).shard(0).primaryShard(), shard);

        // no unassigned shard to explain
        final ClusterState allStartedClusterState = ClusterStateCreationUtils.state(
            "idx",
            randomBoolean(),
            ShardRoutingState.STARTED,
            ShardRoutingState.STARTED
        );
        final ClusterAllocationExplainRequest anyUnassignedShardsRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> findShardToExplain(anyUnassignedShardsRequest, routingAllocation(allStartedClusterState), projectIds)
            ).getMessage(),
            allOf(
                // no point in asserting the precise wording of the message into this test, but we care that it contains these bits:
                containsString("There are no unassigned shards in this cluster."),
                containsString("Specify an assigned shard in the request body"),
                containsString("https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-cluster-allocation-explain")
            )
        );
    }

    public void testFindPrimaryShardToExplain() {
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), randomFrom(ShardRoutingState.values()));
        Set<ProjectId> projectIds = clusterState.metadata().projects().keySet();
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT, "idx", 0, true, null);
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState), projectIds);
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).primaryShard(), shard);
    }

    public void testFindAnyReplicaToExplain() {
        // prefer unassigned replicas to started replicas
        ClusterState clusterState = ClusterStateCreationUtils.state(
            "idx",
            randomBoolean(),
            ShardRoutingState.STARTED,
            ShardRoutingState.STARTED,
            ShardRoutingState.UNASSIGNED
        );
        Set<ProjectId> projectIds = clusterState.metadata().projects().keySet();
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT, "idx", 0, false, null);
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState), projectIds);
        assertEquals(
            clusterState.getRoutingTable()
                .index("idx")
                .shard(0)
                .replicaShards()
                .stream()
                .filter(ShardRouting::unassigned)
                .findFirst()
                .get(),
            shard
        );

        // prefer started replicas to initializing/relocating replicas
        clusterState = ClusterStateCreationUtils.state(
            "idx",
            randomBoolean(),
            ShardRoutingState.STARTED,
            randomFrom(ShardRoutingState.RELOCATING, ShardRoutingState.INITIALIZING),
            ShardRoutingState.STARTED
        );
        request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT, "idx", 0, false, null);
        shard = findShardToExplain(request, routingAllocation(clusterState), projectIds);
        assertEquals(
            clusterState.getRoutingTable().index("idx").shard(0).replicaShards().stream().filter(ShardRouting::started).findFirst().get(),
            shard
        );
    }

    public void testFindShardAssignedToNode() {
        // find shard with given node
        final boolean primary = randomBoolean();
        ShardRoutingState[] replicaStates = new ShardRoutingState[0];
        if (primary == false) {
            replicaStates = new ShardRoutingState[] { ShardRoutingState.STARTED };
        }
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED, replicaStates);
        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();
        ShardRouting shardToExplain = primary
            ? clusterState.routingTable(projectId).index("idx").shard(0).primaryShard()
            : clusterState.routingTable(projectId).index("idx").shard(0).replicaShards().get(0);
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest(
            TEST_REQUEST_TIMEOUT,
            "idx",
            0,
            primary,
            shardToExplain.currentNodeId()
        );
        RoutingAllocation allocation = routingAllocation(clusterState);
        ShardRouting foundShard = findShardToExplain(request, allocation, Set.of(projectId));
        assertEquals(shardToExplain, foundShard);

        // shard is not assigned to given node
        String explainNode = null;
        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            if (routingNode.nodeId().equals(shardToExplain.currentNodeId()) == false) {
                explainNode = routingNode.nodeId();
                break;
            }
        }
        final ClusterAllocationExplainRequest failingRequest = new ClusterAllocationExplainRequest(
            TEST_REQUEST_TIMEOUT,
            "idx",
            0,
            primary,
            explainNode
        );
        expectThrows(IllegalArgumentException.class, () -> findShardToExplain(failingRequest, allocation, Set.of(projectId)));
    }

    private static RoutingAllocation routingAllocation(ClusterState clusterState) {
        return new RoutingAllocation(NOOP_DECIDERS, clusterState, null, null, System.nanoTime());
    }
}
