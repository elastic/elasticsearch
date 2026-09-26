/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanationUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

public class PrimaryFollowerAllocationIT extends CcrIntegTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testDoNotAllocateFollowerPrimaryToNodesWithoutRemoteClusterClientRole() throws Exception {
        final String leaderIndex = "leader-not-allow-index";
        final String followerIndex = "follower-not-allow-index";
        final List<String> dataOnlyNodes = getFollowerCluster().startNodes(
            between(1, 2),
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        assertAcked(
            leaderClient().admin()
                .indices()
                .prepareCreate(leaderIndex)
                .setSource(getIndexSettings(between(1, 2), between(0, 1)), XContentType.JSON)
        );
        final PutFollowAction.Request putFollowRequest = putFollow(leaderIndex, followerIndex);
        putFollowRequest.setSettings(
            Settings.builder()
                .put("index.routing.allocation.include._name", String.join(",", dataOnlyNodes))
                .putNull("index.routing.allocation.include._tier_preference")
                .build()
        );
        putFollowRequest.waitForActiveShards(ActiveShardCount.ONE);
        putFollowRequest.ackTimeout(TimeValue.timeValueSeconds(2));
        final PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, putFollowRequest).get();
        assertFalse(response.isFollowIndexShardsAcked());
        assertFalse(response.isIndexFollowingStarted());
        final var explanation = ClusterAllocationExplanationUtils.getClusterAllocationExplanation(followerClient(), followerIndex, 0, true);
        for (NodeAllocationResult nodeDecision : explanation.getShardAllocationDecision().getAllocateDecision().getNodeDecisions()) {
            assertThat(nodeDecision.getNodeDecision(), equalTo(AllocationDecision.NO));
            if (dataOnlyNodes.contains(nodeDecision.getNode().getName())) {
                final List<String> decisions = nodeDecision.getCanAllocateDecision()
                    .getDecisions()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
                assertThat(
                    "NO(shard is a primary follower and being bootstrapped, but node does not have the remote_cluster_client role)",
                    in(decisions)
                );
            }
        }
    }

    public void testAllocateFollowerPrimaryToNodesWithRemoteClusterClientRole() throws Exception {
        final String leaderIndex = "leader-allow-index";
        final String followerIndex = "follower-allow-index";
        final List<String> dataOnlyNodes = getFollowerCluster().startNodes(
            between(2, 3),
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        final List<String> dataAndRemoteNodes = getFollowerCluster().startNodes(
            between(1, 2),
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
        );
        assertAcked(
            leaderClient().admin()
                .indices()
                .prepareCreate(leaderIndex)
                .setSource(getIndexSettings(between(1, 2), between(0, 1)), XContentType.JSON)
        );
        final PutFollowAction.Request putFollowRequest = putFollow(leaderIndex, followerIndex);
        putFollowRequest.setSettings(
            Settings.builder()
                .put("index.routing.rebalance.enable", "none")
                .put(
                    "index.routing.allocation.include._name",
                    Stream.concat(dataOnlyNodes.stream(), dataAndRemoteNodes.stream()).collect(Collectors.joining(","))
                )
                .putNull("index.routing.allocation.include._tier_preference")
                .build()
        );
        final PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, putFollowRequest).get();
        assertTrue(response.isFollowIndexShardsAcked());
        assertTrue(response.isIndexFollowingStarted());
        ensureFollowerGreen(followerIndex);
        int numDocs = between(0, 20);
        for (int i = 0; i < numDocs; i++) {
            leaderClient().prepareIndex(leaderIndex).setSource("f", i).get();
        }
        // Empty follower primaries must be assigned to nodes with the remote cluster client role
        awaitFollowerClusterState(state -> followerPrimariesOnNodes(state, followerIndex, dataAndRemoteNodes));
        // Follower primaries can be relocated to nodes without the remote cluster client role
        followerClient().admin()
            .indices()
            .prepareUpdateSettings(followerIndex)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .setSettings(
                Settings.builder()
                    .putNull("index.routing.allocation.include._tier_preference")
                    .put("index.routing.allocation.include._name", String.join(",", dataOnlyNodes))
            )
            .get();
        awaitFollowerClusterState(state -> followerShardsOnNodes(state, followerIndex, dataOnlyNodes));
        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);
        // Follower primaries can be recovered from the existing copies on nodes without the remote cluster client role
        getFollowerCluster().fullRestart();
        ensureFollowerGreen(followerIndex);
        awaitFollowerClusterState(state -> followerShardsOnNodes(state, followerIndex, dataOnlyNodes));
        int moreDocs = between(0, 20);
        for (int i = 0; i < moreDocs; i++) {
            leaderClient().prepareIndex(leaderIndex).setSource("f", i).get();
        }
        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);
    }

    private void awaitFollowerClusterState(Predicate<ClusterState> predicate) {
        ClusterServiceUtils.awaitClusterState(predicate, getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class));
    }

    private static boolean followerPrimariesOnNodes(ClusterState state, String followerIndex, List<String> nodes) {
        IndexRoutingTable indexRoutingTable = state.routingTable().index(followerIndex);
        if (indexRoutingTable == null) {
            return false;
        }
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
            ShardRouting primaryShard = shardRoutingTable.primaryShard();
            if (primaryShard.assignedToNode() == false) {
                return false;
            }
            String nodeName = state.nodes().get(primaryShard.currentNodeId()).getName();
            if (nodes.contains(nodeName) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean followerShardsOnNodes(ClusterState state, String followerIndex, List<String> nodes) {
        IndexRoutingTable indexRoutingTable = state.routingTable().index(followerIndex);
        if (indexRoutingTable == null) {
            return false;
        }
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shard = shardRoutingTable.shard(copy);
                if (shard.currentNodeId() == null) {
                    return false;
                }
                String nodeName = state.nodes().get(shard.currentNodeId()).getName();
                if (nodes.contains(nodeName) == false) {
                    return false;
                }
            }
        }
        return true;
    }
}
