/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
        final List<String> dataOnlyNodes = getFollowerCluster().startNodes(between(1, 2),
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE)));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex)
            .setSource(getIndexSettings(between(1, 2), between(0, 1)), XContentType.JSON));
        final PutFollowAction.Request putFollowRequest = putFollow(leaderIndex, followerIndex);
        putFollowRequest.setSettings(Settings.builder()
            .put("index.routing.allocation.include._name", String.join(",", dataOnlyNodes))
            .build());
        putFollowRequest.waitForActiveShards(ActiveShardCount.ONE);
        putFollowRequest.timeout(TimeValue.timeValueSeconds(2));
        final PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, putFollowRequest).get();
        assertFalse(response.isFollowIndexShardsAcked());
        assertFalse(response.isIndexFollowingStarted());
        final ClusterAllocationExplanation explanation = followerClient().admin().cluster().prepareAllocationExplain()
            .setIndex(followerIndex).setShard(0).setPrimary(true).get().getExplanation();
        for (NodeAllocationResult nodeDecision : explanation.getShardAllocationDecision().getAllocateDecision().getNodeDecisions()) {
            assertThat(nodeDecision.getNodeDecision(), equalTo(AllocationDecision.NO));
            if (dataOnlyNodes.contains(nodeDecision.getNode().getName())) {
                final List<String> decisions = nodeDecision.getCanAllocateDecision().getDecisions()
                    .stream().map(Object::toString).collect(Collectors.toList());
                assertThat("NO(shard is a primary follower and being bootstrapped, but node does not have the remote_cluster_client role)",
                    in(decisions));
            }
        }
    }

    public void testAllocateFollowerPrimaryToNodesWithRemoteClusterClientRole() throws Exception {
        final String leaderIndex = "leader-allow-index";
        final String followerIndex = "follower-allow-index";
        final List<String> dataOnlyNodes = getFollowerCluster().startNodes(between(2, 3),
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE)));
        final List<String> dataAndRemoteNodes = getFollowerCluster().startNodes(between(1, 2),
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex)
            .setSource(getIndexSettings(between(1, 2), between(0, 1)), XContentType.JSON));
        final PutFollowAction.Request putFollowRequest = putFollow(leaderIndex, followerIndex);
        putFollowRequest.setSettings(Settings.builder()
            .put("index.routing.rebalance.enable", "none")
            .put("index.routing.allocation.include._name",
                Stream.concat(dataOnlyNodes.stream(), dataAndRemoteNodes.stream()).collect(Collectors.joining(",")))
            .build());
        final PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, putFollowRequest).get();
        assertTrue(response.isFollowIndexShardsAcked());
        assertTrue(response.isIndexFollowingStarted());
        ensureFollowerGreen(followerIndex);
        int numDocs = between(0, 20);
        for (int i = 0; i < numDocs; i++) {
            leaderClient().prepareIndex(leaderIndex).setSource("f", i).get();
        }
        // Empty follower primaries must be assigned to nodes with the remote cluster client role
        assertBusy(() -> {
            final ClusterState state = getFollowerCluster().client().admin().cluster().prepareState().get().getState();
            for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(followerIndex)) {
                final ShardRouting primaryShard = shardRoutingTable.primaryShard();
                assertTrue(primaryShard.assignedToNode());
                final DiscoveryNode assignedNode = state.nodes().get(primaryShard.currentNodeId());
                assertThat(shardRoutingTable.toString(), assignedNode.getName(), in(dataAndRemoteNodes));
            }
        }, 30, TimeUnit.SECONDS);
        // Follower primaries can be relocated to nodes without the remote cluster client role
        followerClient().admin().indices().prepareUpdateSettings(followerIndex)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .setSettings(Settings.builder().put("index.routing.allocation.include._name", String.join(",", dataOnlyNodes)))
            .get();
        assertBusy(() -> {
            final ClusterState state = getFollowerCluster().client().admin().cluster().prepareState().get().getState();
            for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(followerIndex)) {
                for (ShardRouting shard : shardRoutingTable) {
                    assertNotNull(shard.currentNodeId());
                    final DiscoveryNode assignedNode = state.nodes().get(shard.currentNodeId());
                    assertThat(shardRoutingTable.toString(), assignedNode.getName(), in(dataOnlyNodes));
                }
            }
        }, 30, TimeUnit.SECONDS);
        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);
        // Follower primaries can be recovered from the existing copies on nodes without the remote cluster client role
        getFollowerCluster().fullRestart();
        ensureFollowerGreen(followerIndex);
        assertBusy(() -> {
            final ClusterState state = getFollowerCluster().client().admin().cluster().prepareState().get().getState();
            for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(followerIndex)) {
                for (ShardRouting shard : shardRoutingTable) {
                    assertNotNull(shard.currentNodeId());
                    final DiscoveryNode assignedNode = state.nodes().get(shard.currentNodeId());
                    assertThat(shardRoutingTable.toString(), assignedNode.getName(), in(dataOnlyNodes));
                }
            }
        }, 30, TimeUnit.SECONDS);
        int moreDocs = between(0, 20);
        for (int i = 0; i < moreDocs; i++) {
            leaderClient().prepareIndex(leaderIndex).setSource("f", i).get();
        }
        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);
    }
}
