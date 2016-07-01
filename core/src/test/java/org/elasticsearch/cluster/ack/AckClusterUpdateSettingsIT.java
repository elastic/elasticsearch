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

package org.elasticsearch.cluster.ack;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = TEST, minNumDataNodes = 2)
public class AckClusterUpdateSettingsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                //make sure that enough concurrent reroutes can happen at the same time
                //we have a minimum of 2 nodes, and a maximum of 10 shards, thus 5 should be enough
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 5)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 5)
                .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), 10)
                .build();
    }

    @Override
    protected int minimumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }


    private void removePublishTimeout() {
        //to test that the acknowledgement mechanism is working we better disable the wait for publish
        //otherwise the operation is most likely acknowledged even if it doesn't support ack
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0")));
    }

    public void testClusterUpdateSettingsAcknowledgement() {
        createIndex("test");
        ensureGreen();

        // now that the cluster is stable, remove timeout
        removePublishTimeout();

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().get();
        String excludedNodeId = null;
        for (NodeInfo nodeInfo : nodesInfo.getNodes()) {
            if (nodeInfo.getNode().isDataNode()) {
                excludedNodeId = nodeInfo.getNode().getId();
                break;
            }
        }
        assertNotNull(excludedNodeId);

        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse = client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._id", excludedNodeId)).get();
        assertAcked(clusterUpdateSettingsResponse);
        assertThat(clusterUpdateSettingsResponse.getTransientSettings().get("cluster.routing.allocation.exclude._id"), equalTo(excludedNodeId));

        for (Client client : clients()) {
            ClusterState clusterState = getLocalClusterState(client);
            assertThat(clusterState.metaData().transientSettings().get("cluster.routing.allocation.exclude._id"), equalTo(excludedNodeId));
            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        assert clusterState.nodes() != null;
                        if (shardRouting.unassigned() == false && clusterState.nodes().get(shardRouting.currentNodeId()).getId().equals(excludedNodeId)) {
                            //if the shard is still there it must be relocating and all nodes need to know, since the request was acknowledged
                            //reroute happens as part of the update settings and we made sure no throttling comes into the picture via settings
                            assertThat(shardRouting.relocating(), equalTo(true));
                        }
                    }
                }
            }
        }
    }

    public void testClusterUpdateSettingsNoAcknowledgement() {
        client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder()
                        .put("number_of_shards", between(cluster().numDataNodes(), DEFAULT_MAX_NUM_SHARDS))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();

        // now that the cluster is stable, remove timeout
        removePublishTimeout();

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().get();
        String excludedNodeId = null;
        for (NodeInfo nodeInfo : nodesInfo.getNodes()) {
            if (nodeInfo.getNode().isDataNode()) {
                excludedNodeId = nodeInfo.getNode().getId();
                break;
            }
        }
        assertNotNull(excludedNodeId);

        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse = client().admin().cluster().prepareUpdateSettings().setTimeout("0s")
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._id", excludedNodeId)).get();
        assertThat(clusterUpdateSettingsResponse.isAcknowledged(), equalTo(false));
        assertThat(clusterUpdateSettingsResponse.getTransientSettings().get("cluster.routing.allocation.exclude._id"), equalTo(excludedNodeId));
    }

    private static ClusterState getLocalClusterState(Client client) {
        return client.admin().cluster().prepareState().setLocal(true).get().getState();
    }

    public void testOpenIndexNoAcknowledgement() {
        createIndex("test");
        ensureGreen();
        removePublishTimeout();
        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));

        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen("test").setTimeout("0s").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(false));
        ensureGreen("test"); // make sure that recovery from disk has completed, so that check index doesn't fail.
    }
}
