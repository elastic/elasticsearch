/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.AbstractIntegrationTest.ClusterScope;
import static org.elasticsearch.test.AbstractIntegrationTest.Scope.SUITE;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = SUITE)
public class AckTests extends AbstractIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        //to test that the acknowledgement mechanism is working we better disable the wait for publish
        //otherwise the operation is most likely acknowledged even if it doesn't support ack
        return ImmutableSettings.builder().put("discovery.zen.publish_timeout", 0).build();
    }

    @Test
    public void testUpdateSettingsAcknowledgement() {
        createIndex("test");

        UpdateSettingsResponse updateSettingsResponse = client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put("refresh_interval", 9999)).get();
        assertThat(updateSettingsResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().setLocal(true).get();
            String refreshInterval = clusterStateResponse.getState().metaData().index("test").settings().get("index.refresh_interval");
            assertThat(refreshInterval, equalTo("9999"));
        }
    }

    @Test
    public void testUpdateSettingsNoAcknowledgement() {
        createIndex("test");

        UpdateSettingsResponse updateSettingsResponse = client().admin().indices().prepareUpdateSettings("test").setTimeout("0s")
                .setSettings(ImmutableSettings.builder().put("refresh_interval", 9999)).get();
        assertThat(updateSettingsResponse.isAcknowledged(), equalTo(false));
    }

    @Test
    public void testPutWarmerAcknowledgement() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            GetWarmersResponse getWarmersResponse = client.admin().indices().prepareGetWarmers().setLocal(true).get();
            assertThat(getWarmersResponse.warmers().size(), equalTo(1));
            Map.Entry<String,ImmutableList<IndexWarmersMetaData.Entry>> entry = getWarmersResponse.warmers().entrySet().iterator().next();
            assertThat(entry.getKey(), equalTo("test"));
            assertThat(entry.getValue().size(), equalTo(1));
            assertThat(entry.getValue().get(0).name(), equalTo("custom_warmer"));
        }
    }

    @Test
    public void testPutWarmerNoAcknowledgement() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer").setTimeout("0s")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(false));
    }

    @Test
    public void testDeleteWarmerAcknowledgement() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        DeleteWarmerResponse deleteWarmerResponse = client().admin().indices().prepareDeleteWarmer().setIndices("test").setName("custom_warmer").get();
        assertThat(deleteWarmerResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            GetWarmersResponse getWarmersResponse = client.admin().indices().prepareGetWarmers().setLocal(true).get();
            assertThat(getWarmersResponse.warmers().size(), equalTo(0));
        }
    }

    @Test
    public void testDeleteWarmerNoAcknowledgement() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer").setTimeout("0s")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(false));
    }

    @Test
    public void testDeleteMappingAcknowledgement() {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string").get();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1");

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").addTypes("type1").get();
        assertThat(getMappingsResponse.mappings().get("test").get("type1"), notNullValue());

        DeleteMappingResponse deleteMappingResponse = client().admin().indices().prepareDeleteMapping("test").setType("type1").get();
        assertThat(deleteMappingResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            getMappingsResponse = client.admin().indices().prepareGetMappings("test").addTypes("type1").setLocal(true).get();
            assertThat(getMappingsResponse.mappings().size(), equalTo(0));
        }
    }

    @Test
    public void testDeleteMappingNoAcknowledgement() {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string").get();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1");

        DeleteMappingResponse deleteMappingResponse = client().admin().indices().prepareDeleteMapping("test").setTimeout("0s").setType("type1").get();
        assertThat(deleteMappingResponse.isAcknowledged(), equalTo(false));
    }

    @Test
    public void testClusterRerouteAcknowledgement() throws InterruptedException {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", atLeast(cluster().numNodes()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();


        MoveAllocationCommand moveAllocationCommand = getAllocationCommand();

        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().add(moveAllocationCommand).get();
        assertThat(clusterRerouteResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().setLocal(true).get();
            RoutingNode routingNode = clusterStateResponse.getState().routingNodes().nodesToShards().get(moveAllocationCommand.fromNode());
            for (MutableShardRouting mutableShardRouting : routingNode) {
                //if the shard that we wanted to move is still on the same node, it must be relocating
                if (mutableShardRouting.shardId().equals(moveAllocationCommand.shardId())) {
                    assertThat(mutableShardRouting.relocating(), equalTo(true));
                }

            }

            routingNode = clusterStateResponse.getState().routingNodes().nodesToShards().get(moveAllocationCommand.toNode());
            boolean found = false;
            for (MutableShardRouting mutableShardRouting : routingNode) {
                if (mutableShardRouting.shardId().equals(moveAllocationCommand.shardId())) {
                    assertThat(mutableShardRouting.state(), anyOf(equalTo(ShardRoutingState.INITIALIZING), equalTo(ShardRoutingState.STARTED)));
                    found = true;
                    break;
                }
            }
            assertThat(found, equalTo(true));
        }
        //let's wait for the relocation to be completed, otherwise there can be issues with after test checks (mock directory wrapper etc.)
        waitForRelocation();
    }

    @Test
    public void testClusterRerouteNoAcknowledgement() throws InterruptedException {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", atLeast(cluster().numNodes()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();

        MoveAllocationCommand moveAllocationCommand = getAllocationCommand();

        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setTimeout("0s").add(moveAllocationCommand).get();
        assertThat(clusterRerouteResponse.isAcknowledged(), equalTo(false));
    }

    @Test
    public void testClusterRerouteAcknowledgementDryRun() throws InterruptedException {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", atLeast(cluster().numNodes()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();

        MoveAllocationCommand moveAllocationCommand = getAllocationCommand();

        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setDryRun(true).add(moveAllocationCommand).get();
        assertThat(clusterRerouteResponse.isAcknowledged(), equalTo(true));

        //testing only on master with the latest cluster state as we didn't make any change thus we cannot guarantee that
        //all nodes hold the same cluster state version. We only know there was no need to change anything, thus no need for ack on this update.
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        RoutingNode routingNode = clusterStateResponse.getState().routingNodes().nodesToShards().get(moveAllocationCommand.fromNode());
        boolean found = false;
        for (MutableShardRouting mutableShardRouting : routingNode) {
            //the shard that we wanted to move is still on the same node, as we had dryRun flag
            if (mutableShardRouting.shardId().equals(moveAllocationCommand.shardId())) {
                assertThat(mutableShardRouting.started(), equalTo(true));
                found = true;
                break;
            }
        }
        assertThat(found, equalTo(true));

        routingNode = clusterStateResponse.getState().routingNodes().nodesToShards().get(moveAllocationCommand.toNode());
        for (MutableShardRouting mutableShardRouting : routingNode) {
            if (mutableShardRouting.shardId().equals(moveAllocationCommand.shardId())) {
                fail("shard [" + mutableShardRouting + "] shouldn't be on node [" + moveAllocationCommand.toString() + "]");
            }
        }
    }

    @Test
    public void testClusterRerouteNoAcknowledgementDryRun() throws InterruptedException {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", atLeast(cluster().numNodes()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();

        MoveAllocationCommand moveAllocationCommand = getAllocationCommand();

        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setTimeout("0s").setDryRun(true).add(moveAllocationCommand).get();
        //acknowledged anyway as no changes were made
        assertThat(clusterRerouteResponse.isAcknowledged(), equalTo(true));
    }

    private MoveAllocationCommand getAllocationCommand() {
        String fromNodeId = null;
        String toNodeId = null;
        MutableShardRouting shardToBeMoved = null;
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        for (RoutingNode routingNode : clusterStateResponse.getState().routingNodes().nodesToShards().values()) {
            if (routingNode.node().isDataNode()) {
                if (fromNodeId == null && routingNode.numberOfOwningShards() > 0) {
                    fromNodeId = routingNode.nodeId();
                    shardToBeMoved = routingNode.shards().get(randomInt(routingNode.shards().size()-1));
                } else {
                    toNodeId = routingNode.nodeId();
                }

                if (toNodeId != null && fromNodeId != null) {
                    break;
                }
            }
        }

        assert fromNodeId != null;
        assert toNodeId != null;
        assert shardToBeMoved != null;

        logger.info("==> going to move shard [{}] from [{}] to [{}]", shardToBeMoved, fromNodeId, toNodeId);
        return new MoveAllocationCommand(shardToBeMoved.shardId(), fromNodeId, toNodeId);
    }

    @Test
    public void testClusterUpdateSettingsAcknowledgement() {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", atLeast(cluster().numNodes()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().get();
        String excludedNodeId = null;
        for (NodeInfo nodeInfo : nodesInfo) {
            if (nodeInfo.getNode().isDataNode()) {
                excludedNodeId = nodesInfo.getAt(0).getNode().id();
                break;
            }
        }
        assert excludedNodeId != null;

        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse = client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(settingsBuilder().put("cluster.routing.allocation.exclude._id", excludedNodeId)).get();
        assertThat(clusterUpdateSettingsResponse.isAcknowledged(), equalTo(true));
        assertThat(clusterUpdateSettingsResponse.getTransientSettings().get("cluster.routing.allocation.exclude._id"), equalTo(excludedNodeId));

        for (Client client : clients()) {
            ClusterState clusterState = client.admin().cluster().prepareState().setLocal(true).get().getState();
            assertThat(clusterState.routingNodes().metaData().transientSettings().get("cluster.routing.allocation.exclude._id"), equalTo(excludedNodeId));
            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        if (clusterState.nodes().get(shardRouting.currentNodeId()).id().equals(excludedNodeId)) {
                            //if the shard is still there it must be relocating and all nodes need to know, since the request was acknowledged
                            assertThat(shardRouting.relocating(), equalTo(true));
                        }
                    }
                }
            }
        }

        //let's wait for the relocation to be completed, otherwise there can be issues with after test checks (mock directory wrapper etc.)
        waitForRelocation();

        //removes the allocation exclude settings
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put("cluster.routing.allocation.exclude._id", "")).get();
    }

    @Test
    public void testClusterUpdateSettingsNoAcknowledgement() {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("number_of_shards", atLeast(cluster().numNodes()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().get();
        String excludedNodeId = null;
        for (NodeInfo nodeInfo : nodesInfo) {
            if (nodeInfo.getNode().isDataNode()) {
                excludedNodeId = nodesInfo.getAt(0).getNode().id();
                break;
            }
        }
        assert excludedNodeId != null;

        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse = client().admin().cluster().prepareUpdateSettings().setTimeout("0s")
                .setTransientSettings(settingsBuilder().put("cluster.routing.allocation.exclude._id", excludedNodeId)).get();
        assertThat(clusterUpdateSettingsResponse.isAcknowledged(), equalTo(false));
        assertThat(clusterUpdateSettingsResponse.getTransientSettings().get("cluster.routing.allocation.exclude._id"), equalTo(excludedNodeId));

        //let's wait for the relocation to be completed, otherwise there can be issues with after test checks (mock directory wrapper etc.)
        waitForRelocation();

        //removes the allocation exclude settings
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put("cluster.routing.allocation.exclude._id", "")).get();
    }
}
