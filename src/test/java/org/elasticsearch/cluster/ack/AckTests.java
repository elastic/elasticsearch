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

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.*;

import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = SUITE)
public class AckTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        //to test that the acknowledgement mechanism is working we better disable the wait for publish
        //otherwise the operation is most likely acknowledged even if it doesn't support ack
        return ImmutableSettings.builder().put("discovery.zen.publish_timeout", 0).build();
    }

    @Test
    public void testUpdateSettingsAcknowledgement() {
        createIndex("test");

        assertAcked(client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put("refresh_interval", 9999)));

        for (Client client : clients()) {
            String refreshInterval = getLocalClusterState(client).metaData().index("test").settings().get("index.refresh_interval");
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

        assertAcked(client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery())));

        for (Client client : clients()) {
            ClusterState clusterState = client.admin().cluster().prepareState().setLocal(true).get().getState();
            IndexWarmersMetaData warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
            assertThat(warmersMetaData, notNullValue());
            assertThat(warmersMetaData.entries().size(), equalTo(1));
            IndexWarmersMetaData.Entry entry = warmersMetaData.entries().iterator().next();
            assertThat(entry.name(), equalTo("custom_warmer"));
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

        assertAcked(client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery())));

        assertAcked(client().admin().indices().prepareDeleteWarmer().setIndices("test").setName("custom_warmer"));

        for (Client client : clients()) {
            ClusterState clusterState = client.admin().cluster().prepareState().setLocal(true).get().getState();
            IndexWarmersMetaData warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
            assertThat(warmersMetaData, notNullValue());
            assertThat(warmersMetaData.entries().size(), equalTo(0));
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

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        MappingMetaData mapping = clusterStateResponse.getState().metaData().index("test").mapping("type1");
        assertThat(mapping, notNullValue());

        assertAcked(client().admin().indices().prepareDeleteMapping("test").setType("type1"));

        for (Client client : clients()) {
            clusterStateResponse = client.admin().cluster().prepareState().setLocal(true).get();
            mapping = clusterStateResponse.getState().metaData().index("test").mapping("type1");
            assertThat(mapping, nullValue());
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
                        .put("number_of_shards", atLeast(cluster().size()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();


        MoveAllocationCommand moveAllocationCommand = getAllocationCommand();

        assertAcked(client().admin().cluster().prepareReroute().add(moveAllocationCommand));

        for (Client client : clients()) {
            ClusterState clusterState = getLocalClusterState(client);
            RoutingNode routingNode = clusterState.routingNodes().nodesToShards().get(moveAllocationCommand.fromNode());
            for (MutableShardRouting mutableShardRouting : routingNode) {
                //if the shard that we wanted to move is still on the same node, it must be relocating
                if (mutableShardRouting.shardId().equals(moveAllocationCommand.shardId())) {
                    assertThat(mutableShardRouting.relocating(), equalTo(true));
                }

            }

            routingNode = clusterState.routingNodes().nodesToShards().get(moveAllocationCommand.toNode());
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
                        .put("number_of_shards", atLeast(cluster().size()))
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
                        .put("number_of_shards", atLeast(cluster().size()))
                        .put("number_of_replicas", 0)).get();
        ensureGreen();

        MoveAllocationCommand moveAllocationCommand = getAllocationCommand();

        assertAcked(client().admin().cluster().prepareReroute().setDryRun(true).add(moveAllocationCommand));

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
                        .put("number_of_shards", atLeast(cluster().size()))
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
                        .put("number_of_shards", atLeast(cluster().size()))
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
        assertAcked(clusterUpdateSettingsResponse);
        assertThat(clusterUpdateSettingsResponse.getTransientSettings().get("cluster.routing.allocation.exclude._id"), equalTo(excludedNodeId));

        for (Client client : clients()) {
            ClusterState clusterState = getLocalClusterState(client);
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
                        .put("number_of_shards", atLeast(cluster().size()))
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

    @Test
    public void testIndicesAliasesAcknowledgement() {
        createIndex("test");

        //testing acknowledgement when trying to submit an existing alias too
        //in that case it would not make any change, but we are sure about the cluster state
        //as the previous operation was acknowledged
        for (int i = 0; i < 2; i++) {
            assertAcked(client().admin().indices().prepareAliases().addAlias("test", "alias"));

            for (Client client : clients()) {
                AliasMetaData aliasMetaData = getLocalClusterState(client).metaData().aliases().get("alias").get("test");
                assertThat(aliasMetaData.alias(), equalTo("alias"));
            }
        }
    }

    @Test
    public void testIndicesAliasesNoAcknowledgement() {
        createIndex("test");

        IndicesAliasesResponse indicesAliasesResponse = client().admin().indices().prepareAliases().addAlias("test", "alias").setTimeout("0s").get();
        assertThat(indicesAliasesResponse.isAcknowledged(), equalTo(false));
    }

    public void testCloseIndexAcknowledgement() {
        createIndex("test");
        ensureGreen();

        assertAcked(client().admin().indices().prepareClose("test"));

        for (Client client : clients()) {
            IndexMetaData indexMetaData = getLocalClusterState(client).metaData().indices().get("test");
            assertThat(indexMetaData.getState(), equalTo(IndexMetaData.State.CLOSE));
        }
    }

    @Test
    public void testCloseIndexNoAcknowledgement() {
        createIndex("test");
        ensureGreen();

        CloseIndexResponse closeIndexResponse= client().admin().indices().prepareClose("test").setTimeout("0s").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(false));
    }

    @Test
    public void testOpenIndexAcknowledgement() {
        createIndex("test");
        ensureGreen();

        assertAcked(client().admin().indices().prepareClose("test"));

        assertAcked(client().admin().indices().prepareOpen("test"));

        for (Client client : clients()) {
            IndexMetaData indexMetaData = getLocalClusterState(client).metaData().indices().get("test");
            assertThat(indexMetaData.getState(), equalTo(IndexMetaData.State.OPEN));
        }
    }

    @Test
    public void testOpenIndexNoAcknowledgement() {
        createIndex("test");
        ensureGreen();

        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));

        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen("test").setTimeout("0s").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(false));
    }

    @Test
    public void testPutMappingAcknowledgement() {
        createIndex("test");
        ensureGreen();

        assertAcked(client().admin().indices().preparePutMapping("test").setType("test").setSource("field", "type=string,index=not_analyzed"));

        for (Client client : clients()) {
            assertThat(getLocalClusterState(client).metaData().indices().get("test").mapping("test"), notNullValue());
        }
    }

    @Test
    public void testPutMappingNoAcknowledgement() {
        createIndex("test");
        ensureGreen();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("test").setSource("field", "type=string,index=not_analyzed").setTimeout("0s").get();
        assertThat(putMappingResponse.isAcknowledged(), equalTo(false));
    }

    private static ClusterState getLocalClusterState(Client client) {
        return client.admin().cluster().prepareState().setLocal(true).get().getState();
    }
}
