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

package org.elasticsearch.test.integration.gateway.local;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class LocalGatewayIndexStateTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(LocalGatewayIndexStateTests.class);

    @AfterMethod
    public void cleanAndCloseNodes() throws Exception {
        for (int i = 0; i < 10; i++) {
            if (node("node" + i) != null) {
                node("node" + i).stop();
                // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
                if (((InternalNode) node("node" + i)).injector().getInstance(NodeEnvironment.class).hasNodeFile()) {
                    ((InternalNode) node("node" + i)).injector().getInstance(Gateway.class).reset();
                }
            }
        }
        closeAllNodes();
    }

    @Test
    public void testMappingMetaDataParsed() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local"));
        buildNode("node2", settingsBuilder().put("gateway.type", "local"));
        cleanAndCloseNodes();

        logger.info("--> starting 1 nodes");
        startNode("node1", settingsBuilder().put("gateway.type", "local"));

        logger.info("--> creating test index, with meta routing");
        client("node1").admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_routing").field("required", true).endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> waiting for yellow status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForActiveShards(5).setWaitForYellowStatus().execute().actionGet();
        if (health.timedOut()) {
            ClusterStateResponse response = client("node1").admin().cluster().prepareState().execute().actionGet();
            System.out.println("" + response);
        }
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify meta _routing required exists");
        MappingMetaData mappingMd = client("node1").admin().cluster().prepareState().execute().actionGet().state().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));

        logger.info("--> close node");
        closeNode("node1");

        logger.info("--> starting node again...");
        startNode("node1", settingsBuilder().put("gateway.type", "local"));

        logger.info("--> waiting for yellow status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForActiveShards(5).setWaitForYellowStatus().execute().actionGet();
        if (health.timedOut()) {
            ClusterStateResponse response = client("node1").admin().cluster().prepareState().execute().actionGet();
            System.out.println("" + response);
        }
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify meta _routing required exists");
        mappingMd = client("node1").admin().cluster().prepareState().execute().actionGet().state().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));
    }

    @Test
    public void testSimpleOpenClose() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 2 nodes");
        startNode("node1", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        startNode("node2", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> creating test index");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.state().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.state().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();

        logger.info("--> closing test index...");
        client("node1").admin().indices().prepareClose("test").execute().actionGet();

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> verifying that the state is green");
        health = client("node1").admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> trying to index into a closed index ...");
        try {
            client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }

        logger.info("--> creating another index (test2) by indexing into it");
        client("node1").prepareIndex("test2", "type1", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> verifying that the state is green");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> opening the first index again...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> verifying that the state is green");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.state().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.state().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> trying to get the indexed document on the first index");
        GetResponse getResponse = client("node1").prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));

        logger.info("--> closing test index...");
        client("node1").admin().indices().prepareClose("test").execute().actionGet();
        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> closing nodes...");
        closeNode("node2");
        closeNode("node1");

        logger.info("--> starting nodes again...");
        startNode("node1", settingsBuilder().put("gateway.type", "local").build());
        startNode("node2", settingsBuilder().put("gateway.type", "local").build());

        logger.info("--> waiting for two nodes and green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }

        logger.info("--> opening index...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.state().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.state().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> trying to get the indexed document on the first round (before close and shutdown)");
        getResponse = client("node1").prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "2").setSource("field1", "value1").execute().actionGet();
    }

    @Test
    public void testJustMasterNode() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 1 master node non data");
        startNode("node1", settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> create an index");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> closing master node");
        closeNode("node1");

        logger.info("--> starting 1 master node non data again");
        startNode("node1", settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setIndices("test").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify we have an index");
        ClusterStateResponse clusterStateResponse = client("node1").admin().cluster().prepareState().setFilterIndices("test").execute().actionGet();
        assertThat(clusterStateResponse.state().metaData().hasIndex("test"), equalTo(true));
    }

    @Test
    public void testJustMasterNodeAndJustDataNode() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 1 master node non data");
        startNode("node1", settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        startNode("node2", settingsBuilder().put("node.master", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> create an index");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setIndices("test").setWaitForYellowStatus().execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        client("node1").prepareIndex("test", "type1").setSource("field1", "value1").setTimeout("100ms").execute().actionGet();
    }

    @Test
    public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 2 nodes");
        startNode("node1", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());
        startNode("node2", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }

        logger.info("--> closing test index...");
        client("node1").admin().indices().prepareClose("test").execute().actionGet();


        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> opening the index...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }
    }

    @Test
    public void testDanglingIndicesAutoImportYes() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "yes")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting two nodes");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }
        assertThat(client("node1").prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(true));

        logger.info("--> shutting down the nodes");
        Gateway gateway1 = ((InternalNode) node("node1")).injector().getInstance(Gateway.class);
        closeNode("node1");
        closeNode("node2");

        logger.info("--> deleting the data for the first node");
        gateway1.reset();

        logger.info("--> start the 2 nodes back, simulating dangling index (exists on second, doesn't exists on first)");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        // we need to wait for the allocate dangled to kick in
        Thread.sleep(500);

        logger.info("--> verify that the dangling index exists");
        assertThat(client("node1").admin().indices().prepareExists("test").execute().actionGet().exists(), equalTo(true));
        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify the doc is there");
        assertThat(client("node1").prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesAutoImportClose() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "closed")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting two nodes");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }
        assertThat(client("node1").prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(true));

        logger.info("--> shutting down the nodes");
        Gateway gateway1 = ((InternalNode) node("node1")).injector().getInstance(Gateway.class);
        closeNode("node1");
        closeNode("node2");

        logger.info("--> deleting the data for the first node");
        gateway1.reset();

        logger.info("--> start the 2 nodes back, simulating dangling index (exists on second, doesn't exists on first)");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        // we need to wait for the allocate dangled to kick in
        Thread.sleep(500);

        logger.info("--> verify that the dangling index exists");
        assertThat(client("node1").admin().indices().prepareExists("test").execute().actionGet().exists(), equalTo(true));
        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify the index state is closed");
        assertThat(client("node1").admin().cluster().prepareState().execute().actionGet().state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        logger.info("--> open the index");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();
        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify the doc is there");
        assertThat(client("node1").prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesNoAutoImport() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "no")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting two nodes");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }
        assertThat(client("node1").prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(true));

        logger.info("--> shutting down the nodes");
        Gateway gateway1 = ((InternalNode) node("node1")).injector().getInstance(Gateway.class);
        closeNode("node1");
        closeNode("node2");

        logger.info("--> deleting the data for the first node");
        gateway1.reset();

        logger.info("--> start the 2 nodes back, simulating dangling index (exists on second, doesn't exists on first)");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        // we need to wait for the allocate dangled to kick in (even though in this case its disabled)
        // just to make sure
        Thread.sleep(500);

        logger.info("--> verify that the dangling index does not exists");
        assertThat(client("node1").admin().indices().prepareExists("test").execute().actionGet().exists(), equalTo(false));

        logger.info("--> shutdown the nodes");
        closeNode("node1");
        closeNode("node2");

        logger.info("--> start the nodes back, but make sure we do recovery only after we have 2 nodes in the cluster");
        startNode("node1", settingsBuilder().put(settings).put("gateway.recover_after_nodes", 2).build());
        startNode("node2", settingsBuilder().put(settings).put("gateway.recover_after_nodes", 2).build());

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify that the dangling index does exists now!");
        assertThat(client("node1").admin().indices().prepareExists("test").execute().actionGet().exists(), equalTo(true));
        logger.info("--> verify the doc is there");
        assertThat(client("node1").prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesNoAutoImportStillDanglingAndCreatingSameIndex() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "no")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();

        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting two nodes");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }

        logger.info("--> shutting down the nodes");
        Gateway gateway1 = ((InternalNode) node("node1")).injector().getInstance(Gateway.class);
        closeNode("node1");
        closeNode("node2");

        logger.info("--> deleting the data for the first node");
        gateway1.reset();

        logger.info("--> start the 2 nodes back, simulating dangling index (exists on second, doesn't exists on first)");
        startNode("node1", settings);
        startNode("node2", settings);

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify that the dangling index does not exists");
        assertThat(client("node1").admin().indices().prepareExists("test").execute().actionGet().exists(), equalTo(false));

        logger.info("--> close the first node, so we remain with the second that has the dangling index");
        closeNode("node1");

        logger.info("--> index a different doc");
        client("node2").prepareIndex("test", "type1", "2").setSource("field1", "value2").setRefresh(true).execute().actionGet();

        assertThat(client("node2").prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
        assertThat(client("node2").prepareGet("test", "type1", "2").execute().actionGet().exists(), equalTo(true));
    }
}
