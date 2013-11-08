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

package org.elasticsearch.gateway.local;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.test.*;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.TestCluster.RestartCallback;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
@ClusterScope(scope=Scope.TEST, numNodes=0)
public class LocalGatewayIndexStateTests extends ElasticsearchIntegrationTest {

    private final ESLogger logger = Loggers.getLogger(LocalGatewayIndexStateTests.class);

    @Test
    public void testMappingMetaDataParsed() throws Exception {

        logger.info("--> starting 1 nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local"));

        logger.info("--> creating test index, with meta routing");
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_routing").field("required", true).endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> waiting for yellow status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForActiveShards(5).setWaitForYellowStatus().execute().actionGet();
        if (health.isTimedOut()) {
            ClusterStateResponse response = client().admin().cluster().prepareState().execute().actionGet();
            System.out.println("" + response);
        }
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify meta _routing required exists");
        MappingMetaData mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));

        logger.info("--> restarting nodes...");
        cluster().fullRestart();

        logger.info("--> waiting for yellow status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForActiveShards(5).setWaitForYellowStatus().execute().actionGet();
        if (health.isTimedOut()) {
            ClusterStateResponse response = client().admin().cluster().prepareState().execute().actionGet();
            System.out.println("" + response);
        }
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify meta _routing required exists");
        mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));
    }

    @Test
    public void testSimpleOpenClose() throws Exception {

        logger.info("--> starting 2 nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> creating test index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").execute().actionGet();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> verifying that the state is green");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }

        logger.info("--> creating another index (test2) by indexing into it");
        client().prepareIndex("test2", "type1", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> verifying that the state is green");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> opening the first index again...");
        client().admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> verifying that the state is green");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> trying to get the indexed document on the first index");
        GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").execute().actionGet();
        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> restarting nodes...");
        cluster().fullRestart();
        logger.info("--> waiting for two nodes and green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }

        logger.info("--> opening index...");
        client().admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> trying to get the indexed document on the first round (before close and shutdown)");
        getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "2").setSource("field1", "value1").execute().actionGet();
    }

    @Test
    public void testJustMasterNode() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        cluster().startNode(settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> closing master node");
        cluster().closeNonSharedNodes(false);

        logger.info("--> starting 1 master node non data again");
        cluster().startNode(settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setIndices("test").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify we have an index");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setFilterIndices("test").execute().actionGet();
        assertThat(clusterStateResponse.getState().metaData().hasIndex("test"), equalTo(true));
    }

    @Test
    public void testJustMasterNodeAndJustDataNode() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        cluster().startNode(settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        cluster().startNode(settingsBuilder().put("node.master", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setIndices("test").setWaitForYellowStatus().execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        client().prepareIndex("test", "type1").setSource("field1", "value1").setTimeout("100ms").execute().actionGet();
    }

    @Test
    public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 2 nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());
        cluster().startNode(settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));
        }

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").execute().actionGet();


        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> opening the index...");
        client().admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));
        }
    }

    @Test
    public void testDanglingIndicesAutoImportYes() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "yes")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();
        logger.info("--> starting two nodes");
        final String node_1 = cluster().startNode(settings);
        cluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        
        logger.info("--> restarting the nodes");
        final Gateway gateway1 = cluster().getInstance(Gateway.class, node_1);
        cluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                if (node_1.equals(nodeName)) {
                    logger.info("--> deleting the data for the first node");
                    gateway1.reset();
                }
                return null;
            }
        });

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        // spin a bit waiting for the index to exists
        long time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) < TimeValue.timeValueSeconds(10).millis()) {
            if (client().admin().indices().prepareExists("test").execute().actionGet().isExists()) {
                break;
            }
        }

        logger.info("--> verify that the dangling index exists");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesAutoImportClose() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "closed")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();
  

        logger.info("--> starting two nodes");
        final String node_1 = cluster().startNode(settings);
        cluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        
        logger.info("--> restarting the nodes");
        final Gateway gateway1 = cluster().getInstance(Gateway.class, node_1);
        cluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                if (node_1.equals(nodeName)) {
                    logger.info("--> deleting the data for the first node");
                    gateway1.reset();
                }
                return null;
            }
        });

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        // spin a bit waiting for the index to exists
        long time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) < TimeValue.timeValueSeconds(10).millis()) {
            if (client().admin().indices().prepareExists("test").execute().actionGet().isExists()) {
                break;
            }
        }

        logger.info("--> verify that the dangling index exists");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify the index state is closed");
        assertThat(client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        logger.info("--> open the index");
        client().admin().indices().prepareOpen("test").execute().actionGet();
        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesNoAutoImport() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "no")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();
        logger.info("--> starting two nodes");
        final String node_1 = cluster().startNode(settings);
        cluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> restarting the nodes");
        final Gateway gateway1 = cluster().getInstance(Gateway.class, node_1);
        cluster().fullRestart(new RestartCallback() {
            
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                if (node_1.equals(nodeName)) {
                    logger.info("--> deleting the data for the first node");
                    gateway1.reset();
                }
                return null;
            }
        });

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        // we need to wait for the allocate dangled to kick in (even though in this case its disabled)
        // just to make sure
        Thread.sleep(500);

        logger.info("--> verify that the dangling index does not exists");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(false));

        logger.info("--> restart start the nodes, but make sure we do recovery only after we have 2 nodes in the cluster");
        cluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return settingsBuilder().put("gateway.recover_after_nodes", 2).build();
            }
        });

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify that the dangling index does exists now!");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesNoAutoImportStillDanglingAndCreatingSameIndex() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "no")
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                .build();

        logger.info("--> starting two nodes");
        final String node_1 = cluster().startNode(settings);
        cluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));
        }

        logger.info("--> restarting the nodes");
        final Gateway gateway1 = cluster().getInstance(Gateway.class, node_1);
        cluster().fullRestart(new RestartCallback() {
            
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                if (node_1.equals(nodeName)) {
                    logger.info("--> deleting the data for the first node");
                    gateway1.reset();
                }
                return null;
            }
        });

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify that the dangling index does not exists");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(false));

        logger.info("--> close the first node, so we remain with the second that has the dangling index");
        cluster().stopRandomNode(TestCluster.nameFilter(node_1));

        logger.info("--> index a different doc");
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").setRefresh(true).execute().actionGet();

        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        assertThat(client().prepareGet("test", "type1", "2").execute().actionGet().isExists(), equalTo(true));
    }
}
