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

package org.elasticsearch.gateway.local;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
@Slow
@TestLogging("action.search:TRACE,index.shard.service:TRACE")
public class LocalGatewayIndexStateTests extends ElasticsearchIntegrationTest {

    private final ESLogger logger = Loggers.getLogger(LocalGatewayIndexStateTests.class);

    @Test
    public void testMappingMetaDataParsed() throws Exception {

        logger.info("--> starting 1 nodes");
        internalCluster().startNode(settingsBuilder().put("gateway.type", "local"));

        logger.info("--> creating test index, with meta routing");
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_routing").field("required", true).endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> waiting for yellow status");
        ensureYellow();

        logger.info("--> verify meta _routing required exists");
        MappingMetaData mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));

        logger.info("--> restarting nodes...");
        internalCluster().fullRestart();

        logger.info("--> waiting for yellow status");
        ensureYellow();

        logger.info("--> verify meta _routing required exists");
        mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));
    }

    @Test
    public void testSimpleOpenClose() throws Exception {

        logger.info("--> starting 2 nodes");
        internalCluster().startNodesAsync(2, settingsBuilder().put("gateway.type", "local").build()).get();

        logger.info("--> creating test index");
        createIndex("test");

        NumShards test = getNumShards("test");

        logger.info("--> waiting for green status");
        ensureGreen();

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(test.totalNumShards));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").execute().actionGet();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> verifying that the state is green");
        ensureGreen();

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> creating another index (test2) by indexing into it");
        client().prepareIndex("test2", "type1", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> verifying that the state is green");
        ensureGreen();

        logger.info("--> opening the first index again...");
        client().admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> verifying that the state is green");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(test.totalNumShards));

        logger.info("--> trying to get the indexed document on the first index");
        GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").execute().actionGet();
        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> restarting nodes...");
        internalCluster().fullRestart();
        logger.info("--> waiting for two nodes and green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> opening index...");
        client().admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(test.totalNumShards));

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
        internalCluster().startNode(settingsBuilder().put("node.data", false).put("gateway.type", "local").build());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> closing master node");
        internalCluster().closeNonSharedNodes(false);

        logger.info("--> starting 1 master node non data again");
        internalCluster().startNode(settingsBuilder().put("node.data", false).put("gateway.type", "local").build());

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setIndices("test").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify we have an index");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setIndices("test").execute().actionGet();
        assertThat(clusterStateResponse.getState().metaData().hasIndex("test"), equalTo(true));
    }

    @Test
    public void testJustMasterNodeAndJustDataNode() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        internalCluster().startNode(settingsBuilder().put("node.data", false).put("gateway.type", "local").build());
        internalCluster().startNode(settingsBuilder().put("node.master", false).put("gateway.type", "local").build());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for test index to be created");
        ensureYellow();

        client().prepareIndex("test", "type1").setSource("field1", "value1").setTimeout("100ms").execute().actionGet();
    }

    @Test
    public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 2 nodes");
        internalCluster().startNode(settingsBuilder().put("gateway.type", "local").build());
        internalCluster().startNode(settingsBuilder().put("gateway.type", "local").build());

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1l);
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
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1l);
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1l);
        }
    }

    @Test
    public void testDanglingIndicesAutoImportYes() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "yes")
                .build();
        logger.info("--> starting two nodes");
        final String node_1 = internalCluster().startNode(settings);
        internalCluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1l);
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> restarting the nodes");
        final Gateway gateway1 = internalCluster().getInstance(Gateway.class, node_1);
        internalCluster().fullRestart(new RestartCallback() {
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
        ensureGreen();

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
        ensureGreen();

        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesAutoImportClose() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "closed")
                .build();


        logger.info("--> starting two nodes");
        final String node_1 = internalCluster().startNode(settings);
        internalCluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1l);
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> restarting the nodes");
        final Gateway gateway1 = internalCluster().getInstance(Gateway.class, node_1);
        internalCluster().fullRestart(new RestartCallback() {
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
        ensureGreen();

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
        ensureGreen();

        logger.info("--> verify the index state is closed");
        assertThat(client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        logger.info("--> open the index");
        assertAcked(client().admin().indices().prepareOpen("test").get());
        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesNoAutoImport() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "no")
                .build();
        logger.info("--> starting two nodes");
        final String node_1 = internalCluster().startNodesAsync(2, settings).get().get(0);
        internalCluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 1l);
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> restarting the nodes");
        final Gateway gateway1 = internalCluster().getInstance(Gateway.class, node_1);
        internalCluster().fullRestart(new RestartCallback() {

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
        ensureGreen();

        // we need to wait for the allocate dangled to kick in (even though in this case its disabled)
        // just to make sure
        Thread.sleep(500);

        logger.info("--> verify that the dangling index does not exists");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(false));

        logger.info("--> restart start the nodes, but make sure we do recovery only after we have 2 nodes in the cluster");
        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return settingsBuilder().put("gateway.recover_after_nodes", 2).build();
            }
        });

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify that the dangling index does exists now!");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    @Test
    public void testDanglingIndicesNoAutoImportStillDanglingAndCreatingSameIndex() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").put("gateway.local.auto_import_dangled", "no")
                .build();

        logger.info("--> starting two nodes");
        final String node_1 = internalCluster().startNode(settings);
        internalCluster().startNode(settings);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1l);
        }

        logger.info("--> restarting the nodes");
        final Gateway gateway1 = internalCluster().getInstance(Gateway.class, node_1);
        internalCluster().fullRestart(new RestartCallback() {

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
        ensureGreen();

        logger.info("--> verify that the dangling index does not exists");
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(false));

        logger.info("--> close the first node, so we remain with the second that has the dangling index");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_1));

        logger.info("--> index a different doc");
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").setRefresh(true).execute().actionGet();

        logger.info("--> verify that doc 2 does exist");
        assertThat(client().prepareGet("test", "type1", "2").execute().actionGet().isExists(), equalTo(true));

        // Need an ensure yellow here, since the index gets created (again) when we index doc2, so the shard that doc
        // with id 1 is assigned to might not be in a started state. We don't need to do this when verifying if doc 2
        // exists, because we index into the shard that doc gets assigned to.
        ensureYellow("test");
        logger.info("--> verify that doc 1 doesn't exist");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
    }
}
