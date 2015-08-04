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

package org.elasticsearch.gateway;

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
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class GatewayIndexStateIT extends ESIntegTestCase {

    private final ESLogger logger = Loggers.getLogger(GatewayIndexStateIT.class);

    @Test
    public void testMappingMetaDataParsed() throws Exception {

        logger.info("--> starting 1 nodes");
        internalCluster().startNode();

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
        internalCluster().startNodesAsync(2).get();

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
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").get();

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
        internalCluster().startNode(settingsBuilder().put("node.data", false).build());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> closing master node");
        internalCluster().closeNonSharedNodes(false);

        logger.info("--> starting 1 master node non data again");
        internalCluster().startNode(settingsBuilder().put("node.data", false).build());

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
        internalCluster().startNode(settingsBuilder().put("node.data", false).build());
        internalCluster().startNode(settingsBuilder().put("node.master", false).build());

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
        internalCluster().startNodesAsync(2).get();

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
    public void testDanglingIndicesConflictWithAlias() throws Exception {
        logger.info("--> starting two nodes");
        internalCluster().startNodesAsync(2).get();

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1l);
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        internalCluster().stopRandomNonMasterNode();

        // wait for master to processed node left (so delete won't timeout waiting for it)
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("1").get().isTimedOut());

        logger.info("--> deleting index");
        assertAcked(client().admin().indices().prepareDelete("test"));

        index("test2", "type1", "2", "{}");

        logger.info("--> creating index with an alias");
        assertAcked(client().admin().indices().prepareAliases().addAlias("test2", "test"));

        logger.info("--> starting node back up");
        internalCluster().startNode();

        ensureGreen();

        // make sure that any other events were processed
        assertFalse(client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).get().isTimedOut());

        logger.info("--> verify we read the right thing through alias");
        assertThat(client().prepareGet("test", "type1", "2").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> deleting alias");
        assertAcked(client().admin().indices().prepareAliases().removeAlias("test2", "test"));

        logger.info("--> waiting for dangling index to be imported");

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertTrue(client().admin().indices().prepareExists("test").execute().actionGet().isExists());
            }
        });

        ensureGreen();

        logger.info("--> verifying dangling index contains doc");

        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    @Test
    public void testDanglingIndices() throws Exception {
        logger.info("--> starting two nodes");

        final String node_1 = internalCluster().startNodesAsync(2).get().get(0);

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
}
