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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class GatewayIndexStateIT extends ESIntegTestCase {

    private final Logger logger = Loggers.getLogger(GatewayIndexStateIT.class);

    public void testMappingMetaDataParsed() throws Exception {
        logger.info("--> starting 1 nodes");
        internalCluster().startNode();

        logger.info("--> creating test index, with meta routing");
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_routing")
                    .field("required", true).endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> verify meta _routing required exists");
        MappingMetaData mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metaData()
            .index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));

        logger.info("--> restarting nodes...");
        internalCluster().fullRestart();

        logger.info("--> waiting for yellow status");
        ensureYellow();

        logger.info("--> verify meta _routing required exists");
        mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));
    }

    public void testSimpleOpenClose() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        logger.info("--> creating test index");
        createIndex("test");

        NumShards test = getNumShards("test");

        logger.info("--> waiting for green status");
        ensureGreen();

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").getState(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(test.totalNumShards));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").get();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").getState(), equalTo(IndexMetaData.State.CLOSE));
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
        assertThat(stateResponse.getState().metaData().index("test").getState(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(test.totalNumShards));

        logger.info("--> trying to get the indexed document on the first index");
        GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").execute().actionGet();
        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").getState(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> restarting nodes...");
        internalCluster().fullRestart();
        logger.info("--> waiting for two nodes and green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").getState(), equalTo(IndexMetaData.State.CLOSE));
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
        assertThat(stateResponse.getState().metaData().index("test").getState(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(test.totalNumShards));

        logger.info("--> trying to get the indexed document on the first round (before close and shutdown)");
        getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "2").setSource("field1", "value1").execute().actionGet();
    }

    public void testJustMasterNode() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).execute().actionGet();

        logger.info("--> closing master node");
        internalCluster().closeNonSharedNodes(false);

        logger.info("--> starting 1 master node non data again");
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setIndices("test")
            .execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify we have an index");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setIndices("test").execute().actionGet();
        assertThat(clusterStateResponse.getState().metaData().hasIndex("test"), equalTo(true));
    }

    public void testJustMasterNodeAndJustDataNode() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());
        internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).build());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        client().prepareIndex("test", "type1").setSource("field1", "value1").setTimeout("100ms").execute().actionGet();
    }

    public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus()
            .setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        }

        logger.info("--> closing test index...");
        client().admin().indices().prepareClose("test").execute().actionGet();

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").getState(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> opening the index...");
        client().admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2")
            .execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        }
    }

    public void testDanglingIndices() throws Exception {
        logger.info("--> starting two nodes");

        final String node_1 = internalCluster().startNodes(2).get(0);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> restarting the nodes");
        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return node_1.equals(nodeName);
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

    /**
     * This test ensures that when an index deletion takes place while a node is offline, when that
     * node rejoins the cluster, it deletes the index locally instead of importing it as a dangling index.
     */
    public void testIndexDeletionWhenNodeRejoins() throws Exception {
        final String indexName = "test-index-del-on-node-rejoin-idx";
        final int numNodes = 2;

        final List<String> nodes;
        logger.info("--> starting a cluster with " + numNodes + " nodes");
        nodes = internalCluster().startNodes(numNodes,
            Settings.builder().put(IndexGraveyard.SETTING_MAX_TOMBSTONES.getKey(), randomIntBetween(10, 100)).build());
        logger.info("--> create an index");
        createIndex(indexName);

        logger.info("--> waiting for green status");
        ensureGreen();
        final String indexUUID = resolveIndex(indexName).getUUID();

        logger.info("--> restart a random date node, deleting the index in between stopping and restarting");
        internalCluster().restartRandomDataNode(new RestartCallback() {
            @Override
            public Settings onNodeStopped(final String nodeName) throws Exception {
                nodes.remove(nodeName);
                logger.info("--> stopped node[{}], remaining nodes {}", nodeName, nodes);
                assert nodes.size() > 0;
                final String otherNode = nodes.get(0);
                logger.info("--> delete index and verify it is deleted");
                final Client client = client(otherNode);
                client.admin().indices().prepareDelete(indexName).execute().actionGet();
                assertFalse(client.admin().indices().prepareExists(indexName).execute().actionGet().isExists());
                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> wait until all nodes are back online");
        client().admin().cluster().health(Requests.clusterHealthRequest().waitForEvents(Priority.LANGUID)
                                              .waitForNodes(Integer.toString(numNodes))).actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify that the deleted index is removed from the cluster and not reimported as dangling by the restarted node");
        assertFalse(client().admin().indices().prepareExists(indexName).execute().actionGet().isExists());
        assertBusy(() -> {
            final NodeEnvironment nodeEnv = internalCluster().getInstance(NodeEnvironment.class);
            try {
                assertFalse("index folder " + indexUUID + " should be deleted", nodeEnv.availableIndexFolders().contains(indexUUID));
            } catch (IOException e) {
                logger.error("Unable to retrieve available index folders from the node", e);
                fail("Unable to retrieve available index folders from the node");
            }
        });
    }

    /**
     * This test really tests worst case scenario where we have a broken setting or any setting that prevents an index from being
     * allocated in our metadata that we recover. In that case we now have the ability to check the index on local recovery from disk
     * if it is sane and if we can successfully create an IndexService. This also includes plugins etc.
     */
    public void testRecoverBrokenIndexMetadata() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();
        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        logger.info("--> waiting for green status");
        if (usually()) {
            ensureYellow();
        } else {
            internalCluster().startNode();
            client().admin().cluster()
                .health(Requests.clusterHealthRequest()
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true).waitForNodes("2")).actionGet();
        }
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexMetaData metaData = state.getMetaData().index("test");
        for (NodeEnvironment services : internalCluster().getInstances(NodeEnvironment.class)) {
            IndexMetaData brokenMeta = IndexMetaData.builder(metaData).settings(Settings.builder().put(metaData.getSettings())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion().id)
                 // this is invalid but should be archived
                .put("index.similarity.BM25.type", "classic")
                 // this one is not validated ahead of time and breaks allocation
                .put("index.analysis.filter.myCollator.type", "icu_collation")
            ).build();
            IndexMetaData.FORMAT.write(brokenMeta, services.indexPaths(brokenMeta.getIndex()));
        }
        internalCluster().fullRestart();
        // ensureGreen(closedIndex) waits for the index to show up in the metadata
        // this is crucial otherwise the state call below might not contain the index yet
        ensureGreen(metaData.getIndex().getName());
        state = client().admin().cluster().prepareState().get().getState();
        assertEquals(IndexMetaData.State.CLOSE, state.getMetaData().index(metaData.getIndex()).getState());
        assertEquals("classic", state.getMetaData().index(metaData.getIndex()).getSettings().get("archived.index.similarity.BM25.type"));
        // try to open it with the broken setting - fail again!
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> client().admin().indices().prepareOpen("test").get());
        assertEquals(ex.getMessage(), "Failed to verify index " + metaData.getIndex());
        assertNotNull(ex.getCause());
        assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
        assertEquals(ex.getCause().getMessage(), "Unknown filter type [icu_collation] for [myCollator]");
    }

    /**
     * This test really tests worst case scenario where we have a missing analyzer setting.
     * In that case we now have the ability to check the index on local recovery from disk
     * if it is sane and if we can successfully create an IndexService.
     * This also includes plugins etc.
     */
    public void testRecoverMissingAnalyzer() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();
        prepareCreate("test").setSettings(Settings.builder()
            .put("index.analysis.analyzer.test.tokenizer", "keyword")
            .put("index.number_of_shards", "1"))
            .addMapping("type1", "{\n" +
                "    \"type1\": {\n" +
                "      \"properties\": {\n" +
                "        \"field1\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"analyzer\": \"test\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }}", XContentType.JSON).get();
        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value one").setRefreshPolicy(IMMEDIATE).get();
        logger.info("--> waiting for green status");
        if (usually()) {
            ensureYellow();
        } else {
            internalCluster().startNode();
            client().admin().cluster()
                .health(Requests.clusterHealthRequest()
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true).waitForNodes("2")).actionGet();
        }
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexMetaData metaData = state.getMetaData().index("test");
        for (NodeEnvironment services : internalCluster().getInstances(NodeEnvironment.class)) {
            IndexMetaData brokenMeta = IndexMetaData.builder(metaData).settings(metaData.getSettings()
                .filter((s) -> "index.analysis.analyzer.test.tokenizer".equals(s) == false)).build();
            IndexMetaData.FORMAT.write(brokenMeta, services.indexPaths(brokenMeta.getIndex()));
        }
        internalCluster().fullRestart();
        // ensureGreen(closedIndex) waits for the index to show up in the metadata
        // this is crucial otherwise the state call below might not contain the index yet
        ensureGreen(metaData.getIndex().getName());
        state = client().admin().cluster().prepareState().get().getState();
        assertEquals(IndexMetaData.State.CLOSE, state.getMetaData().index(metaData.getIndex()).getState());

        // try to open it with the broken setting - fail again!
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> client().admin().indices().prepareOpen("test").get());
        assertEquals(ex.getMessage(), "Failed to verify index " + metaData.getIndex());
        assertNotNull(ex.getCause());
        assertEquals(MapperParsingException.class, ex.getCause().getClass());
        assertThat(ex.getCause().getMessage(), containsString("analyzer [test] not found for field [field1]"));
    }

    public void testArchiveBrokenClusterSettings() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        logger.info("--> waiting for green status");
        if (usually()) {
            ensureYellow();
        } else {
            internalCluster().startNode();
            client().admin().cluster()
                .health(Requests.clusterHealthRequest()
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true).waitForNodes("2")).actionGet();
        }
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        MetaData metaData = state.getMetaData();
        for (NodeEnvironment nodeEnv : internalCluster().getInstances(NodeEnvironment.class)) {
            MetaData brokenMeta = MetaData.builder(metaData).persistentSettings(Settings.builder()
                .put(metaData.persistentSettings()).put("this.is.unknown", true)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), "broken").build()).build();
            MetaData.FORMAT.write(brokenMeta, nodeEnv.nodeDataPaths());
        }
        internalCluster().fullRestart();
        ensureYellow("test"); // wait for state recovery
        state = client().admin().cluster().prepareState().get().getState();
        assertEquals("true", state.metaData().persistentSettings().get("archived.this.is.unknown"));
        assertEquals("broken", state.metaData().persistentSettings().get("archived."
            + ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey()));

        // delete these settings
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull("archived.*")).get();

        state = client().admin().cluster().prepareState().get().getState();
        assertNull(state.metaData().persistentSettings().get("archived.this.is.unknown"));
        assertNull(state.metaData().persistentSettings().get("archived."
            + ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey()));
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
    }
}
