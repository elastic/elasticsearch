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

import org.apache.logging.log4j.LogManager;
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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class GatewayIndexStateIT extends ESIntegTestCase {

    private final Logger logger = LogManager.getLogger(GatewayIndexStateIT.class);

    @Override
    protected boolean addMockInternalEngine() {
        // testRecoverBrokenIndexMetadata replies on the flushing on shutdown behavior which can be randomly disabled in MockInternalEngine.
        return false;
    }

    public void testMappingMetadataParsed() throws Exception {
        logger.info("--> starting 1 nodes");
        internalCluster().startNode();

        logger.info("--> creating test index, with meta routing");
        client().admin().indices().prepareCreate("test")
                .setMapping(XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("_routing")
                    .field("required", true).endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> verify meta _routing required exists");
        MappingMetadata mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metadata()
            .index("test").mapping();
        assertThat(mappingMd.routingRequired(), equalTo(true));

        logger.info("--> restarting nodes...");
        internalCluster().fullRestart();

        logger.info("--> waiting for yellow status");
        ensureYellow();

        logger.info("--> verify meta _routing required exists");
        mappingMd = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test").mapping();
        assertThat(mappingMd.routingRequired(), equalTo(true));
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
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(test.totalNumShards));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();

        logger.info("--> closing test index...");
        assertAcked(client().admin().indices().prepareClose("test"));

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), notNullValue());

        logger.info("--> verifying that the state is green");
        ensureGreen();

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test").setId("1").setSource("field1", "value1").execute().actionGet();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> creating another index (test2) by indexing into it");
        client().prepareIndex("test2").setId("1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> verifying that the state is green");
        ensureGreen();

        logger.info("--> opening the first index again...");
        assertAcked(client().admin().indices().prepareOpen("test"));

        logger.info("--> verifying that the state is green");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(test.totalNumShards));

        logger.info("--> trying to get the indexed document on the first index");
        GetResponse getResponse = client().prepareGet("test", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        logger.info("--> closing test index...");
        assertAcked(client().admin().indices().prepareClose("test"));
        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), notNullValue());

        logger.info("--> restarting nodes...");
        internalCluster().fullRestart();
        logger.info("--> waiting for two nodes and green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), notNullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test").setId("1").setSource("field1", "value1").execute().actionGet();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> opening index...");
        client().admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(test.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(test.totalNumShards));

        logger.info("--> trying to get the indexed document on the first round (before close and shutdown)");
        getResponse = client().prepareGet("test", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("2").setSource("field1", "value1").execute().actionGet();
    }

    public void testJustMasterNode() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        internalCluster().startNode(nonDataNode());

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).execute().actionGet();

        logger.info("--> restarting master node");
        internalCluster().fullRestart(new RestartCallback(){
            @Override
            public Settings onNodeStopped(String nodeName) {
                return nonDataNode();
            }
        });

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setIndices("test")
            .execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify we have an index");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setIndices("test").execute().actionGet();
        assertThat(clusterStateResponse.getState().metadata().hasIndex("test"), equalTo(true));
    }

    public void testJustMasterNodeAndJustDataNode() {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        logger.info("--> create an index");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        client().prepareIndex("test").setSource("field1", "value1").execute().actionGet();
    }

    public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus()
            .setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        }

        logger.info("--> closing test index...");
        assertAcked(client().admin().indices().prepareClose("test"));

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), notNullValue());

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
                assertFalse(indexExists(indexName, client));
                logger.info("--> index deleted");
                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> wait until all nodes are back online");
        client().admin().cluster().health(Requests.clusterHealthRequest().waitForEvents(Priority.LANGUID)
                                              .waitForNodes(Integer.toString(numNodes))).actionGet();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify that the deleted index is removed from the cluster and not reimported as dangling by the restarted node");
        assertFalse(indexExists(indexName));
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
        client().prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
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

        final IndexMetadata metadata = state.getMetadata().index("test");
        final IndexMetadata.Builder brokenMeta = IndexMetadata.builder(metadata).settings(Settings.builder().put(metadata.getSettings())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion().id)
                // this is invalid but should be archived
                .put("index.similarity.BM25.type", "boolean")
                // this one is not validated ahead of time and breaks allocation
                .put("index.analysis.filter.myCollator.type", "icu_collation"));
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(brokenMeta)));

        // check that the cluster does not keep reallocating shards
        assertBusy(() -> {
            final RoutingTable routingTable = client().admin().cluster().prepareState().get().getState().routingTable();
            final IndexRoutingTable indexRoutingTable = routingTable.index("test");
            assertNotNull(indexRoutingTable);
            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                assertTrue(shardRoutingTable.primaryShard().unassigned());
                assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    shardRoutingTable.primaryShard().unassignedInfo().getLastAllocationStatus());
                assertThat(shardRoutingTable.primaryShard().unassignedInfo().getNumFailedAllocations(), greaterThan(0));
            }
        }, 60, TimeUnit.SECONDS);
        client().admin().indices().prepareClose("test").get();

        state = client().admin().cluster().prepareState().get().getState();
        assertEquals(IndexMetadata.State.CLOSE, state.getMetadata().index(metadata.getIndex()).getState());
        assertEquals("boolean", state.getMetadata().index(metadata.getIndex()).getSettings().get("archived.index.similarity.BM25.type"));
        // try to open it with the broken setting - fail again!
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> client().admin().indices().prepareOpen("test").get());
        assertEquals(ex.getMessage(), "Failed to verify index " + metadata.getIndex());
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
            .put("index.analysis.analyzer.test.tokenizer", "standard")
            .put("index.number_of_shards", "1"))
            .setMapping("{\n" +
                "      \"properties\": {\n" +
                "        \"field1\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"analyzer\": \"test\"\n" +
                "        }\n" +
                "      }\n" +
                "  }}").get();
        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value one").setRefreshPolicy(IMMEDIATE).get();
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

        final IndexMetadata metadata = state.getMetadata().index("test");
        final IndexMetadata.Builder brokenMeta = IndexMetadata.builder(metadata).settings(metadata.getSettings()
                .filter((s) -> "index.analysis.analyzer.test.tokenizer".equals(s) == false));
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(brokenMeta)));

        // check that the cluster does not keep reallocating shards
        assertBusy(() -> {
            final RoutingTable routingTable = client().admin().cluster().prepareState().get().getState().routingTable();
            final IndexRoutingTable indexRoutingTable = routingTable.index("test");
            assertNotNull(indexRoutingTable);
            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                assertTrue(shardRoutingTable.primaryShard().unassigned());
                assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    shardRoutingTable.primaryShard().unassignedInfo().getLastAllocationStatus());
                assertThat(shardRoutingTable.primaryShard().unassignedInfo().getNumFailedAllocations(), greaterThan(0));
            }
        }, 60, TimeUnit.SECONDS);
        client().admin().indices().prepareClose("test").get();

        // try to open it with the broken setting - fail again!
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> client().admin().indices().prepareOpen("test").get());
        assertEquals(ex.getMessage(), "Failed to verify index " + metadata.getIndex());
        assertNotNull(ex.getCause());
        assertEquals(MapperParsingException.class, ex.getCause().getClass());
        assertThat(ex.getCause().getMessage(), containsString("analyzer [test] not found for field [field1]"));
    }

    public void testArchiveBrokenClusterSettings() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();
        client().prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
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

        final Metadata metadata = state.getMetadata();
        final Metadata brokenMeta = Metadata.builder(metadata).persistentSettings(Settings.builder()
                .put(metadata.persistentSettings()).put("this.is.unknown", true)
                .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), "broken").build()).build();
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(brokenMeta));

        ensureYellow("test"); // wait for state recovery
        state = client().admin().cluster().prepareState().get().getState();
        assertEquals("true", state.metadata().persistentSettings().get("archived.this.is.unknown"));
        assertEquals("broken", state.metadata().persistentSettings().get("archived."
            + ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()));

        // delete these settings
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull("archived.*")).get();

        state = client().admin().cluster().prepareState().get().getState();
        assertNull(state.metadata().persistentSettings().get("archived.this.is.unknown"));
        assertNull(state.metadata().persistentSettings().get("archived."
            + ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()));
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/48701")
    // This test relates to loading a broken state that was written by a 6.x node, but for now we do not load state from old nodes.
    public void testHalfDeletedIndexImport() throws Exception {
        // It's possible for a 6.x node to add a tombstone for an index but not actually delete the index metadata from disk since that
        // deletion is slightly deferred and may race against the node being shut down; if you upgrade to 7.x when in this state then the
        // node won't start.

        internalCluster().startNode();
        createIndex("test", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        ensureGreen("test");

        final Metadata metadata = internalCluster().getInstance(ClusterService.class).state().metadata();
        final Path[] paths = internalCluster().getInstance(NodeEnvironment.class).nodeDataPaths();
//        writeBrokenMeta(metaStateService -> {
//            metaStateService.writeGlobalState("test", Metadata.builder(metadata)
//                // we remove the manifest file, resetting the term and making this look like an upgrade from 6.x, so must also reset the
//                // term in the coordination metadata
//                .coordinationMetadata(CoordinationMetadata.builder(metadata.coordinationMetadata()).term(0L).build())
//                // add a tombstone but do not delete the index metadata from disk
//               .putCustom(IndexGraveyard.TYPE, IndexGraveyard.builder().addTombstone(metadata.index("test").getIndex()).build()).build());
//            for (final Path path : paths) {
//                try (Stream<Path> stateFiles = Files.list(path.resolve(MetadataStateFormat.STATE_DIR_NAME))) {
//                    for (final Path manifestPath : stateFiles
//                        .filter(p -> p.getFileName().toString().startsWith(Manifest.FORMAT.getPrefix())).collect(Collectors.toList())) {
//                        IOUtils.rm(manifestPath);
//                    }
//                }
//            }
//        });

        ensureGreen();

        assertBusy(() -> assertThat(internalCluster().getInstance(NodeEnvironment.class).availableIndexFolders(), empty()));
    }

    private void restartNodesOnBrokenClusterState(ClusterState.Builder clusterStateBuilder) throws Exception {
        Map<String, PersistedClusterStateService> lucenePersistedStateFactories = Stream.of(internalCluster().getNodeNames())
            .collect(Collectors.toMap(Function.identity(),
                nodeName -> internalCluster().getInstance(PersistedClusterStateService.class, nodeName)));
        final ClusterState clusterState = clusterStateBuilder.build();
        internalCluster().fullRestart(new RestartCallback(){
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                final PersistedClusterStateService lucenePersistedStateFactory = lucenePersistedStateFactories.get(nodeName);
                try (PersistedClusterStateService.Writer writer = lucenePersistedStateFactory.createWriter()) {
                    writer.writeFullStateAndCommit(clusterState.term(), clusterState);
                }
                return super.onNodeStopped(nodeName);
            }
        });
    }
}
