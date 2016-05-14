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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.elasticsearch.test.store.MockFSIndexStore;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(numDataNodes = 0, scope = Scope.TEST)
public class RecoveryFromGatewayIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockFSIndexStore.TestPlugin.class);
    }

    public void testOneNodeRecoverFromGateway() throws Exception {

        internalCluster().startNode();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("appAccountIds").field("type", "text").endObject().endObject()
            .endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));

        client().prepareIndex("test", "type1", "10990239").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990473").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990513").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990695").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "11026351").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();

        refresh();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
        ensureYellow("test"); // wait for primary allocations here otherwise if we have a lot of shards we might have a
        // shard that is still in post recovery when we restart and the ensureYellow() below will timeout

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);
        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
    }

    private Map<String, long[]> assertAndCapturePrimaryTerms(Map<String, long[]> previousTerms) {
        if (previousTerms == null) {
            previousTerms = new HashMap<>();
        }
        final Map<String, long[]> result = new HashMap<>();
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        for (ObjectCursor<IndexMetaData> cursor : state.metaData().indices().values()) {
            final IndexMetaData indexMetaData = cursor.value;
            final String index = indexMetaData.getIndex().getName();
            final long[] previous = previousTerms.get(index);
            final long[] current = IntStream.range(0, indexMetaData.getNumberOfShards()).mapToLong(indexMetaData::primaryTerm).toArray();
            if (previous == null) {
                result.put(index, current);
            } else {
                assertThat("number of terms changed for index [" + index + "]", current.length, equalTo(previous.length));
                for (int shard = 0; shard < current.length; shard++) {
                    assertThat("primary term didn't increase for [" + index + "][" + shard + "]", current[shard], greaterThan(previous[shard]));
                }
                result.put(index, current);
            }
        }

        return result;
    }

    public void testSingleNodeNoFlush() throws Exception {
        internalCluster().startNode();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("field").field("type", "text").endObject().startObject("num").field("type", "integer").endObject().endObject()
            .endObject().endObject().string();
        // note: default replica settings are tied to #data nodes-1 which is 0 here. We can do with 1 in this test.
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test").setSettings(
            SETTING_NUMBER_OF_SHARDS, numberOfShards(),
            SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1)
        ).addMapping("type1", mapping));

        int value1Docs;
        int value2Docs;
        boolean indexToAllShards = randomBoolean();

        if (indexToAllShards) {
            // insert enough docs so all shards will have a doc
            value1Docs = randomIntBetween(numberOfShards * 10, numberOfShards * 20);
            value2Docs = randomIntBetween(numberOfShards * 10, numberOfShards * 20);

        } else {
            // insert a two docs, some shards will not have anything
            value1Docs = 1;
            value2Docs = 1;
        }


        for (int i = 0; i < 1 + randomInt(100); i++) {
            for (int id = 0; id < Math.max(value1Docs, value2Docs); id++) {
                if (id < value1Docs) {
                    index("test", "type1", "1_" + id,
                        jsonBuilder().startObject().field("field", "value1").startArray("num").value(14).value(179).endArray().endObject()
                    );
                }
                if (id < value2Docs) {
                    index("test", "type1", "2_" + id,
                        jsonBuilder().startObject().field("field", "value2").startArray("num").value(14).endArray().endObject()
                    );
                }
            }

        }

        refresh();

        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }
        if (!indexToAllShards) {
            // we have to verify primaries are started for them to be restored
            logger.info("Ensure all primaries have been started");
            ensureYellow();
        }

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }

        internalCluster().fullRestart();


        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }
    }

    public void testSingleNodeWithFlush() throws Exception {
        internalCluster().startNode();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        flush();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        refresh();

        assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);

        ensureYellow("test"); // wait for primary allocations here otherwise if we have a lot of shards we might have a
        // shard that is still in post recovery when we restart and the ensureYellow() below will timeout

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    public void testTwoNodeFirstNodeCleared() throws Exception {
        final String firstNode = internalCluster().startNode();
        internalCluster().startNode();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        flush();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        refresh();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.builder().put("gateway.recover_after_nodes", 2).build();
            }

            @Override
            public boolean clearData(String nodeName) {
                return firstNode.equals(nodeName);
            }

        });

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureGreen();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    public void testLatestVersionLoaded() throws Exception {
        // clean two nodes
        internalCluster().startNodesAsync(2, Settings.builder().put("gateway.recover_after_nodes", 2).build()).get();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        String metaDataUuid = client().admin().cluster().prepareState().execute().get().getState().getMetaData().clusterUUID();
        assertThat(metaDataUuid, not(equalTo("_na_")));

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        logger.info("--> closing first node, and indexing more data to the second node");
        internalCluster().fullRestart(new RestartCallback() {

            @Override
            public void doAfterNodes(int numNodes, Client client) throws Exception {
                if (numNodes == 1) {
                    logger.info("--> one node is closed - start indexing data into the second one");
                    client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).execute().actionGet();
                    // TODO: remove once refresh doesn't fail immediately if there a master block:
                    // https://github.com/elastic/elasticsearch/issues/9997
                    client.admin().cluster().prepareHealth("test").setWaitForYellowStatus().get();
                    client.admin().indices().prepareRefresh().execute().actionGet();

                    for (int i = 0; i < 10; i++) {
                        assertHitCount(client.prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
                    }

                    logger.info("--> add some metadata, additional type and template");
                    client.admin().indices().preparePutMapping("test").setType("type2")
                        .setSource(jsonBuilder().startObject().startObject("type2").endObject().endObject())
                        .execute().actionGet();
                    client.admin().indices().preparePutTemplate("template_1")
                        .setTemplate("te*")
                        .setOrder(0)
                        .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                            .startObject("field1").field("type", "text").field("store", true).endObject()
                            .startObject("field2").field("type", "keyword").field("store", true).endObject()
                            .endObject().endObject().endObject())
                        .execute().actionGet();
                    client.admin().indices().prepareAliases().addAlias("test", "test_alias", QueryBuilders.termQuery("field", "value")).execute().actionGet();
                    logger.info("--> starting two nodes back, verifying we got the latest version");
                }

            }

        });

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        assertThat(client().admin().cluster().prepareState().execute().get().getState().getMetaData().clusterUUID(), equalTo(metaDataUuid));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metaData().index("test").mapping("type2"), notNullValue());
        assertThat(state.metaData().templates().get("template_1").template(), equalTo("te*"));
        assertThat(state.metaData().index("test").getAliases().get("test_alias"), notNullValue());
        assertThat(state.metaData().index("test").getAliases().get("test_alias").filter(), notNullValue());
    }

    public void testReusePeerRecovery() throws Exception {
        final Settings settings = Settings.builder()
            .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
            .put("gateway.recover_after_nodes", 4)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 4)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4)
            .put(MockFSDirectoryService.CRASH_INDEX_SETTING.getKey(), false).build();

        internalCluster().startNodesAsync(4, settings).get();
        // prevent any rebalance actions during the peer recovery
        // if we run into a relocation the reuse count will be 0 and this fails the test. We are testing here if
        // we reuse the files on disk after full restarts for replicas.
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put(indexSettings())
            .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)));
        ensureGreen();
        logger.info("--> indexing docs");
        for (int i = 0; i < 1000; i++) {
            client().prepareIndex("test", "type").setSource("field", "value").execute().actionGet();
            if ((i % 200) == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }
        if (randomBoolean()) {
            client().admin().indices().prepareFlush().execute().actionGet();
        }
        logger.info("Running Cluster Health");
        ensureGreen();
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(100).get(); // just wait for merges
        client().admin().indices().prepareFlush().setWaitIfOngoing(true).setForce(true).get();

        boolean useSyncIds = randomBoolean();
        if (useSyncIds == false) {
            logger.info("--> disabling allocation while the cluster is shut down");

            // Disable allocations while we are closing nodes
            client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                    .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE))
                .get();
            logger.info("--> full cluster restart");
            internalCluster().fullRestart();

            logger.info("--> waiting for cluster to return to green after first shutdown");
            ensureGreen();
        } else {
            logger.info("--> trying to sync flush");
            assertEquals(client().admin().indices().prepareSyncedFlush("test").get().failedShards(), 0);
            assertSyncIdsNotNull();
        }

        logger.info("--> disabling allocation while the cluster is shut down{}", useSyncIds ? "" : " a second time");
        // Disable allocations while we are closing nodes
        client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE))
            .get();

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        logger.info("--> full cluster restart");
        internalCluster().fullRestart();

        logger.info("--> waiting for cluster to return to green after {}shutdown", useSyncIds ? "" : "second ");
        ensureGreen();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        if (useSyncIds) {
            assertSyncIdsNotNull();
        }
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            long recovered = 0;
            for (RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                if (file.name().startsWith("segments")) {
                    recovered += file.length();
                }
            }
            if (!recoveryState.getPrimary() && (useSyncIds == false)) {
                logger.info("--> replica shard {} recovered from {} to {}, recovered {}, reuse {}",
                    recoveryState.getShardId().getId(), recoveryState.getSourceNode().getName(), recoveryState.getTargetNode().getName(),
                    recoveryState.getIndex().recoveredBytes(), recoveryState.getIndex().reusedBytes());
                assertThat("no bytes should be recovered", recoveryState.getIndex().recoveredBytes(), equalTo(recovered));
                assertThat("data should have been reused", recoveryState.getIndex().reusedBytes(), greaterThan(0L));
                // we have to recover the segments file since we commit the translog ID on engine startup
                assertThat("all bytes should be reused except of the segments file", recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes() - recovered));
                assertThat("no files should be recovered except of the segments file", recoveryState.getIndex().recoveredFileCount(), equalTo(1));
                assertThat("all files should be reused except of the segments file", recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount() - 1));
                assertThat("> 0 files should be reused", recoveryState.getIndex().reusedFileCount(), greaterThan(0));
            } else {
                if (useSyncIds && !recoveryState.getPrimary()) {
                    logger.info("--> replica shard {} recovered from {} to {} using sync id, recovered {}, reuse {}",
                        recoveryState.getShardId().getId(), recoveryState.getSourceNode().getName(), recoveryState.getTargetNode().getName(),
                        recoveryState.getIndex().recoveredBytes(), recoveryState.getIndex().reusedBytes());
                }
                assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(0L));
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes()));
                assertThat(recoveryState.getIndex().recoveredFileCount(), equalTo(0));
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount()));
            }
        }
    }

    public void assertSyncIdsNotNull() {
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    public void testRecoveryDifferentNodeOrderStartup() throws Exception {
        // we need different data paths so we make sure we start the second node fresh

        final String node_1 = internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()).build());

        client().prepareIndex("test", "type1", "1").setSource("field", "value").execute().actionGet();

        internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()).build());

        ensureGreen();
        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);


        internalCluster().fullRestart(new RestartCallback() {

            @Override
            public boolean doRestart(String nodeName) {
                return !node_1.equals(nodeName);
            }
        });

        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 1);
    }

    public void testStartedShardFoundIfStateNotYetProcessed() throws Exception {
        // nodes may need to report the shards they processed the initial recovered cluster state from the master
        final String nodeName = internalCluster().startNode();
        assertAcked(prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        final Index index = resolveIndex("test");
        final ShardId shardId = new ShardId(index, 0);
        index("test", "type", "1");
        flush("test");

        final boolean corrupt = randomBoolean();

        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // make sure state is not recovered
                return Settings.builder().put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 2).build();
            }
        });

        if (corrupt) {
            for (Path path : internalCluster().getInstance(NodeEnvironment.class, nodeName).availableShardPaths(shardId)) {
                final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
                if (Files.exists(indexPath)) { // multi data path might only have one path in use
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                        for (Path item : stream) {
                            if (item.getFileName().toString().startsWith("segments_")) {
                                logger.debug("--> deleting [{}]", item);
                                Files.delete(item);
                            }
                        }
                    }
                }

            }
        }

        DiscoveryNode node = internalCluster().getInstance(ClusterService.class, nodeName).localNode();

        TransportNodesListGatewayStartedShards.NodesGatewayStartedShards response;
        response = internalCluster().getInstance(TransportNodesListGatewayStartedShards.class)
            .execute(new TransportNodesListGatewayStartedShards.Request(shardId, new String[]{node.getId()}))
            .get();

        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodes().get(0).allocationId(), notNullValue());
        if (corrupt) {
            assertThat(response.getNodes().get(0).storeException(), notNullValue());
        } else {
            assertThat(response.getNodes().get(0).storeException(), nullValue());
        }

        // start another node so cluster consistency checks won't time out due to the lack of state
        internalCluster().startNode();
    }
}
