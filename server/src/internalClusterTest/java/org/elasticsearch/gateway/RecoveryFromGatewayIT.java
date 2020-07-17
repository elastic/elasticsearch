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
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ElectionSchedulerFactory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.store.MockFSIndexStore;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.gateway.GatewayService.RECOVER_AFTER_NODES_SETTING;
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
        return Arrays.asList(MockFSIndexStore.TestPlugin.class, InternalSettingsPlugin.class);
    }

    public void testOneNodeRecoverFromGateway() throws Exception {

        internalCluster().startNode();

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("appAccountIds").field("type", "text").endObject()
            .endObject().endObject());
        assertAcked(prepareCreate("test").setMapping(mapping));

        client().prepareIndex("test").setId("10990239").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test").setId("10990473").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test").setId("10990513").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test").setId("10990695").setSource(jsonBuilder().startObject()
            .startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test").setId("11026351").setSource(jsonBuilder().startObject()
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
        for (ObjectCursor<IndexMetadata> cursor : state.metadata().indices().values()) {
            final IndexMetadata indexMetadata = cursor.value;
            final String index = indexMetadata.getIndex().getName();
            final long[] previous = previousTerms.get(index);
            final long[] current = IntStream.range(0, indexMetadata.getNumberOfShards()).mapToLong(indexMetadata::primaryTerm).toArray();
            if (previous == null) {
                result.put(index, current);
            } else {
                assertThat("number of terms changed for index [" + index + "]", current.length, equalTo(previous.length));
                for (int shard = 0; shard < current.length; shard++) {
                    assertThat("primary term didn't increase for [" + index + "][" + shard + "]", current[shard],
                        greaterThan(previous[shard]));
                }
                result.put(index, current);
            }
        }

        return result;
    }

    public void testSingleNodeNoFlush() throws Exception {
        internalCluster().startNode();

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("field").field("type", "text").endObject().startObject("num").field("type", "integer")
            .endObject()
            .endObject().endObject());
        // note: default replica settings are tied to #data nodes-1 which is 0 here. We can do with 1 in this test.
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards())
            .put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))).setMapping(mapping));

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
                    index("test", "1_" + id,
                        jsonBuilder().startObject().field("field", "value1").startArray("num").value(14).value(179).endArray().endObject()
                    );
                }
                if (id < value2Docs) {
                    index("test", "2_" + id,
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
        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute()
            .actionGet();
        flush();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute()
            .actionGet();
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

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute()
            .actionGet();
        flush();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute()
            .actionGet();
        refresh();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(firstNode)).get();

        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .put(RECOVER_AFTER_NODES_SETTING.getKey(), 2)
                    .putList(INITIAL_MASTER_NODES_SETTING.getKey()) // disable bootstrapping
                    .build();
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

        client().execute(ClearVotingConfigExclusionsAction.INSTANCE, new ClearVotingConfigExclusionsRequest()).get();
    }

    public void testLatestVersionLoaded() throws Exception {
        // clean two nodes
        List<String> nodes = internalCluster().startNodes(2, Settings.builder().put("gateway.recover_after_nodes", 2).build());
        Settings node1DataPathSettings = internalCluster().dataPathSettings(nodes.get(0));
        Settings node2DataPathSettings = internalCluster().dataPathSettings(nodes.get(1));

        assertAcked(client().admin().indices().prepareCreate("test"));
        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute()
            .actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute()
            .actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        String metadataUuid = client().admin().cluster().prepareState().execute().get().getState().getMetadata().clusterUUID();
        assertThat(metadataUuid, not(equalTo("_na_")));

        logger.info("--> closing first node, and indexing more data to the second node");
        internalCluster().stopRandomDataNode();

        logger.info("--> one node is closed - start indexing data into the second one");
        client().prepareIndex("test").setId("3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).execute()
            .actionGet();
        // TODO: remove once refresh doesn't fail immediately if there a master block:
        // https://github.com/elastic/elasticsearch/issues/9997
        // client().admin().cluster().prepareHealth("test").setWaitForYellowStatus().get();
        logger.info("--> refreshing all indices after indexing is complete");
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> checking if documents exist, there should be 3");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        logger.info("--> add some metadata and additional template");
        client().admin().indices().preparePutTemplate("template_1")
            .setPatterns(Collections.singletonList("te*"))
            .setOrder(0)
            .setMapping(XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties")
                .startObject("field1").field("type", "text").field("store", true).endObject()
                .startObject("field2").field("type", "keyword").field("store", true).endObject()
                .endObject().endObject().endObject())
            .execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test", "test_alias", QueryBuilders.termQuery("field", "value")).execute()
            .actionGet();

        logger.info("--> stopping the second node");
        internalCluster().stopRandomDataNode();

        logger.info("--> starting the two nodes back");

        internalCluster().startNodes(
            Settings.builder().put(node1DataPathSettings).put("gateway.recover_after_nodes", 2).build(),
            Settings.builder().put(node2DataPathSettings).put("gateway.recover_after_nodes", 2).build());

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        assertThat(client().admin().cluster().prepareState().execute().get().getState().getMetadata().clusterUUID(), equalTo(metadataUuid));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().templates().get("template_1").patterns(), equalTo(Collections.singletonList("te*")));
        assertThat(state.metadata().index("test").getAliases().get("test_alias"), notNullValue());
        assertThat(state.metadata().index("test").getAliases().get("test_alias").filter(), notNullValue());
    }

    public void testReuseInFileBasedPeerRecovery() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode(nodeSettings(0));

        // create the index with our mapping
        client(primaryNode)
            .admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 1)

                // disable merges to keep segments the same
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)

                // expire retention leases quickly
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            ).get();

        logger.info("--> indexing docs");
        int numDocs = randomIntBetween(1, 1024);
        for (int i = 0; i < numDocs; i++) {
            client(primaryNode).prepareIndex("test").setSource("field", "value").execute().actionGet();
        }

        client(primaryNode).admin().indices().prepareFlush("test").setForce(true).get();

        // start the replica node; we do this after indexing so a file-based recovery is triggered to ensure the files are identical
        final String replicaNode = internalCluster().startDataOnlyNode(nodeSettings(1));
        ensureGreen();

        final RecoveryResponse initialRecoveryReponse = client().admin().indices().prepareRecoveries("test").get();
        final Set<String> files = new HashSet<>();
        for (final RecoveryState recoveryState : initialRecoveryReponse.shardRecoveryStates().get("test")) {
            if (recoveryState.getTargetNode().getName().equals(replicaNode)) {
                for (final RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                    files.add(file.name());
                }
                break;
            }
        }

        logger.info("--> restart replica node");
        boolean softDeleteEnabled = internalCluster().getInstance(IndicesService.class, primaryNode)
            .indexServiceSafe(resolveIndex("test")).getShard(0).indexSettings().isSoftDeleteEnabled();

        int moreDocs = randomIntBetween(1, 1024);
        internalCluster().restartNode(replicaNode, new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // index some more documents; we expect to reuse the files that already exist on the replica
                for (int i = 0; i < moreDocs; i++) {
                    client(primaryNode).prepareIndex("test").setSource("field", "value").execute().actionGet();
                }

                // prevent a sequence-number-based recovery from being possible
                client(primaryNode).admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)
                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), "0s")
                ).get();
                assertBusy(() -> assertThat(client().admin().indices().prepareStats("test").get().getShards()[0]
                    .getRetentionLeaseStats().retentionLeases().leases().size(), equalTo(1)));
                client().admin().indices().prepareFlush("test").setForce(true).get();
                if (softDeleteEnabled) { // We need an extra flush to advance the min_retained_seqno of the SoftDeletesPolicy
                    client().admin().indices().prepareFlush("test").setForce(true).get();
                }
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();

        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
        for (final RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            long recovered = 0;
            long reused = 0;
            int filesRecovered = 0;
            int filesReused = 0;
            for (final RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                if (files.contains(file.name()) == false) {
                    recovered += file.length();
                    filesRecovered++;
                } else {
                    reused += file.length();
                    filesReused++;
                }
            }
            if (recoveryState.getPrimary()) {
                assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(0L));
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes()));
                assertThat(recoveryState.getIndex().recoveredFileCount(), equalTo(0));
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount()));
            } else {
                logger.info("--> replica shard {} recovered from {} to {}, recovered {}, reuse {}",
                    recoveryState.getShardId().getId(), recoveryState.getSourceNode().getName(), recoveryState.getTargetNode().getName(),
                    recoveryState.getIndex().recoveredBytes(), recoveryState.getIndex().reusedBytes());
                assertThat("bytes should have been recovered", recoveryState.getIndex().recoveredBytes(), equalTo(recovered));
                assertThat("data should have been reused", recoveryState.getIndex().reusedBytes(), greaterThan(0L));
                // we have to recover the segments file since we commit the translog ID on engine startup
                assertThat("all existing files should be reused, byte count mismatch", recoveryState.getIndex().reusedBytes(),
                    equalTo(reused));
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes() - recovered));
                assertThat("the segment from the last round of indexing should be recovered", recoveryState.getIndex().recoveredFileCount(),
                    equalTo(filesRecovered));
                assertThat("all existing files should be reused, file count mismatch", recoveryState.getIndex().reusedFileCount(),
                    equalTo(filesReused));
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount() - filesRecovered));
                assertThat("> 0 files should be reused", recoveryState.getIndex().reusedFileCount(), greaterThan(0));
                assertThat("no translog ops should be recovered", recoveryState.getTranslog().recoveredOperations(), equalTo(0));
            }
        }
    }

    public void assertSyncIdsNotNull() {
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    public void testStartedShardFoundIfStateNotYetProcessed() throws Exception {
        // nodes may need to report the shards they processed the initial recovered cluster state from the master
        final String nodeName = internalCluster().startNode();
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build());
        final String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(
            client().admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test"));
        final Index index = resolveIndex("test");
        final ShardId shardId = new ShardId(index, 0);
        indexDoc("test", "1");
        flush("test");

        final boolean corrupt = randomBoolean();

        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // make sure state is not recovered
                return Settings.builder().put(RECOVER_AFTER_NODES_SETTING.getKey(), 2).build();
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
        response = ActionTestUtils.executeBlocking(internalCluster().getInstance(TransportNodesListGatewayStartedShards.class),
            new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, new DiscoveryNode[]{node}));

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

    public void testMessyElectionsStillMakeClusterGoGreen() throws Exception {
        internalCluster().startNodes(3,
            Settings.builder().put(ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING.getKey(),
                "2ms").build());
        createIndex("test", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms").build());
        ensureGreen("test");
        internalCluster().fullRestart();
        ensureGreen("test");
    }
}
