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

package org.elasticsearch.snapshots;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStage;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.Entry;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.StoredScriptsIT;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.shard.IndexShardTests.getEngineFromShard;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAliasesExist;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAliasesMissing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertIndexTemplateExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertIndexTemplateMissing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class SharedClusterSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestTestPlugin.class,
            StoredScriptsIT.CustomScriptPlugin.class,
            MockRepository.Plugin.class);
    }

    private Settings randomRepoSettings() {
        Settings.Builder repoSettings = Settings.builder();
        repoSettings.put("location", randomRepoPath());
        if (randomBoolean()) {
            repoSettings.put("compress", randomBoolean());
        }
        if (randomBoolean()) {
            repoSettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
        } else {
            if (randomBoolean()) {
                repoSettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
            } else {
                repoSettings.put("chunk_size", (String) null);
            }
        }
        return repoSettings.build();
    }

    public void testBasicWorkFlow() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(randomRepoSettings()));

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertHitCount(client.prepareSearch("test-idx-1").setSize(0).get(), 100L);
        assertHitCount(client.prepareSearch("test-idx-2").setSize(0).get(), 100L);
        assertHitCount(client.prepareSearch("test-idx-3").setSize(0).get(), 100L);

        ActionFuture<FlushResponse> flushResponseFuture = null;
        if (randomBoolean()) {
            ArrayList<String> indicesToFlush = new ArrayList<>();
            for (int i = 1; i < 4; i++) {
                if (randomBoolean()) {
                    indicesToFlush.add("test-idx-" + i);
                }
            }
            if (!indicesToFlush.isEmpty()) {
                String[] indices = indicesToFlush.toArray(new String[indicesToFlush.size()]);
                logger.info("--> starting asynchronous flush for indices {}", Arrays.toString(indices));
                flushResponseFuture = client.admin().indices().prepareFlush(indices).execute();
            }
        }

        final String[] indicesToSnapshot = {"test-idx-*", "-test-idx-3"};

        logger.info("--> capturing history UUIDs");
        final Map<ShardId, String> historyUUIDs = new HashMap<>();
        for (ShardStats shardStats: client().admin().indices().prepareStats(indicesToSnapshot).clear().get().getShards()) {
            String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
            ShardId shardId = shardStats.getShardRouting().shardId();
            if (historyUUIDs.containsKey(shardId)) {
                assertThat(shardStats.getShardRouting() + " has a different history uuid", historyUUID, equalTo(historyUUIDs.get(shardId)));
            } else {
                historyUUIDs.put(shardId, historyUUID);
            }
        }

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices(indicesToSnapshot).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        List<SnapshotInfo> snapshotInfos = client.admin().cluster().prepareGetSnapshots("test-repo")
            .setSnapshots(randomFrom("test-snap", "_all", "*", "*-snap", "test*")).get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        logger.info("--> delete some data");
        for (int i = 0; i < 50; i++) {
            client.prepareDelete("test-idx-1", "doc", Integer.toString(i)).get();
        }
        for (int i = 50; i < 100; i++) {
            client.prepareDelete("test-idx-2", "doc", Integer.toString(i)).get();
        }
        for (int i = 0; i < 100; i += 2) {
            client.prepareDelete("test-idx-3", "doc", Integer.toString(i)).get();
        }
        assertAllSuccessful(refresh());
        assertHitCount(client.prepareSearch("test-idx-1").setSize(0).get(), 50L);
        assertHitCount(client.prepareSearch("test-idx-2").setSize(0).get(), 50L);
        assertHitCount(client.prepareSearch("test-idx-3").setSize(0).get(), 50L);

        logger.info("--> close indices");
        client.admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        for (int i=0; i<5; i++) {
            assertHitCount(client.prepareSearch("test-idx-1").setSize(0).get(), 100L);
            assertHitCount(client.prepareSearch("test-idx-2").setSize(0).get(), 100L);
            assertHitCount(client.prepareSearch("test-idx-3").setSize(0).get(), 50L);
        }

        for (ShardStats shardStats: client().admin().indices().prepareStats(indicesToSnapshot).clear().get().getShards()) {
            String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
            ShardId shardId = shardStats.getShardRouting().shardId();
            assertThat(shardStats.getShardRouting() + " doesn't have a history uuid", historyUUID, notNullValue());
            assertThat(shardStats.getShardRouting() + " doesn't have a new history", historyUUID, not(equalTo(historyUUIDs.get(shardId))));
        }

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-2").execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        for (int i=0; i<5; i++) {
            assertHitCount(client.prepareSearch("test-idx-1").setSize(0).get(), 100L);
        }
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(false));

        for (ShardStats shardStats: client().admin().indices().prepareStats(indicesToSnapshot).clear().get().getShards()) {
            String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
            ShardId shardId = shardStats.getShardRouting().shardId();
            assertThat(shardStats.getShardRouting() + " doesn't have a history uuid", historyUUID, notNullValue());
            assertThat(shardStats.getShardRouting() + " doesn't have a new history", historyUUID, not(equalTo(historyUUIDs.get(shardId))));
        }

        if (flushResponseFuture != null) {
            // Finish flush
            flushResponseFuture.actionGet();
        }
    }

    public void testSingleGetAfterRestore() throws Exception {
        String indexName = "testindex";
        String repoName = "test-restore-snapshot-repo";
        String snapshotName = "test-restore-snapshot";
        String absolutePath = randomRepoPath().toAbsolutePath().toString();
        logger.info("Path [{}]", absolutePath);
        String restoredIndexName = indexName + "-restored";
        String typeName = "actions";
        String expectedValue = "expected";

        Client client = client();
        // Write a document
        String docId = Integer.toString(randomInt());
        index(indexName, typeName, docId, "value", expectedValue);

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
                .setType("fs").setSettings(Settings.builder()
                        .put("location", absolutePath)
                        ));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .setRenamePattern(indexName)
                .setRenameReplacement(restoredIndexName)
                .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareGet(restoredIndexName, typeName, docId).get().isExists(), equalTo(true));
    }

    public void testFreshIndexUUID() {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(randomRepoSettings()));

        createIndex("test");
        String originalIndexUUID = client().admin().indices().prepareGetSettings("test").get().getSetting("test", IndexMetaData.SETTING_INDEX_UUID);
        assertTrue(originalIndexUUID, originalIndexUUID != null);
        assertFalse(originalIndexUUID, originalIndexUUID.equals(IndexMetaData.INDEX_UUID_NA_VALUE));
        ensureGreen();
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        NumShards numShards = getNumShards("test");

        cluster().wipeIndices("test");
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, numShards.numPrimaries)));
        ensureGreen();
        String newIndexUUID = client().admin().indices().prepareGetSettings("test").get().getSetting("test", IndexMetaData.SETTING_INDEX_UUID);
        assertTrue(newIndexUUID, newIndexUUID != null);
        assertFalse(newIndexUUID, newIndexUUID.equals(IndexMetaData.INDEX_UUID_NA_VALUE));
        assertFalse(newIndexUUID, newIndexUUID.equals(originalIndexUUID));
        logger.info("--> close index");
        client.admin().indices().prepareClose("test").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        String newAfterRestoreIndexUUID = client().admin().indices().prepareGetSettings("test").get().getSetting("test", IndexMetaData.SETTING_INDEX_UUID);
        assertTrue("UUID has changed after restore: " + newIndexUUID + " vs. " + newAfterRestoreIndexUUID, newIndexUUID.equals(newAfterRestoreIndexUUID));

        logger.info("--> restore indices with different names");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        String copyRestoreUUID = client().admin().indices().prepareGetSettings("test-copy").get().getSetting("test-copy", IndexMetaData.SETTING_INDEX_UUID);
        assertFalse("UUID has been reused on restore: " + copyRestoreUUID + " vs. " + originalIndexUUID, copyRestoreUUID.equals(originalIndexUUID));
    }

    public void testRestoreWithDifferentMappingsAndSettings() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(randomRepoSettings()));

        logger.info("--> create index with foo type");
        assertAcked(prepareCreate("test-idx", 2, Settings.builder()
                .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        NumShards numShards = getNumShards("test-idx");

        assertAcked(client().admin().indices().preparePutMapping("test-idx").setType("foo").setSource("baz", "type=text"));
        ensureGreen();

        logger.info("--> snapshot it");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete the index and recreate it with bar type");
        cluster().wipeIndices("test-idx");
        assertAcked(prepareCreate("test-idx", 2, Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, numShards.numPrimaries).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 5, TimeUnit.SECONDS)));
        assertAcked(client().admin().indices().preparePutMapping("test-idx").setType("bar").setSource("baz", "type=text"));
        ensureGreen();

        logger.info("--> close index");
        client.admin().indices().prepareClose("test-idx").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        logger.info("--> assert that old mapping is restored");
        ImmutableOpenMap<String, MappingMetaData> mappings = client().admin().cluster().prepareState().get().getState().getMetaData().getIndices().get("test-idx").getMappings();
        assertThat(mappings.get("foo"), notNullValue());
        assertThat(mappings.get("bar"), nullValue());

        logger.info("--> assert that old settings are restored");
        GetSettingsResponse getSettingsResponse = client.admin().indices().prepareGetSettings("test-idx").execute().actionGet();
        assertThat(getSettingsResponse.getSetting("test-idx", "index.refresh_interval"), equalTo("10000ms"));
    }

    public void testEmptySnapshot() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));

        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
    }

    public void testRestoreAliases() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())));

        logger.info("--> create test indices");
        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> create aliases");
        assertAcked(client.admin().indices().prepareAliases()
                .addAlias("test-idx-1", "alias-123")
                .addAlias("test-idx-2", "alias-123")
                .addAlias("test-idx-3", "alias-123")
                .addAlias("test-idx-1", "alias-1")
                .get());
        assertAliasesExist(client.admin().indices().prepareAliasesExist("alias-123").get());

        logger.info("--> snapshot");
        assertThat(client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setIndices().setWaitForCompletion(true).get().getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        logger.info("-->  delete all indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2", "test-idx-3");
        assertAliasesMissing(client.admin().indices().prepareAliasesExist("alias-123", "alias-1").get());

        logger.info("--> restore snapshot with aliases");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        // We don't restore any indices here
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), allOf(greaterThan(0), equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards())));

        logger.info("--> check that aliases are restored");
        assertAliasesExist(client.admin().indices().prepareAliasesExist("alias-123", "alias-1").get());

        logger.info("-->  update aliases");
        assertAcked(client.admin().indices().prepareAliases().removeAlias("test-idx-3", "alias-123"));
        assertAcked(client.admin().indices().prepareAliases().addAlias("test-idx-3", "alias-3"));

        logger.info("-->  delete and close indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        assertAcked(client.admin().indices().prepareClose("test-idx-3"));
        assertAliasesMissing(client.admin().indices().prepareAliasesExist("alias-123", "alias-1").get());

        logger.info("--> restore snapshot without aliases");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setRestoreGlobalState(true).setIncludeAliases(false).execute().actionGet();
        // We don't restore any indices here
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), allOf(greaterThan(0), equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards())));

        logger.info("--> check that aliases are not restored and existing aliases still exist");
        assertAliasesMissing(client.admin().indices().prepareAliasesExist("alias-123", "alias-1").get());
        assertAliasesExist(client.admin().indices().prepareAliasesExist("alias-3").get());

    }

    public void testRestoreTemplates() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())));

        logger.info("-->  creating test template");
        assertThat(client.admin().indices()
            .preparePutTemplate("test-template")
                .setPatterns(Collections.singletonList("te*"))
                .addMapping("test-mapping", XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("test-mapping")
                            .startObject("properties")
                                .startObject("field1")
                                    .field("type", "text")
                                    .field("store", true)
                                .endObject()
                                .startObject("field2")
                                    .field("type", "keyword")
                                    .field("store", true)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject())
            .get().isAcknowledged(), equalTo(true));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setIndices().setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("-->  delete test template");
        assertThat(client.admin().indices().prepareDeleteTemplate("test-template").get().isAcknowledged(), equalTo(true));
        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> restore cluster state");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        // We don't restore any indices here
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        logger.info("--> check that template is restored");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateExists(getIndexTemplatesResponse, "test-template");

    }

    public void testIncludeGlobalState() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path location = randomRepoPath();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", location)));

        boolean testTemplate = randomBoolean();
        boolean testPipeline = randomBoolean();
        boolean testScript = (testTemplate == false && testPipeline == false) || randomBoolean(); // At least something should be stored

        if(testTemplate) {
            logger.info("-->  creating test template");
            assertThat(client.admin().indices()
                .preparePutTemplate("test-template")
                .setPatterns(Collections.singletonList("te*"))
                .addMapping("doc", XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("doc")
                            .startObject("properties")
                                .startObject("field1")
                                    .field("type", "text")
                                    .field("store", true)
                                .endObject()
                                .startObject("field2")
                                    .field("type", "keyword")
                                    .field("store", true)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject())
                .get().isAcknowledged(), equalTo(true));
        }

        if(testPipeline) {
            logger.info("-->  creating test pipeline");
            BytesReference pipelineSource = jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject().bytes();
            assertAcked(client().admin().cluster().preparePutPipeline("barbaz", pipelineSource, XContentType.JSON).get());
        }

        if(testScript) {
            logger.info("-->  creating test script");
            assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("foobar")
                .setContent(new BytesArray(
                    "{\"script\": { \"lang\": \"" + MockScriptEngine.NAME + "\", \"source\": \"1\"} }"), XContentType.JSON));
        }

        logger.info("--> snapshot without global state");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-no-global-state").setIndices().setIncludeGlobalState(false).setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-no-global-state").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> snapshot with global state");
        createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-with-global-state").setIndices().setIncludeGlobalState(true).setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-with-global-state").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        if (testTemplate) {
            logger.info("-->  delete test template");
            cluster().wipeTemplates("test-template");
            GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
            assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");
        }

        if (testPipeline) {
            logger.info("-->  delete test pipeline");
            assertAcked(client().admin().cluster().deletePipeline(new DeletePipelineRequest("barbaz")).get());
        }

        if (testScript) {
            logger.info("-->  delete test script");
            assertAcked(client().admin().cluster().prepareDeleteStoredScript("foobar").get());
        }

        logger.info("--> try restoring cluster state from snapshot without global state");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        logger.info("--> check that template wasn't restored");
        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> restore cluster state");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-with-global-state").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        if (testTemplate) {
            logger.info("--> check that template is restored");
            getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
            assertIndexTemplateExists(getIndexTemplatesResponse, "test-template");
        }

        if (testPipeline) {
            logger.info("--> check that pipeline is restored");
            GetPipelineResponse getPipelineResponse = client().admin().cluster().prepareGetPipeline("barbaz").get();
            assertTrue(getPipelineResponse.isFound());
        }

        if (testScript) {
            logger.info("--> check that script is restored");
            GetStoredScriptResponse getStoredScriptResponse = client().admin().cluster().prepareGetStoredScript("foobar").get();
            assertNotNull(getStoredScriptResponse.getSource());
        }

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot without global state but with indices");
        createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-no-global-state-with-index").setIndices("test-idx").setIncludeGlobalState(false).setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-no-global-state-with-index").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("-->  delete global state and index ");
        cluster().wipeIndices("test-idx");
        if (testTemplate) {
            cluster().wipeTemplates("test-template");
        }
        if (testPipeline) {
            assertAcked(client().admin().cluster().deletePipeline(new DeletePipelineRequest("barbaz")).get());
        }

        if (testScript) {
            assertAcked(client().admin().cluster().prepareDeleteStoredScript("foobar").get());
        }

        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> try restoring index and cluster state from snapshot without global state");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state-with-index").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        logger.info("--> check that global state wasn't restored but index was");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");
        assertFalse(client().admin().cluster().prepareGetPipeline("barbaz").get().isFound());
        assertNull(client().admin().cluster().prepareGetStoredScript("foobar").get().getSource());
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

    }

    public void testSnapshotFileFailureDuringSnapshot() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", randomRepoPath())
                                .put("random", randomAlphaOfLength(10))
                                .put("random_control_io_exception_rate", 0.2))
                .setVerify(false));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        try {
            CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
            if (createSnapshotResponse.getSnapshotInfo().totalShards() == createSnapshotResponse.getSnapshotInfo().successfulShards()) {
                // If we are here, that means we didn't have any failures, let's check it
                assertThat(getFailureCount("test-repo"), equalTo(0L));
            } else {
                assertThat(getFailureCount("test-repo"), greaterThan(0L));
                assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), greaterThan(0));
                for (SnapshotShardFailure shardFailure : createSnapshotResponse.getSnapshotInfo().shardFailures()) {
                    assertThat(shardFailure.reason(), containsString("Random IOException"));
                    assertThat(shardFailure.nodeId(), notNullValue());
                    assertThat(shardFailure.index(), equalTo("test-idx"));
                }
                GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap").get();
                assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));
                SnapshotInfo snapshotInfo = getSnapshotsResponse.getSnapshots().get(0);
                if (snapshotInfo.state() == SnapshotState.SUCCESS) {
                    assertThat(snapshotInfo.shardFailures().size(), greaterThan(0));
                    assertThat(snapshotInfo.totalShards(), greaterThan(snapshotInfo.successfulShards()));
                }
            }
        } catch (Exception ex) {
            logger.info("--> caught a top level exception, asserting what's expected", ex);
            assertThat(getFailureCount("test-repo"), greaterThan(0L));
            assertThat(ExceptionsHelper.detailedMessage(ex), containsString("IOException"));
        }
    }

    public void testDataFileFailureDuringSnapshot() throws Exception {
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", randomRepoPath())
                                .put("random", randomAlphaOfLength(10))
                                .put("random_data_file_io_exception_rate", 0.3)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        if (createSnapshotResponse.getSnapshotInfo().totalShards() == createSnapshotResponse.getSnapshotInfo().successfulShards()) {
            logger.info("--> no failures");
            // If we are here, that means we didn't have any failures, let's check it
            assertThat(getFailureCount("test-repo"), equalTo(0L));
        } else {
            logger.info("--> some failures");
            assertThat(getFailureCount("test-repo"), greaterThan(0L));
            assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), greaterThan(0));
            for (SnapshotShardFailure shardFailure : createSnapshotResponse.getSnapshotInfo().shardFailures()) {
                assertThat(shardFailure.nodeId(), notNullValue());
                assertThat(shardFailure.index(), equalTo("test-idx"));
            }
            GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap").get();
            assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));
            SnapshotInfo snapshotInfo = getSnapshotsResponse.getSnapshots().get(0);
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.PARTIAL));
            assertThat(snapshotInfo.shardFailures().size(), greaterThan(0));
            assertThat(snapshotInfo.totalShards(), greaterThan(snapshotInfo.successfulShards()));

            // Verify that snapshot status also contains the same failures
            SnapshotsStatusResponse snapshotsStatusResponse = client.admin().cluster().prepareSnapshotStatus("test-repo").addSnapshots("test-snap").get();
            assertThat(snapshotsStatusResponse.getSnapshots().size(), equalTo(1));
            SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
            assertThat(snapshotStatus.getIndices().size(), equalTo(1));
            SnapshotIndexStatus indexStatus = snapshotStatus.getIndices().get("test-idx");
            assertThat(indexStatus, notNullValue());
            assertThat(indexStatus.getShardsStats().getFailedShards(), equalTo(snapshotInfo.failedShards()));
            assertThat(indexStatus.getShardsStats().getDoneShards(), equalTo(snapshotInfo.successfulShards()));
            assertThat(indexStatus.getShards().size(), equalTo(snapshotInfo.totalShards()));

            int numberOfFailures = 0;
            for (SnapshotIndexShardStatus shardStatus : indexStatus.getShards().values()) {
                if (shardStatus.getStage() == SnapshotIndexShardStage.FAILURE) {
                    assertThat(shardStatus.getFailure(), notNullValue());
                    numberOfFailures++;
                } else {
                    assertThat(shardStatus.getFailure(), nullValue());
                }
            }
            assertThat(indexStatus.getShardsStats().getFailedShards(), equalTo(numberOfFailures));
        }
    }

    public void testDataFileFailureDuringRestore() throws Exception {
        Path repositoryLocation = randomRepoPath();
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation)));

        prepareCreate("test-idx").setSettings(Settings.builder().put("index.allocation.max_retries", Integer.MAX_VALUE)).get();
        ensureGreen();

        final NumShards numShards = getNumShards("test-idx");

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(createSnapshotResponse.getSnapshotInfo().successfulShards()));

        logger.info("-->  update repository with mock version");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", repositoryLocation)
                                .put("random", randomAlphaOfLength(10))
                                .put("random_data_file_io_exception_rate", 0.3)));

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore index after deletion");
        final RestoreSnapshotResponse restoreResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                                                                                .setWaitForCompletion(true)
                                                                                .get();

        logger.info("--> total number of simulated failures during restore: [{}]", getFailureCount("test-repo"));
        final RestoreInfo restoreInfo = restoreResponse.getRestoreInfo();
        assertThat(restoreInfo.totalShards(), equalTo(numShards.numPrimaries));

        if (restoreInfo.successfulShards() == restoreInfo.totalShards()) {
            // All shards were restored, we must find the exact number of hits
            assertHitCount(client.prepareSearch("test-idx").setSize(0).get(), 100L);
        } else {
            // One or more shards failed to be restored. This can happen when there is
            // only 1 data node: a shard failed because of the random IO exceptions
            // during restore and then we don't allow the shard to be assigned on the
            // same node again during the same reroute operation. Then another reroute
            // operation is scheduled, but the RestoreInProgressAllocationDecider will
            // block the shard to be assigned again because it failed during restore.
            final ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().get();
            assertEquals(1, clusterStateResponse.getState().getNodes().getDataNodes().size());
            assertEquals(restoreInfo.failedShards(),
                clusterStateResponse.getState().getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size());
        }
    }

    public void testDataFileCorruptionDuringRestore() throws Exception {
        Path repositoryLocation = randomRepoPath();
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().successfulShards()));

        logger.info("-->  update repository with mock version");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType("mock").setSettings(
                Settings.builder()
                    .put("location", repositoryLocation)
                    .put("random", randomAlphaOfLength(10))
                    .put("use_lucene_corruption", true)
                    .put("max_failure_number", 10000000L)
                    .put("random_data_file_io_exception_rate", 1.0)));

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore corrupt index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap").setMasterNodeTimeout("30s")
            .setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(),
            equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards()));
        // we have to delete the index here manually, otherwise the cluster will keep
        // trying to allocate the shards for the index, even though the restore operation
        // is completed and marked as failed, which can lead to nodes having pending
        // cluster states to process in their queue when the test is finished
        cluster().wipeIndices("test-idx");
    }

    /**
     * Test that restoring a snapshot whose files can't be downloaded at all is not stuck or
     * does not hang indefinitely.
     */
    public void testUnrestorableFilesDuringRestore() throws Exception {
        final String indexName = "unrestorable-files";
        final int maxRetries = randomIntBetween(1, 10);

        Settings createIndexSettings = Settings.builder().put(SETTING_ALLOCATION_MAX_RETRY.getKey(), maxRetries).build();

        Settings repositorySettings = Settings.builder()
                                                .put("random", randomAlphaOfLength(10))
                                                .put("max_failure_number", 10000000L)
                                                // No lucene corruptions, we want to test retries
                                                .put("use_lucene_corruption", false)
                                                // Restoring a file will never complete
                                                .put("random_data_file_io_exception_rate", 1.0)
                                                .build();

        Consumer<UnassignedInfo> checkUnassignedInfo = unassignedInfo -> {
            assertThat(unassignedInfo.getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
            assertThat(unassignedInfo.getNumFailedAllocations(), anyOf(equalTo(maxRetries), equalTo(1)));
        };

        unrestorableUseCase(indexName, createIndexSettings, repositorySettings, Settings.EMPTY, checkUnassignedInfo, () -> {});
    }

    /**
     * Test that restoring an index with shard allocation filtering settings that prevents
     * its allocation does not hang indefinitely.
     */
    public void testUnrestorableIndexDuringRestore() throws Exception {
        final String indexName = "unrestorable-index";
        Settings restoreIndexSettings = Settings.builder().put("index.routing.allocation.include._name", randomAlphaOfLength(5)).build();

        Consumer<UnassignedInfo> checkUnassignedInfo = unassignedInfo -> {
            assertThat(unassignedInfo.getReason(), equalTo(UnassignedInfo.Reason.NEW_INDEX_RESTORED));
        };

        Runnable fixupAction =() -> {
            // remove the shard allocation filtering settings and use the Reroute API to retry the failed shards
            assertAcked(client().admin().indices().prepareUpdateSettings(indexName)
                                                    .setSettings(Settings.builder()
                                                                            .putNull("index.routing.allocation.include._name")
                                                                            .build()));
            assertAcked(client().admin().cluster().prepareReroute().setRetryFailed(true));
        };

        unrestorableUseCase(indexName, Settings.EMPTY, Settings.EMPTY, restoreIndexSettings, checkUnassignedInfo, fixupAction);
    }

    /** Execute the unrestorable test use case **/
    private void unrestorableUseCase(final String indexName,
                                     final Settings createIndexSettings,
                                     final Settings repositorySettings,
                                     final Settings restoreIndexSettings,
                                     final Consumer<UnassignedInfo> checkUnassignedInfo,
                                     final Runnable fixUpAction) throws Exception {
        // create a test repository
        final Path repositoryLocation = randomRepoPath();
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                                                .setType("fs")
                                                .setSettings(Settings.builder().put("location", repositoryLocation)));
        // create a test index
        assertAcked(prepareCreate(indexName, Settings.builder().put(createIndexSettings)));

        // index some documents
        final int nbDocs = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < nbDocs; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        flushAndRefresh(indexName);
        assertThat(client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits(), equalTo((long) nbDocs));

        // create a snapshot
        final NumShards numShards = getNumShards(indexName);
        CreateSnapshotResponse snapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                                                                            .setWaitForCompletion(true)
                                                                            .setIndices(indexName)
                                                                            .get();

        assertThat(snapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotResponse.getSnapshotInfo().successfulShards(), equalTo(numShards.numPrimaries));
        assertThat(snapshotResponse.getSnapshotInfo().failedShards(), equalTo(0));

        // delete the test index
        assertAcked(client().admin().indices().prepareDelete(indexName));

        // update the test repository
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                                              .setType("mock")
                                              .setSettings(Settings.builder()
                                                                   .put("location", repositoryLocation)
                                                                   .put(repositorySettings)
                                                                   .build()));

        // attempt to restore the snapshot with the given settings
        RestoreSnapshotResponse restoreResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                                                                            .setIndices(indexName)
                                                                            .setIndexSettings(restoreIndexSettings)
                                                                            .setWaitForCompletion(true)
                                                                            .get();

        // check that all shards failed during restore
        assertThat(restoreResponse.getRestoreInfo().totalShards(), equalTo(numShards.numPrimaries));
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(0));

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setCustoms(true).setRoutingTable(true).get();

        // check that there is no restore in progress
        RestoreInProgress restoreInProgress = clusterStateResponse.getState().custom(RestoreInProgress.TYPE);
        assertNotNull("RestoreInProgress must be not null", restoreInProgress);
        assertThat("RestoreInProgress must be empty", restoreInProgress.entries(), hasSize(0));

        // check that the shards have been created but are not assigned
        assertThat(clusterStateResponse.getState().getRoutingTable().allShards(indexName), hasSize(numShards.totalNumShards));

        // check that every primary shard is unassigned
        for (ShardRouting shard : clusterStateResponse.getState().getRoutingTable().allShards(indexName)) {
            if (shard.primary()) {
                assertThat(shard.state(), equalTo(ShardRoutingState.UNASSIGNED));
                assertThat(shard.recoverySource().getType(), equalTo(RecoverySource.Type.SNAPSHOT));
                assertThat(shard.unassignedInfo().getLastAllocationStatus(), equalTo(UnassignedInfo.AllocationStatus.DECIDERS_NO));
                checkUnassignedInfo.accept(shard.unassignedInfo());
            }
        }

        // update the test repository in order to make it work
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                                              .setType("fs")
                                              .setSettings(Settings.builder().put("location", repositoryLocation)));

        // execute action to eventually fix the situation
        fixUpAction.run();

        // delete the index and restore again
        assertAcked(client().admin().indices().prepareDelete(indexName));

        restoreResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).get();
        assertThat(restoreResponse.getRestoreInfo().totalShards(), equalTo(numShards.numPrimaries));
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(numShards.numPrimaries));

        // Wait for the shards to be assigned
        ensureGreen(indexName);
        refresh(indexName);

        assertThat(client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits(), equalTo((long) nbDocs));
    }

    public void testDeletionOfFailingToRecoverIndexShouldStopRestore() throws Exception {
        Path repositoryLocation = randomRepoPath();
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(createSnapshotResponse.getSnapshotInfo().successfulShards()));

        logger.info("-->  update repository with mock version");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", repositoryLocation)
                                .put("random", randomAlphaOfLength(10))
                                .put("random_data_file_io_exception_rate", 1.0) // Fail completely
                ));

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore index after deletion");
        ActionFuture<RestoreSnapshotResponse> restoreSnapshotResponseFuture =
                client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute();

        logger.info("--> wait for the index to appear");
        //  that would mean that recovery process started and failing
        assertThat(waitForIndex("test-idx", TimeValue.timeValueSeconds(10)), equalTo(true));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> get restore results");
        // Now read restore results and make sure it failed
        RestoreSnapshotResponse restoreSnapshotResponse = restoreSnapshotResponseFuture.actionGet(TimeValue.timeValueSeconds(10));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(restoreSnapshotResponse.getRestoreInfo().failedShards()));

        logger.info("--> restoring working repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation)));

        logger.info("--> trying to restore index again");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        SearchResponse countResponse = client.prepareSearch("test-idx").setSize(0).get();
        assertThat(countResponse.getHits().getTotalHits(), equalTo(100L));

    }

    public void testUnallocatedShards() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())));

        logger.info("-->  creating index that cannot be allocated");
        prepareCreate("test-idx", 2, Settings.builder().put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag", "nowhere").put("index.number_of_shards", 3)).setWaitForActiveShards(ActiveShardCount.NONE).get();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.FAILED));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(3));
        assertThat(createSnapshotResponse.getSnapshotInfo().reason(), startsWith("Indices don't have primary shards"));
    }

    public void testDeleteSnapshot() throws Exception {
        final int numberOfSnapshots = between(5, 15);
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx");
        ensureGreen();

        int[] numberOfFiles = new int[numberOfSnapshots];
        logger.info("--> creating {} snapshots ", numberOfSnapshots);
        for (int i = 0; i < numberOfSnapshots; i++) {
            for (int j = 0; j < 10; j++) {
                index("test-idx", "doc", Integer.toString(i * 10 + j), "foo", "bar" + i * 10 + j);
            }
            refresh();
            logger.info("--> snapshot {}", i);
            CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-" + i).setWaitForCompletion(true).setIndices("test-idx").get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
            // Store number of files after each snapshot
            numberOfFiles[i] = numberOfFiles(repo);
        }
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(10L * numberOfSnapshots));
        int numberOfFilesBeforeDeletion = numberOfFiles(repo);

        logger.info("--> delete all snapshots except the first one and last one");
        for (int i = 1; i < numberOfSnapshots - 1; i++) {
            client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap-" + i).get();
        }

        int numberOfFilesAfterDeletion = numberOfFiles(repo);

        assertThat(numberOfFilesAfterDeletion, lessThan(numberOfFilesBeforeDeletion));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> restore index");
        String lastSnapshot = "test-snap-" + (numberOfSnapshots - 1);
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", lastSnapshot).setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(10L * numberOfSnapshots));

        logger.info("--> delete the last snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", lastSnapshot).get();
        logger.info("--> make sure that number of files is back to what it was when the first snapshot was made, " +
                    "plus two because one backup index-N file should remain and incompatible-snapshots");
        assertThat(numberOfFiles(repo), equalTo(numberOfFiles[0] + 2));
    }

    public void testDeleteSnapshotWithMissingIndexAndShardMetadata() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
                client().prepareIndex("test-idx-1", "doc").setSource("foo", "bar"),
                client().prepareIndex("test-idx-2", "doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete index metadata and shard metadata");
        Path indices = repo.resolve("indices");
        Path testIndex1 = indices.resolve("test-idx-1");
        Path testIndex2 = indices.resolve("test-idx-2");
        Path testIndex2Shard0 = testIndex2.resolve("0");
        IOUtils.deleteFilesIgnoringExceptions(testIndex1.resolve("snapshot-test-snap-1"));
        IOUtils.deleteFilesIgnoringExceptions(testIndex2Shard0.resolve("snapshot-test-snap-1"));

        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1"), SnapshotMissingException.class);
    }

    public void testDeleteSnapshotWithMissingMetadata() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
                client().prepareIndex("test-idx-1", "doc").setSource("foo", "bar"),
                client().prepareIndex("test-idx-2", "doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete index metadata and shard metadata");
        Path metadata = repo.resolve("meta-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        Files.delete(metadata);

        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1"), SnapshotMissingException.class);
    }

    public void testDeleteSnapshotWithCorruptedSnapshotFile() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
                client().prepareIndex("test-idx-1", "doc").setSource("foo", "bar"),
                client().prepareIndex("test-idx-2", "doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> truncate snapshot file to make it unreadable");
        Path snapshotPath = repo.resolve("snap-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        try(SeekableByteChannel outChan = Files.newByteChannel(snapshotPath, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }
        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1"), SnapshotMissingException.class);

        logger.info("--> make sure that we can create the snapshot again");
        createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
    }

    public void testSnapshotWithMissingShardLevelIndexFile() throws Exception {
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client().admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(
            Settings.builder().put("location", repo).put("compress", false)));

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1", "doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2", "doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true).setIndices("test-idx-*").get();

        logger.info("--> deleting shard level index file");
        try (Stream<Path> files = Files.list(repo.resolve("indices"))) {
            files.forEach(indexPath ->
                IOUtils.deleteFilesIgnoringExceptions(indexPath.resolve("0").resolve("index-0"))
            );
        }

        logger.info("--> creating another snapshot");
        CreateSnapshotResponse createSnapshotResponse =
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2")
                .setWaitForCompletion(true).setIndices("test-idx-1").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> restoring the first snapshot, the repository should not have lost any shard data despite deleting index-N, " +
                        "because it should have iterated over the snap-*.data files as backup");
        client().admin().indices().prepareDelete("test-idx-1", "test-idx-2").get();
        RestoreSnapshotResponse restoreSnapshotResponse =
            client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).get();
        assertEquals(0, restoreSnapshotResponse.getRestoreInfo().failedShards());
    }

    public void testSnapshotClosedIndex() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())));

        createIndex("test-idx", "test-idx-closed");
        ensureGreen();
        logger.info("-->  closing index test-idx-closed");
        assertAcked(client.admin().indices().prepareClose("test-idx-closed"));
        ClusterStateResponse stateResponse = client.admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metaData().index("test-idx-closed").getState(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test-idx-closed"), nullValue());

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().indices().size(), equalTo(1));
        assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), equalTo(0));

        logger.info("-->  deleting snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").get();

        logger.info("--> snapshot with closed index");
        assertBlocked(client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx", "test-idx-closed"), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
    }

    public void testSnapshotSingleClosedIndex() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())));

        createIndex("test-idx");
        ensureGreen();
        logger.info("-->  closing index test-idx");
        assertAcked(client.admin().indices().prepareClose("test-idx"));

        logger.info("--> snapshot");
        assertBlocked(client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setWaitForCompletion(true).setIndices("test-idx"), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
    }

    public void testRenameOnRestore() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())));

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        assertAcked(client.admin().indices().prepareAliases()
                        .addAlias("test-idx-1", "alias-1")
                        .addAlias("test-idx-2", "alias-2")
                        .addAlias("test-idx-3", "alias-3")
        );

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        assertThat(client.prepareSearch("test-idx-2").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-1", "test-idx-2").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> restore indices with different names");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx-1-copy").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        assertThat(client.prepareSearch("test-idx-2-copy").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> close just restored indices");
        client.admin().indices().prepareClose("test-idx-1-copy", "test-idx-2-copy").get();

        logger.info("--> and try to restore these indices again");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx-1-copy").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        assertThat(client.prepareSearch("test-idx-2-copy").setSize(0).get().getHits().getTotalHits(), equalTo(100L));


        logger.info("--> close indices");
        assertAcked(client.admin().indices().prepareClose("test-idx-1", "test-idx-2-copy"));

        logger.info("--> restore indices with different names");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("(.+-2)").setRenameReplacement("$1-copy").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-1-copy", "test-idx-2", "test-idx-2-copy");

        logger.info("--> try renaming indices using the same name");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRenamePattern("(.+)").setRenameReplacement("same-name").setWaitForCompletion(true).execute().actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices using the same name");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRenamePattern("test-idx-2").setRenameReplacement("test-idx-1").setWaitForCompletion(true).execute().actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices using invalid index name");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setIndices("test-idx-1").setRenamePattern(".+").setRenameReplacement("__WRONG__").setWaitForCompletion(true).execute().actionGet();
            fail("Shouldn't be here");
        } catch (InvalidIndexNameException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias name");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setIndices("test-idx-1").setRenamePattern(".+").setRenameReplacement("alias-3").setWaitForCompletion(true).execute().actionGet();
            fail("Shouldn't be here");
        } catch (InvalidIndexNameException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias of itself");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setIndices("test-idx-1").setRenamePattern("test-idx").setRenameReplacement("alias").setWaitForCompletion(true).execute().actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias of another restored index");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setIndices("test-idx-1", "test-idx-2").setRenamePattern("test-idx-1").setRenameReplacement("alias-2").setWaitForCompletion(true).execute().actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias of itself, but don't restore aliases ");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setIndices("test-idx-1").setRenamePattern("test-idx").setRenameReplacement("alias")
                .setWaitForCompletion(true).setIncludeAliases(false).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));


    }

    public void testMoveShardWhileSnapshotting() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", repositoryLocation)
                                .put("random", randomAlphaOfLength(10))
                                .put("wait_after_unblock", 200)));

        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_replicas", 0)));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], moving shards away from this node", blockedNode);
        Settings.Builder excludeSettings = Settings.builder().put("index.routing.allocation.exclude._name", blockedNode);
        client().admin().indices().prepareUpdateSettings("test-idx").setSettings(excludeSettings).get();

        logger.info("--> unblocking blocked node");
        unblockNode("test-repo", blockedNode);
        logger.info("--> waiting for completion");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");

        List<SnapshotInfo> snapshotInfos = client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots();

        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).shardFailures().size(), equalTo(0));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> replace mock repository with real one at the same location");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation)));

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
    }

    public void testDeleteRepositoryWhileSnapshotting() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", repositoryLocation)
                                .put("random", randomAlphaOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_replicas", 0)));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], trying to delete repository", blockedNode);

        try {
            client.admin().cluster().prepareDeleteRepository("test-repo").execute().get();
            fail("shouldn't be able to delete in-use repository");
        } catch (Exception ex) {
            logger.info("--> in-use repository deletion failed");
        }

        logger.info("--> trying to move repository to another location");
        try {
            client.admin().cluster().preparePutRepository("test-repo")
                    .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation.resolve("test"))
            ).get();
            fail("shouldn't be able to replace in-use repository");
        } catch (Exception ex) {
            logger.info("--> in-use repository replacement failed");
        }

        logger.info("--> trying to create a repository with different name");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo-2")
                .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation.resolve("test"))));

        logger.info("--> unblocking blocked node");
        unblockNode("test-repo", blockedNode);
        logger.info("--> waiting for completion");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");

        List<SnapshotInfo> snapshotInfos = client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots();

        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).shardFailures().size(), equalTo(0));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> replace mock repository with real one at the same location");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", repositoryLocation)));

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
    }

    @TestLogging("_root:DEBUG")  // this fails every now and then: https://github.com/elastic/elasticsearch/issues/18121 but without
    // more logs we cannot find out why
    public void testReadonlyRepository() throws Exception {
        Client client = client();
        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repositoryLocation)
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> create read-only URL repository");
        assertAcked(client.admin().cluster().preparePutRepository("readonly-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repositoryLocation)
                        .put("compress", randomBoolean())
                        .put("readonly", true)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("readonly-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> list available shapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("readonly-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));

        logger.info("--> try deleting snapshot");
        assertThrows(client.admin().cluster().prepareDeleteSnapshot("readonly-repo", "test-snap"), RepositoryException.class, "cannot delete snapshot from a readonly repository");

        logger.info("--> try making another snapshot");
        assertThrows(client.admin().cluster().prepareCreateSnapshot("readonly-repo", "test-snap-2").setWaitForCompletion(true).setIndices("test-idx"), RepositoryException.class, "cannot create snapshot in a readonly repository");
    }

    public void testThrottling() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        boolean throttleSnapshot = randomBoolean();
        boolean throttleRestore = randomBoolean();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repositoryLocation)
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                        .put("max_restore_bytes_per_sec", throttleRestore ? "10k" : "0")
                        .put("max_snapshot_bytes_per_sec", throttleSnapshot ? "10k" : "0")));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        long snapshotPause = 0L;
        long restorePause = 0L;
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            snapshotPause += repositoriesService.repository("test-repo").getSnapshotThrottleTimeInNanos();
            restorePause += repositoriesService.repository("test-repo").getRestoreThrottleTimeInNanos();
        }

        if (throttleSnapshot) {
            assertThat(snapshotPause, greaterThan(0L));
        } else {
            assertThat(snapshotPause, equalTo(0L));
        }

        if (throttleRestore) {
            assertThat(restorePause, greaterThan(0L));
        } else {
            assertThat(restorePause, equalTo(0L));
        }
    }

    public void testSnapshotStatus() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", repositoryLocation)
                                .put("random", randomAlphaOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_replicas", 0)));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], checking snapshot status with specified repository and snapshot", blockedNode);
        SnapshotsStatusResponse response = client.admin().cluster().prepareSnapshotStatus("test-repo").execute().actionGet();
        assertThat(response.getSnapshots().size(), equalTo(1));
        SnapshotStatus snapshotStatus = response.getSnapshots().get(0);
        assertThat(snapshotStatus.getState(), equalTo(SnapshotsInProgress.State.STARTED));
        // We blocked the node during data write operation, so at least one shard snapshot should be in STARTED stage
        assertThat(snapshotStatus.getShardsStats().getStartedShards(), greaterThan(0));
        for (SnapshotIndexShardStatus shardStatus : snapshotStatus.getIndices().get("test-idx")) {
            if (shardStatus.getStage() == SnapshotIndexShardStage.STARTED) {
                assertThat(shardStatus.getNodeId(), notNullValue());
            }
        }

        logger.info("--> checking snapshot status for all currently running and snapshot with empty repository");
        response = client.admin().cluster().prepareSnapshotStatus().execute().actionGet();
        assertThat(response.getSnapshots().size(), equalTo(1));
        snapshotStatus = response.getSnapshots().get(0);
        assertThat(snapshotStatus.getState(), equalTo(SnapshotsInProgress.State.STARTED));
        // We blocked the node during data write operation, so at least one shard snapshot should be in STARTED stage
        assertThat(snapshotStatus.getShardsStats().getStartedShards(), greaterThan(0));
        for (SnapshotIndexShardStatus shardStatus : snapshotStatus.getIndices().get("test-idx")) {
            if (shardStatus.getStage() == SnapshotIndexShardStage.STARTED) {
                assertThat(shardStatus.getNodeId(), notNullValue());
            }
        }

        logger.info("--> checking that _current returns the currently running snapshot");
        GetSnapshotsResponse getResponse = client.admin().cluster().prepareGetSnapshots("test-repo").setCurrentSnapshot().execute().actionGet();
        assertThat(getResponse.getSnapshots().size(), equalTo(1));
        SnapshotInfo snapshotInfo = getResponse.getSnapshots().get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.IN_PROGRESS));

        logger.info("--> unblocking blocked node");
        unblockNode("test-repo", blockedNode);

        snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");


        logger.info("--> checking snapshot status again after snapshot is done");
        response = client.admin().cluster().prepareSnapshotStatus("test-repo").addSnapshots("test-snap").execute().actionGet();
        snapshotStatus = response.getSnapshots().get(0);
        assertThat(snapshotStatus.getIndices().size(), equalTo(1));
        SnapshotIndexStatus indexStatus = snapshotStatus.getIndices().get("test-idx");
        assertThat(indexStatus, notNullValue());
        assertThat(indexStatus.getShardsStats().getInitializingShards(), equalTo(0));
        assertThat(indexStatus.getShardsStats().getFailedShards(), equalTo(snapshotInfo.failedShards()));
        assertThat(indexStatus.getShardsStats().getDoneShards(), equalTo(snapshotInfo.successfulShards()));
        assertThat(indexStatus.getShards().size(), equalTo(snapshotInfo.totalShards()));

        logger.info("--> checking snapshot status after it is done with empty repository");
        response = client.admin().cluster().prepareSnapshotStatus().execute().actionGet();
        assertThat(response.getSnapshots().size(), equalTo(0));

        logger.info("--> checking that _current no longer returns the snapshot");
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("_current").execute().actionGet().getSnapshots().isEmpty(), equalTo(true));

        // test that getting an unavailable snapshot status throws an exception if ignoreUnavailable is false on the request
        SnapshotMissingException ex = expectThrows(SnapshotMissingException.class, () ->
            client.admin().cluster().prepareSnapshotStatus("test-repo").addSnapshots("test-snap-doesnt-exist").get());
        assertEquals("[test-repo:test-snap-doesnt-exist] is missing", ex.getMessage());
        // test that getting an unavailable snapshot status does not throw an exception if ignoreUnavailable is true on the request
        response = client.admin().cluster().prepareSnapshotStatus("test-repo")
                       .addSnapshots("test-snap-doesnt-exist")
                       .setIgnoreUnavailable(true)
                       .get();
        assertTrue(response.getSnapshots().isEmpty());
        // test getting snapshot status for available and unavailable snapshots where ignoreUnavailable is true
        // (available one should be returned)
        response = client.admin().cluster().prepareSnapshotStatus("test-repo")
                       .addSnapshots("test-snap", "test-snap-doesnt-exist")
                       .setIgnoreUnavailable(true)
                       .get();
        assertEquals(1, response.getSnapshots().size());
        assertEquals("test-snap", response.getSnapshots().get(0).getSnapshot().getSnapshotId().getName());
    }

    public void testSnapshotRelocatingPrimary() throws Exception {
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        // Create index on 1 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 1, Settings.builder().put("number_of_replicas", 0)));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> start relocations");
        allowNodes("test-idx", internalCluster().numDataNodes());

        logger.info("--> wait for relocations to start");

        waitForRelocationsToStart("test-idx", TimeValue.timeValueMillis(300));

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> wait for snapshot to complete");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600));
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.shardFailures().size(), equalTo(0));
        logger.info("--> done");
    }

    public void testSnapshotMoreThanOnce() throws ExecutionException, InterruptedException {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        // only one shard
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)));
        ensureGreen();
        logger.info("-->  indexing");

        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "doc", Integer.toString(i)).setSource("foo", "bar" + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();
        assertNoFailures(client().admin().indices().prepareForceMerge("test").setFlush(true).setMaxNumSegments(1).get());

        CreateSnapshotResponse createSnapshotResponseFirst = client.admin().cluster().prepareCreateSnapshot("test-repo", "test").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseFirst.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseFirst.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseFirst.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), greaterThan(1));
            }
        }

        CreateSnapshotResponse createSnapshotResponseSecond = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-1").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseSecond.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseSecond.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseSecond.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-1").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-1").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), equalTo(0));
            }
        }

        client().prepareDelete("test", "doc", "1").get();
        CreateSnapshotResponse createSnapshotResponseThird = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-2").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseThird.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseThird.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseThird.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-2").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-2").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), equalTo(2)); // we flush before the snapshot such that we have to process the segments_N files plus the .del file
            }
        }
    }

    public void testChangeSettingsOnRestore() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        logger.info("--> create test index with synonyms search analyzer");

        Settings.Builder indexSettings = Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))
                .put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), "10s")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                .putList("index.analysis.analyzer.my_analyzer.filter", "lowercase", "my_synonym")
                .put("index.analysis.filter.my_synonym.type", "synonym")
                .put("index.analysis.filter.my_synonym.synonyms", "foo => bar");

        assertAcked(prepareCreate("test-idx", 2, indexSettings));

        int numberOfShards = getNumShards("test-idx").numPrimaries;
        assertAcked(client().admin().indices().preparePutMapping("test-idx").setType("type1").setSource("field1", "type=text,analyzer=standard,search_analyzer=my_analyzer"));
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx", "type1", Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        assertHitCount(client.prepareSearch("test-idx").setSize(0).setQuery(matchQuery("field1", "foo")).get(), numdocs);
        assertHitCount(client.prepareSearch("test-idx").setSize(0).setQuery(matchQuery("field1", "bar")).get(), numdocs);

        logger.info("--> snapshot it");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete the index and recreate it while changing refresh interval and analyzer");
        cluster().wipeIndices("test-idx");

        Settings newIndexSettings = Settings.builder()
                .put("refresh_interval", "5s")
                .put("index.analysis.analyzer.my_analyzer.type", "standard")
                .build();

        Settings newIncorrectIndexSettings = Settings.builder()
                .put(newIndexSettings)
                .put(SETTING_NUMBER_OF_SHARDS, numberOfShards + 100)
                .build();

        logger.info("--> try restoring while changing the number of shards - should fail");
        assertThrows(client.admin().cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setIgnoreIndexSettings("index.analysis.*")
                .setIndexSettings(newIncorrectIndexSettings)
                .setWaitForCompletion(true), SnapshotRestoreException.class);

        logger.info("--> try restoring while changing the number of replicas to a negative number - should fail");
        Settings newIncorrectReplicasIndexSettings = Settings.builder()
            .put(newIndexSettings)
            .put(SETTING_NUMBER_OF_REPLICAS.substring(IndexMetaData.INDEX_SETTING_PREFIX.length()), randomIntBetween(-10, -1))
            .build();
        assertThrows(client.admin().cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setIgnoreIndexSettings("index.analysis.*")
            .setIndexSettings(newIncorrectReplicasIndexSettings)
            .setWaitForCompletion(true), IllegalArgumentException.class);

        logger.info("--> restore index with correct settings from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setIgnoreIndexSettings("index.analysis.*")
                .setIndexSettings(newIndexSettings)
                .setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        logger.info("--> assert that correct settings are restored");
        GetSettingsResponse getSettingsResponse = client.admin().indices().prepareGetSettings("test-idx").execute().actionGet();
        assertThat(getSettingsResponse.getSetting("test-idx", INDEX_REFRESH_INTERVAL_SETTING.getKey()), equalTo("5s"));
        // Make sure that number of shards didn't change
        assertThat(getSettingsResponse.getSetting("test-idx", SETTING_NUMBER_OF_SHARDS), equalTo("" + numberOfShards));
        assertThat(getSettingsResponse.getSetting("test-idx", "index.analysis.analyzer.my_analyzer.type"), equalTo("standard"));
        assertThat(getSettingsResponse.getSetting("test-idx", "index.analysis.filter.my_synonym.type"), nullValue());

        assertHitCount(client.prepareSearch("test-idx").setSize(0).setQuery(matchQuery("field1", "foo")).get(), 0);
        assertHitCount(client.prepareSearch("test-idx").setSize(0).setQuery(matchQuery("field1", "bar")).get(), numdocs);

        logger.info("--> delete the index and recreate it while deleting all index settings");
        cluster().wipeIndices("test-idx");

        logger.info("--> restore index with correct settings from the snapshot");
        restoreSnapshotResponse = client.admin().cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setIgnoreIndexSettings("*") // delete everything we can delete
                .setIndexSettings(newIndexSettings)
                .setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        logger.info("--> assert that correct settings are restored and index is still functional");
        getSettingsResponse = client.admin().indices().prepareGetSettings("test-idx").execute().actionGet();
        assertThat(getSettingsResponse.getSetting("test-idx", INDEX_REFRESH_INTERVAL_SETTING.getKey()), equalTo("5s"));
        // Make sure that number of shards didn't change
        assertThat(getSettingsResponse.getSetting("test-idx", SETTING_NUMBER_OF_SHARDS), equalTo("" + numberOfShards));

        assertHitCount(client.prepareSearch("test-idx").setSize(0).setQuery(matchQuery("field1", "foo")).get(), 0);
        assertHitCount(client.prepareSearch("test-idx").setSize(0).setQuery(matchQuery("field1", "bar")).get(), numdocs);

    }

    public void testRecreateBlocksOnRestore() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        Settings.Builder indexSettings = Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))
                .put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), "10s");

        logger.info("--> create index");
        assertAcked(prepareCreate("test-idx", 2, indexSettings));

        try {
            List<String> initialBlockSettings = randomSubsetOf(randomInt(3),
                    IndexMetaData.SETTING_BLOCKS_WRITE, IndexMetaData.SETTING_BLOCKS_METADATA, IndexMetaData.SETTING_READ_ONLY);
            Settings.Builder initialSettingsBuilder = Settings.builder();
            for (String blockSetting : initialBlockSettings) {
                initialSettingsBuilder.put(blockSetting, true);
            }
            Settings initialSettings = initialSettingsBuilder.build();
            logger.info("--> using initial block settings {}", initialSettings);

            if (!initialSettings.isEmpty()) {
                logger.info("--> apply initial blocks to index");
                client().admin().indices().prepareUpdateSettings("test-idx").setSettings(initialSettingsBuilder).get();
            }

            logger.info("--> snapshot index");
            CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                    .setWaitForCompletion(true).setIndices("test-idx").get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

            logger.info("--> remove blocks and delete index");
            disableIndexBlock("test-idx", IndexMetaData.SETTING_BLOCKS_METADATA);
            disableIndexBlock("test-idx", IndexMetaData.SETTING_READ_ONLY);
            disableIndexBlock("test-idx", IndexMetaData.SETTING_BLOCKS_WRITE);
            disableIndexBlock("test-idx", IndexMetaData.SETTING_BLOCKS_READ);
            cluster().wipeIndices("test-idx");

            logger.info("--> restore index with additional block changes");
            List<String> changeBlockSettings = randomSubsetOf(randomInt(4),
                    IndexMetaData.SETTING_BLOCKS_METADATA, IndexMetaData.SETTING_BLOCKS_WRITE,
                    IndexMetaData.SETTING_READ_ONLY, IndexMetaData.SETTING_BLOCKS_READ);
            Settings.Builder changedSettingsBuilder = Settings.builder();
            for (String blockSetting : changeBlockSettings) {
                changedSettingsBuilder.put(blockSetting, randomBoolean());
            }
            Settings changedSettings = changedSettingsBuilder.build();
            logger.info("--> applying changed block settings {}", changedSettings);

            RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
                    .prepareRestoreSnapshot("test-repo", "test-snap")
                    .setIndexSettings(changedSettings)
                    .setWaitForCompletion(true).execute().actionGet();
            assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

            ClusterBlocks blocks = client.admin().cluster().prepareState().clear().setBlocks(true).get().getState().blocks();
            // compute current index settings (as we cannot query them if they contain SETTING_BLOCKS_METADATA)
            Settings mergedSettings = Settings.builder()
                    .put(initialSettings)
                    .put(changedSettings)
                    .build();
            logger.info("--> merged block settings {}", mergedSettings);

            logger.info("--> checking consistency between settings and blocks");
            assertThat(mergedSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_METADATA, false),
                    is(blocks.hasIndexBlock("test-idx", IndexMetaData.INDEX_METADATA_BLOCK)));
            assertThat(mergedSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_READ, false),
                    is(blocks.hasIndexBlock("test-idx", IndexMetaData.INDEX_READ_BLOCK)));
            assertThat(mergedSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_WRITE, false),
                    is(blocks.hasIndexBlock("test-idx", IndexMetaData.INDEX_WRITE_BLOCK)));
            assertThat(mergedSettings.getAsBoolean(IndexMetaData.SETTING_READ_ONLY, false),
                    is(blocks.hasIndexBlock("test-idx", IndexMetaData.INDEX_READ_ONLY_BLOCK)));
        } finally {
            logger.info("--> cleaning up blocks");
            disableIndexBlock("test-idx", IndexMetaData.SETTING_BLOCKS_METADATA);
            disableIndexBlock("test-idx", IndexMetaData.SETTING_READ_ONLY);
            disableIndexBlock("test-idx", IndexMetaData.SETTING_BLOCKS_WRITE);
            disableIndexBlock("test-idx", IndexMetaData.SETTING_BLOCKS_READ);
        }
    }

    public void testCloseOrDeleteIndexDuringSnapshot() throws Exception {
        Client client = client();

        boolean allowPartial = randomBoolean();
        logger.info("-->  creating repository");

        // only block on repo init if we have partial snapshot or we run into deadlock when acquiring shard locks for index deletion/closing
        boolean initBlocking = allowPartial || randomBoolean();
        if (initBlocking) {
            assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(Settings.builder()
                    .put("location", randomRepoPath())
                    .put("compress", randomBoolean())
                    .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                    .put("block_on_init", true)
                ));
        } else {
            assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(Settings.builder()
                    .put("location", randomRepoPath())
                    .put("compress", randomBoolean())
                    .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                    .put("block_on_data", true)
                ));
        }

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        assertThat(client.prepareSearch("test-idx-2").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        assertThat(client.prepareSearch("test-idx-3").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot allow partial {}", allowPartial);
        ActionFuture<CreateSnapshotResponse> future = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
            .setIndices("test-idx-*").setWaitForCompletion(true).setPartial(allowPartial).execute();
        logger.info("--> wait for block to kick in");
        if (initBlocking) {
            waitForBlock(internalCluster().getMasterName(), "test-repo", TimeValue.timeValueMinutes(1));
        } else {
            waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));
        }
        try {
            if (allowPartial) {
                // partial snapshots allow close / delete operations
                if (randomBoolean()) {
                    logger.info("--> delete index while partial snapshot is running");
                    client.admin().indices().prepareDelete("test-idx-1").get();
                } else {
                    logger.info("--> close index while partial snapshot is running");
                    client.admin().indices().prepareClose("test-idx-1").get();
                }
            } else {
                // non-partial snapshots do not allow close / delete operations on indices where snapshot has not been completed
                if (randomBoolean()) {
                    try {
                        logger.info("--> delete index while non-partial snapshot is running");
                        client.admin().indices().prepareDelete("test-idx-1").get();
                        fail("Expected deleting index to fail during snapshot");
                    } catch (IllegalArgumentException e) {
                        assertThat(e.getMessage(), containsString("Cannot delete indices that are being snapshotted: [[test-idx-1/"));
                    }
                } else {
                    try {
                        logger.info("--> close index while non-partial snapshot is running");
                        client.admin().indices().prepareClose("test-idx-1").get();
                        fail("Expected closing index to fail during snapshot");
                    } catch (IllegalArgumentException e) {
                        assertThat(e.getMessage(), containsString("Cannot close indices that are being snapshotted: [[test-idx-1/"));
                    }
                }
            }
        } finally {
            if (initBlocking) {
                logger.info("--> unblock running master node");
                unblockNode("test-repo", internalCluster().getMasterName());
            } else {
                logger.info("--> unblock all data nodes");
                unblockAllDataNodes("test-repo");
            }
        }
        logger.info("--> waiting for snapshot to finish");
        CreateSnapshotResponse createSnapshotResponse = future.get();

        if (allowPartial) {
            logger.info("Deleted/Closed index during snapshot, but allow partial");
            assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo((SnapshotState.PARTIAL)));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().failedShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), lessThan(createSnapshotResponse.getSnapshotInfo().totalShards()));
        } else {
            logger.info("Snapshot successfully completed");
            assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo((SnapshotState.SUCCESS)));
        }
    }

    public void testCloseIndexDuringRestore() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType("mock").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            ));

        createIndex("test-idx-1", "test-idx-2");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), equalTo(100L));
        assertThat(client.prepareSearch("test-idx-2").setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> snapshot");
        assertThat(client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
            .setIndices("test-idx-*").setWaitForCompletion(true).get().getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> deleting indices before restoring");
        assertAcked(client.admin().indices().prepareDelete("test-idx-*").get());

        blockAllDataNodes("test-repo");
        logger.info("--> execution will be blocked on all data nodes");

        final ActionFuture<RestoreSnapshotResponse> restoreFut;
        try {
            logger.info("--> start restore");
            restoreFut = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .execute();

            logger.info("--> waiting for block to kick in");
            waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));

            logger.info("--> close index while restore is running");
            try {
                client.admin().indices().prepareClose("test-idx-1").get();
                fail("Expected closing index to fail during restore");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString("Cannot close indices that are being restored: [[test-idx-1/"));
            }
        } finally {
            // unblock even if the try block fails otherwise we will get bogus failures when we delete all indices in test teardown.
            logger.info("--> unblocking all data nodes");
            unblockAllDataNodes("test-repo");
        }

        logger.info("--> wait for restore to finish");
        RestoreSnapshotResponse restoreSnapshotResponse = restoreFut.get();
        logger.info("--> check that all shards were recovered");
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
    }

    public void testDeleteSnapshotWhileRestoringFails() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        final String repoName = "test-repo";
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
                        .setType("mock")
                        .setSettings(Settings.builder().put("location", randomRepoPath())));

        logger.info("--> creating index");
        final String indexName = "test-idx";
        assertAcked(prepareCreate(indexName).setWaitForActiveShards(ActiveShardCount.ALL));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch(indexName).setSize(0).get().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> take snapshots");
        final String snapshotName = "test-snap";
        assertThat(client.admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                       .setIndices(indexName).setWaitForCompletion(true).get().getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        final String snapshotName2 = "test-snap-2";
        assertThat(client.admin().cluster().prepareCreateSnapshot(repoName, snapshotName2)
                       .setIndices(indexName).setWaitForCompletion(true).get().getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete index before restoring");
        assertAcked(client.admin().indices().prepareDelete(indexName).get());

        logger.info("--> execution will be blocked on all data nodes");
        blockAllDataNodes(repoName);

        final ActionFuture<RestoreSnapshotResponse> restoreFut;
        try {
            logger.info("--> start restore");
            restoreFut = client.admin().cluster().prepareRestoreSnapshot(repoName, snapshotName)
                             .setWaitForCompletion(true)
                             .execute();

            logger.info("--> waiting for block to kick in");
            waitForBlockOnAnyDataNode(repoName, TimeValue.timeValueMinutes(1));

            logger.info("--> try deleting the snapshot while the restore is in progress (should throw an error)");
            ConcurrentSnapshotExecutionException e = expectThrows(ConcurrentSnapshotExecutionException.class, () ->
                client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());
            assertEquals(repoName, e.getRepositoryName());
            assertEquals(snapshotName, e.getSnapshotName());
            assertThat(e.getMessage(), containsString("cannot delete snapshot during a restore"));

            logger.info("-- try deleting another snapshot while the restore is in progress (should throw an error)");
            e = expectThrows(ConcurrentSnapshotExecutionException.class, () ->
                client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName2).get());
            assertEquals(repoName, e.getRepositoryName());
            assertEquals(snapshotName2, e.getSnapshotName());
            assertThat(e.getMessage(), containsString("cannot delete snapshot during a restore"));
        } finally {
            // unblock even if the try block fails otherwise we will get bogus failures when we delete all indices in test teardown.
            logger.info("--> unblocking all data nodes");
            unblockAllDataNodes(repoName);
        }

        logger.info("--> wait for restore to finish");
        restoreFut.get();
    }

    public void testDeleteOrphanSnapshot() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        final String repositoryName = "test-repo";
        assertAcked(client.admin().cluster().preparePutRepository(repositoryName)
                .setType("mock").setSettings(Settings.builder()
                                .put("location", randomRepoPath())
                                .put("compress", randomBoolean())
                                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                ));

        logger.info("--> create the index");
        final String idxName = "test-idx";
        createIndex(idxName);
        ensureGreen();

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("--> snapshot");
        final String snapshotName = "test-snap";
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName).setWaitForCompletion(true).setIndices(idxName).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> emulate an orphan snapshot");
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        final RepositoryData repositoryData = repositoriesService.repository(repositoryName).getRepositoryData();
        final IndexId indexId = repositoryData.resolveIndexId(idxName);

        clusterService.submitStateUpdateTask("orphan snapshot test", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                // Simulate orphan snapshot
                ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                shards.put(new ShardId(idxName, "_na_", 0), new ShardSnapshotStatus("unknown-node", State.ABORTED, "aborted"));
                shards.put(new ShardId(idxName, "_na_", 1), new ShardSnapshotStatus("unknown-node", State.ABORTED, "aborted"));
                shards.put(new ShardId(idxName, "_na_", 2), new ShardSnapshotStatus("unknown-node", State.ABORTED, "aborted"));
                List<Entry> entries = new ArrayList<>();
                entries.add(new Entry(new Snapshot(repositoryName,
                                                   createSnapshotResponse.getSnapshotInfo().snapshotId()),
                                                   true,
                                                   false,
                                                   State.ABORTED,
                                                   Collections.singletonList(indexId),
                                                   System.currentTimeMillis(),
                                                   repositoryData.getGenId(),
                                                   shards.build()));
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(Collections.unmodifiableList(entries))).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                fail();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        logger.info("--> try deleting the orphan snapshot");

        assertAcked(client.admin().cluster().prepareDeleteSnapshot(repositoryName, snapshotName).get("10s"));
    }

    private boolean waitForIndex(final String index, TimeValue timeout) throws InterruptedException {
        return awaitBusy(() -> client().admin().indices().prepareExists(index).execute().actionGet().isExists(), timeout.millis(), TimeUnit.MILLISECONDS);
    }

    private boolean waitForRelocationsToStart(final String index, TimeValue timeout) throws InterruptedException {
        return awaitBusy(() -> client().admin().cluster().prepareHealth(index).execute().actionGet().getRelocatingShards() > 0, timeout.millis(), TimeUnit.MILLISECONDS);
    }

    public void testSnapshotName() throws Exception {
        final Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        expectThrows(InvalidSnapshotNameException.class,
                     () -> client.admin().cluster().prepareCreateSnapshot("test-repo", "_foo").get());
        expectThrows(SnapshotMissingException.class,
                     () -> client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("_foo").get());
        expectThrows(SnapshotMissingException.class,
                     () -> client.admin().cluster().prepareDeleteSnapshot("test-repo", "_foo").get());
        expectThrows(SnapshotMissingException.class,
                     () -> client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("_foo").get());
    }

    public void testListCorruptedSnapshot() throws Exception {
        Client client = client();
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", repo)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        logger.info("--> indexing some data");
        indexRandom(true,
                client().prepareIndex("test-idx-1", "doc").setSource("foo", "bar"),
                client().prepareIndex("test-idx-2", "doc").setSource("foo", "bar"),
                client().prepareIndex("test-idx-3", "doc").setSource("foo", "bar"));

        logger.info("--> creating 2 snapshots");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> truncate snapshot file to make it unreadable");
        Path snapshotPath = repo.resolve("snap-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        try(SeekableByteChannel outChan = Files.newByteChannel(snapshotPath, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        logger.info("--> get snapshots request should return both snapshots");
        List<SnapshotInfo> snapshotInfos = client.admin().cluster()
                .prepareGetSnapshots("test-repo")
                .setIgnoreUnavailable(true).get().getSnapshots();

        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo("test-snap-1"));

        try {
            client.admin().cluster().prepareGetSnapshots("test-repo").setIgnoreUnavailable(false).get().getSnapshots();
        } catch (SnapshotException ex) {
            assertThat(ex.getRepositoryName(), equalTo("test-repo"));
            assertThat(ex.getSnapshotName(), equalTo("test-snap-2"));
        }
    }

    public void testCannotCreateSnapshotsWithSameName() throws Exception {
        final String repositoryName = "test-repo";
        final String snapshotName = "test-snap";
        final String indexName = "test-idx";
        final Client client = client();
        final Path repo = randomRepoPath();

        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repositoryName)
                                            .setType("fs").setSettings(Settings.builder()
                                                            .put("location", repo)
                                                            .put("compress", false)
                                                            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        logger.info("--> creating an index and indexing documents");
        createIndex(indexName);
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> take first snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
                                                              .cluster()
                                                              .prepareCreateSnapshot(repositoryName, snapshotName)
                                                              .setWaitForCompletion(true)
                                                              .setIndices(indexName)
                                                              .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> index more documents");
        for (int i = 10; i < 20; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> second snapshot of the same name should fail");
        try {
            createSnapshotResponse = client.admin()
                                           .cluster()
                                           .prepareCreateSnapshot(repositoryName, snapshotName)
                                           .setWaitForCompletion(true)
                                           .setIndices(indexName)
                                           .get();
            fail("should not be allowed to create a snapshot with the same name as an already existing snapshot: " +
                 createSnapshotResponse.getSnapshotInfo().snapshotId());
        } catch (InvalidSnapshotNameException e) {
            assertThat(e.getMessage(), containsString("snapshot with the same name already exists"));
        }

        logger.info("--> delete the first snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repositoryName, snapshotName).get();

        logger.info("--> try creating a snapshot with the same name, now it should work because the first one was deleted");
        createSnapshotResponse = client.admin()
                                       .cluster()
                                       .prepareCreateSnapshot(repositoryName, snapshotName)
                                       .setWaitForCompletion(true)
                                       .setIndices(indexName)
                                       .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().snapshotId().getName(), equalTo(snapshotName));
    }

    public void testGetSnapshotsRequest() throws Exception {
        final String repositoryName = "test-repo";
        final String indexName = "test-idx";
        final Client client = client();
        final Path repo = randomRepoPath();

        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repositoryName)
                          .setType("mock").setSettings(Settings.builder()
                                                               .put("location", repo)
                                                               .put("compress", false)
                                                               .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                                                               .put("wait_after_unblock", 200)));

        logger.info("--> get snapshots on an empty repository");
        expectThrows(SnapshotMissingException.class, () -> client.admin()
                                                                 .cluster()
                                                                 .prepareGetSnapshots(repositoryName)
                                                                 .addSnapshots("non-existent-snapshot")
                                                                 .get());
        // with ignore unavailable set to true, should not throw an exception
        GetSnapshotsResponse getSnapshotsResponse = client.admin()
                                                          .cluster()
                                                          .prepareGetSnapshots(repositoryName)
                                                          .setIgnoreUnavailable(true)
                                                          .addSnapshots("non-existent-snapshot")
                                                          .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(0));

        logger.info("--> creating an index and indexing documents");
        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate(indexName, 1, Settings.builder().put("number_of_replicas", 0)));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        // make sure we return only the in-progress snapshot when taking the first snapshot on a clean repository
        // take initial snapshot with a block, making sure we only get 1 in-progress snapshot returned
        // block a node so the create snapshot operation can remain in progress
        final String initialBlockedNode = blockNodeWithIndex(repositoryName, indexName);
        ActionFuture<CreateSnapshotResponse> responseListener =
            client.admin().cluster().prepareCreateSnapshot(repositoryName, "snap-on-empty-repo")
                .setWaitForCompletion(false)
                .setIndices(indexName)
                .execute();
        waitForBlock(initialBlockedNode, repositoryName, TimeValue.timeValueSeconds(60)); // wait for block to kick in
        getSnapshotsResponse = client.admin().cluster()
                                   .prepareGetSnapshots("test-repo")
                                   .setSnapshots(randomFrom("_all", "_current", "snap-on-*", "*-on-empty-repo", "snap-on-empty-repo"))
                                   .get();
        assertEquals(1, getSnapshotsResponse.getSnapshots().size());
        assertEquals("snap-on-empty-repo", getSnapshotsResponse.getSnapshots().get(0).snapshotId().getName());
        unblockNode(repositoryName, initialBlockedNode); // unblock node
        responseListener.actionGet(TimeValue.timeValueMillis(10000L)); // timeout after 10 seconds
        client.admin().cluster().prepareDeleteSnapshot(repositoryName, "snap-on-empty-repo").get();

        final int numSnapshots = randomIntBetween(1, 3) + 1;
        logger.info("--> take {} snapshot(s)", numSnapshots - 1);
        final String[] snapshotNames = new String[numSnapshots];
        for (int i = 0; i < numSnapshots - 1; i++) {
            final String snapshotName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            CreateSnapshotResponse createSnapshotResponse = client.admin()
                                                                  .cluster()
                                                                  .prepareCreateSnapshot(repositoryName, snapshotName)
                                                                  .setWaitForCompletion(true)
                                                                  .setIndices(indexName)
                                                                  .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            snapshotNames[i] = snapshotName;
        }
        logger.info("--> take another snapshot to be in-progress");
        // add documents so there are data files to block on
        for (int i = 10; i < 20; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final String inProgressSnapshot = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        snapshotNames[numSnapshots - 1] = inProgressSnapshot;
        // block a node so the create snapshot operation can remain in progress
        final String blockedNode = blockNodeWithIndex(repositoryName, indexName);
        client.admin().cluster().prepareCreateSnapshot(repositoryName, inProgressSnapshot)
                                .setWaitForCompletion(false)
                                .setIndices(indexName)
                                .get();
        waitForBlock(blockedNode, repositoryName, TimeValue.timeValueSeconds(60)); // wait for block to kick in

        logger.info("--> get all snapshots with a current in-progress");
        // with ignore unavailable set to true, should not throw an exception
        final List<String> snapshotsToGet = new ArrayList<>();
        if (randomBoolean()) {
            // use _current plus the individual names of the finished snapshots
            snapshotsToGet.add("_current");
            for (int i = 0; i < numSnapshots - 1; i++) {
                snapshotsToGet.add(snapshotNames[i]);
            }
        } else {
            snapshotsToGet.add("_all");
        }
        getSnapshotsResponse = client.admin().cluster()
                                             .prepareGetSnapshots(repositoryName)
                                             .setSnapshots(snapshotsToGet.toArray(Strings.EMPTY_ARRAY))
                                             .get();
        List<String> sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream()
                                                      .map(s -> s.snapshotId().getName())
                                                      .sorted()
                                                      .collect(Collectors.toList()), equalTo(sortedNames));

        getSnapshotsResponse = client.admin().cluster()
                                   .prepareGetSnapshots(repositoryName)
                                   .addSnapshots(snapshotNames)
                                   .get();
        sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream()
                                                      .map(s -> s.snapshotId().getName())
                                                      .sorted()
                                                      .collect(Collectors.toList()), equalTo(sortedNames));

        logger.info("--> make sure duplicates are not returned in the response");
        String regexName = snapshotNames[randomIntBetween(0, numSnapshots - 1)];
        final int splitPos = regexName.length() / 2;
        final String firstRegex = regexName.substring(0, splitPos) + "*";
        final String secondRegex = "*" + regexName.substring(splitPos);
        getSnapshotsResponse = client.admin().cluster()
                                             .prepareGetSnapshots(repositoryName)
                                             .addSnapshots(snapshotNames)
                                             .addSnapshots(firstRegex, secondRegex)
                                             .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream()
                                                      .map(s -> s.snapshotId().getName())
                                                      .sorted()
                                                      .collect(Collectors.toList()), equalTo(sortedNames));

        unblockNode(repositoryName, blockedNode); // unblock node
        waitForCompletion(repositoryName, inProgressSnapshot, TimeValue.timeValueSeconds(60));
    }

    /**
     * This test ensures that when a shard is removed from a node (perhaps due to the node
     * leaving the cluster, then returning), all snapshotting of that shard is aborted, so
     * all Store references held onto by the snapshot are released.
     *
     * See https://github.com/elastic/elasticsearch/issues/20876
     */
    public void testSnapshotCanceledOnRemovedShard() throws Exception {
        final int numPrimaries = 1;
        final int numReplicas = 1;
        final int numDocs = 100;
        final String repo = "test-repo";
        final String index = "test-idx";
        final String snapshot = "test-snap";

        assertAcked(prepareCreate(index, 1,
            Settings.builder().put("number_of_shards", numPrimaries).put("number_of_replicas", numReplicas)));

        logger.info("--> indexing some data");
        for (int i = 0; i < numDocs; i++) {
            index(index, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse =
            client().admin().cluster().preparePutRepository(repo).setType("mock").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("random", randomAlphaOfLength(10))
                .put("wait_after_unblock", 200)
            ).get();
        assertTrue(putRepositoryResponse.isAcknowledged());

        String blockedNode = blockNodeWithIndex(repo, index);

        logger.info("--> snapshot");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot)
            .setWaitForCompletion(false)
            .execute();

        logger.info("--> waiting for block to kick in on node [{}]", blockedNode);
        waitForBlock(blockedNode, repo, TimeValue.timeValueSeconds(10));

        logger.info("--> removing primary shard that is being snapshotted");
        ClusterState clusterState = internalCluster().clusterService(internalCluster().getMasterName()).state();
        IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(index);
        String nodeWithPrimary = clusterState.nodes().get(indexRoutingTable.shard(0).primaryShard().currentNodeId()).getName();
        assertNotNull("should be at least one node with a primary shard", nodeWithPrimary);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeWithPrimary);
        IndexService indexService = indicesService.indexService(resolveIndex(index));
        indexService.removeShard(0, "simulate node removal");

        logger.info("--> unblocking blocked node [{}]", blockedNode);
        unblockNode(repo, blockedNode);

        logger.info("--> ensuring snapshot is aborted and the aborted shard was marked as failed");
        SnapshotInfo snapshotInfo = waitForCompletion(repo, snapshot, TimeValue.timeValueSeconds(10));
        assertEquals(1, snapshotInfo.shardFailures().size());
        assertEquals(0, snapshotInfo.shardFailures().get(0).shardId());
        assertEquals("IndexShardSnapshotFailedException[Aborted]", snapshotInfo.shardFailures().get(0).reason());
    }

    public void testSnapshotSucceedsAfterSnapshotFailure() throws Exception {
        logger.info("--> creating repository");
        final Path repoPath = randomRepoPath();
        final Client client = client();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("mock").setVerify(false).setSettings(
                Settings.builder()
                    .put("location", repoPath)
                    .put("random_control_io_exception_rate", randomIntBetween(5, 20) / 100f)
                    // test that we can take a snapshot after a failed one, even if a partial index-N was written
                    .put("atomic_move", false)
                    .put("random", randomAlphaOfLength(10))));

        logger.info("--> indexing some data");
        assertAcked(prepareCreate("test-idx").setSettings(
            // the less the number of shards, the less control files we have, so we are giving a higher probability of
            // triggering an IOException toward the end when writing the pending-index-* files, which are the files
            // that caused problems with writing subsequent snapshots if they happened to be lingering in the repository
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        final int numDocs = randomIntBetween(1, 5);
        for (int i = 0; i < numDocs; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo((long) numDocs));

        logger.info("--> snapshot with potential I/O failures");
        try {
            CreateSnapshotResponse createSnapshotResponse =
                client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                    .setWaitForCompletion(true)
                    .setIndices("test-idx")
                    .get();
            if (createSnapshotResponse.getSnapshotInfo().totalShards() != createSnapshotResponse.getSnapshotInfo().successfulShards()) {
                assertThat(getFailureCount("test-repo"), greaterThan(0L));
                assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), greaterThan(0));
                for (SnapshotShardFailure shardFailure : createSnapshotResponse.getSnapshotInfo().shardFailures()) {
                    assertThat(shardFailure.reason(), containsString("Random IOException"));
                }
            }
        } catch (SnapshotCreationException | RepositoryException ex) {
            // sometimes, the snapshot will fail with a top level I/O exception
            assertThat(ExceptionsHelper.stackTrace(ex), containsString("Random IOException"));
        }

        logger.info("--> snapshot with no I/O failures");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo-2").setType("mock").setSettings(
            Settings.builder().put("location", repoPath)));
        CreateSnapshotResponse createSnapshotResponse =
            client.admin().cluster().prepareCreateSnapshot("test-repo-2", "test-snap-2")
                .setWaitForCompletion(true)
                .setIndices("test-idx")
                .get();
        assertEquals(0, createSnapshotResponse.getSnapshotInfo().failedShards());
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("test-repo-2")
                                                        .addSnapshots("test-snap-2").get();
        assertEquals(SnapshotState.SUCCESS, getSnapshotsResponse.getSnapshots().get(0).state());
    }

    public void testSnapshotStatusOnFailedIndex() throws Exception {
        logger.info("--> creating repository");
        final Path repoPath = randomRepoPath();
        final Client client = client();
        assertAcked(client.admin().cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setVerify(false)
            .setSettings(Settings.builder().put("location", repoPath)));

        logger.info("--> creating good index");
        assertAcked(prepareCreate("test-idx-good")
            .setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        final int numDocs = randomIntBetween(1, 5);
        for (int i = 0; i < numDocs; i++) {
            index("test-idx-good", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> creating bad index");
        assertAcked(prepareCreate("test-idx-bad")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                // set shard allocation to none so the primary cannot be
                // allocated - simulates a "bad" index that fails to snapshot
                .put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                     "none")));

        logger.info("--> snapshot bad index and get status");
        client.admin().cluster()
            .prepareCreateSnapshot("test-repo", "test-snap1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-bad")
            .get();
        SnapshotsStatusResponse snapshotsStatusResponse = client.admin().cluster()
            .prepareSnapshotStatus("test-repo")
            .setSnapshots("test-snap1")
            .get();
        assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
        assertEquals(State.FAILED, snapshotsStatusResponse.getSnapshots().get(0).getState());

        logger.info("--> snapshot both good and bad index and get status");
        client.admin().cluster()
            .prepareCreateSnapshot("test-repo", "test-snap2")
            .setWaitForCompletion(true)
            .setIndices("test-idx-good", "test-idx-bad")
            .get();
        snapshotsStatusResponse = client.admin().cluster()
            .prepareSnapshotStatus("test-repo")
            .setSnapshots("test-snap2")
            .get();
        assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
        // verify a FAILED status is returned instead of a 500 status code
        // see https://github.com/elastic/elasticsearch/issues/23716
        SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
        assertEquals(State.FAILED, snapshotStatus.getState());
        for (SnapshotIndexShardStatus shardStatus : snapshotStatus.getShards()) {
            assertEquals(SnapshotIndexShardStage.FAILURE, shardStatus.getStage());
            if (shardStatus.getIndex().equals("test-idx-good")) {
                assertEquals("skipped", shardStatus.getFailure());
            } else {
                assertEquals("primary shard is not allocated", shardStatus.getFailure());
            }
        }
    }

    public void testGetSnapshotsFromIndexBlobOnly() throws Exception {
        logger.info("--> creating repository");
        final Path repoPath = randomRepoPath();
        final Client client = client();
        assertAcked(client.admin().cluster()
            .preparePutRepository("test-repo")
            .setType("fs")
            .setVerify(false)
            .setSettings(Settings.builder().put("location", repoPath)));

        logger.info("--> creating random number of indices");
        final int numIndices = randomIntBetween(1, 10);
        for (int i = 0; i < numIndices; i++) {
            assertAcked(prepareCreate("test-idx-" + i).setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        }

        logger.info("--> creating random number of snapshots");
        final int numSnapshots = randomIntBetween(1, 10);
        final Map<String, List<String>> indicesPerSnapshot = new HashMap<>();
        for (int i = 0; i < numSnapshots; i++) {
            // index some additional docs (maybe) for each index
            for (int j = 0; j < numIndices; j++) {
                if (randomBoolean()) {
                    final int numDocs = randomIntBetween(1, 5);
                    for (int k = 0; k < numDocs; k++) {
                        index("test-idx-" + j, "doc", Integer.toString(k), "foo", "bar" + k);
                    }
                    refresh();
                }
            }
            final boolean all = randomBoolean();
            boolean atLeastOne = false;
            List<String> indices = new ArrayList<>();
            for (int j = 0; j < numIndices; j++) {
                if (all || randomBoolean() || !atLeastOne) {
                    indices.add("test-idx-" + j);
                    atLeastOne = true;
                }
            }
            final String snapshotName = "test-snap-" + i;
            indicesPerSnapshot.put(snapshotName, indices);
            client.admin().cluster()
                .prepareCreateSnapshot("test-repo", snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indices.toArray(new String[indices.size()]))
                .get();
        }

        logger.info("--> verify _all returns snapshot info");
        GetSnapshotsResponse response = client().admin().cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots("_all")
            .setVerbose(false)
            .get();
        assertEquals(indicesPerSnapshot.size(), response.getSnapshots().size());
        verifySnapshotInfo(response, indicesPerSnapshot);

        logger.info("--> verify wildcard returns snapshot info");
        response = client().admin().cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap-*")
            .setVerbose(false)
            .get();
        assertEquals(indicesPerSnapshot.size(), response.getSnapshots().size());
        verifySnapshotInfo(response, indicesPerSnapshot);

        logger.info("--> verify individual requests return snapshot info");
        for (int i = 0; i < numSnapshots; i++) {
            response = client().admin().cluster()
                .prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap-" + i)
                .setVerbose(false)
                .get();
            assertEquals(1, response.getSnapshots().size());
            verifySnapshotInfo(response, indicesPerSnapshot);
        }
    }

    public void testSnapshottingWithMissingSequenceNumbers() {
        final String repositoryName = "test-repo";
        final String snapshotName = "test-snap";
        final String indexName = "test-idx";
        final Client client = client();
        final Path repo = randomRepoPath();

        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repositoryName)
            .setType("fs").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        logger.info("--> creating an index and indexing documents");
        final String dataNode = internalCluster().getDataNodeInstance(ClusterService.class).localNode().getName();
        final Settings settings =
            Settings
                .builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.include._name", dataNode)
                .build();
        createIndex(indexName, settings);
        ensureGreen();
        for (int i = 0; i < 5; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }

        final Index index = resolveIndex(indexName);
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, dataNode).getShardOrNull(new ShardId(index, 0));
        // create a gap in the sequence numbers
        getEngineFromShard(primary).seqNoService().generateSeqNo();

        for (int i = 5; i < 10; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }

        refresh();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true).setIndices(indexName).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete indices");
        assertAcked(client.admin().indices().prepareDelete(indexName));

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        logger.info("--> indexing some more");
        for (int i = 10; i < 15; i++) {
            index(indexName, "doc", Integer.toString(i), "foo", "bar" + i);
        }

        IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().get();
        ShardStats shardStats = stats.getShards()[0];
        assertTrue(shardStats.getShardRouting().primary());
        assertThat(shardStats.getSeqNoStats().getLocalCheckpoint(), equalTo(15L)); // 15 indexed docs and one "missing" op.
        assertThat(shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(15L));
        assertThat(shardStats.getSeqNoStats().getMaxSeqNo(), equalTo(15L));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/27974")
    public void testAbortedSnapshotDuringInitDoesNotStart() throws Exception {
        final Client client = client();

        // Blocks on initialization
        assertAcked(client.admin().cluster().preparePutRepository("repository")
            .setType("mock").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("block_on_init", true)
            ));

        createIndex("test-idx");
        final int nbDocs = scaledRandomIntBetween(100, 500);
        for (int i = 0; i < nbDocs; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits(), equalTo((long) nbDocs));

        // Create a snapshot
        client.admin().cluster().prepareCreateSnapshot("repository", "snap").execute();
        waitForBlock(internalCluster().getMasterName(), "repository", TimeValue.timeValueMinutes(1));
        boolean blocked = true;

        // Snapshot is initializing (and is blocked at this stage)
        SnapshotsStatusResponse snapshotsStatus = client.admin().cluster().prepareSnapshotStatus("repository").setSnapshots("snap").get();
        assertThat(snapshotsStatus.getSnapshots().iterator().next().getState(), equalTo(State.INIT));

        final List<State> states = new CopyOnWriteArrayList<>();
        final ClusterStateListener listener = event -> {
            SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                if ("snap".equals(entry.snapshot().getSnapshotId().getName())) {
                    states.add(entry.state());
                }
            }
        };

        try {
            // Record the upcoming states of the snapshot on all nodes
            internalCluster().getInstances(ClusterService.class).forEach(clusterService -> clusterService.addListener(listener));

            // Delete the snapshot while it is being initialized
            ActionFuture<DeleteSnapshotResponse> delete = client.admin().cluster().prepareDeleteSnapshot("repository", "snap").execute();

            // The deletion must set the snapshot in the ABORTED state
            assertBusy(() -> {
                SnapshotsStatusResponse status = client.admin().cluster().prepareSnapshotStatus("repository").setSnapshots("snap").get();
                assertThat(status.getSnapshots().iterator().next().getState(), equalTo(State.ABORTED));
            });

            // Now unblock the repository
            unblockNode("repository", internalCluster().getMasterName());
            blocked = false;

            assertAcked(delete.get());
            expectThrows(SnapshotMissingException.class, () ->
                client.admin().cluster().prepareGetSnapshots("repository").setSnapshots("snap").get());

            assertFalse("Expecting snapshot state to be updated", states.isEmpty());
            assertFalse("Expecting snapshot to be aborted and not started at all", states.contains(State.STARTED));
        } finally {
            internalCluster().getInstances(ClusterService.class).forEach(clusterService -> clusterService.removeListener(listener));
            if (blocked) {
                unblockNode("repository", internalCluster().getMasterName());
            }
        }
    }

    private void verifySnapshotInfo(final GetSnapshotsResponse response, final Map<String, List<String>> indicesPerSnapshot) {
        for (SnapshotInfo snapshotInfo : response.getSnapshots()) {
            final List<String> expected = snapshotInfo.indices();
            assertEquals(expected, indicesPerSnapshot.get(snapshotInfo.snapshotId().getName()));
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        }
    }
}
