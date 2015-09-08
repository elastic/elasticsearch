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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ListenableActionFuture;
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
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.Entry;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.shard.IndexShard.INDEX_REFRESH_INTERVAL;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class SharedClusterSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Test
    public void basicWorkFlowTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertHitCount(client.prepareCount("test-idx-1").get(), 100L);
        assertHitCount(client.prepareCount("test-idx-2").get(), 100L);
        assertHitCount(client.prepareCount("test-idx-3").get(), 100L);

        ListenableActionFuture<FlushResponse> flushResponseFuture = null;
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
        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-3").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        SnapshotInfo snapshotInfo = client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0);
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
        assertHitCount(client.prepareCount("test-idx-1").get(), 50L);
        assertHitCount(client.prepareCount("test-idx-2").get(), 50L);
        assertHitCount(client.prepareCount("test-idx-3").get(), 50L);

        logger.info("--> close indices");
        client.admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        for (int i=0; i<5; i++) {
            assertHitCount(client.prepareCount("test-idx-1").get(), 100L);
            assertHitCount(client.prepareCount("test-idx-2").get(), 100L);
            assertHitCount(client.prepareCount("test-idx-3").get(), 50L);
        }

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-2").execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        for (int i=0; i<5; i++) {
            assertHitCount(client.prepareCount("test-idx-1").get(), 100L);
        }
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(false));

        if (flushResponseFuture != null) {
            // Finish flush
            flushResponseFuture.actionGet();
        }
    }


    @Test
    public void singleGetAfterRestoreTest() throws Exception {
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
                .setType("fs").setSettings(Settings.settingsBuilder()
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

    @Test
    public void testFreshIndexUUID() {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

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

    @Test
    public void restoreWithDifferentMappingsAndSettingsTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        logger.info("--> create index with foo type");
        assertAcked(prepareCreate("test-idx", 2, Settings.builder()
                .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        NumShards numShards = getNumShards("test-idx");

        assertAcked(client().admin().indices().preparePutMapping("test-idx").setType("foo").setSource("baz", "type=string"));
        ensureGreen();

        logger.info("--> snapshot it");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete the index and recreate it with bar type");
        cluster().wipeIndices("test-idx");
        assertAcked(prepareCreate("test-idx", 2, Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, numShards.numPrimaries).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 5, TimeUnit.SECONDS)));
        assertAcked(client().admin().indices().preparePutMapping("test-idx").setType("bar").setSource("baz", "type=string"));
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

    @Test
    public void emptySnapshotTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", randomRepoPath())).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));

        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
    }

    @Test
    public void restoreAliasesTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", randomRepoPath())));

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

    @Test
    public void restoreTemplatesTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", randomRepoPath())));

        logger.info("-->  creating test template");
        assertThat(client.admin().indices().preparePutTemplate("test-template").setTemplate("te*").addMapping("test-mapping", "{}").get().isAcknowledged(), equalTo(true));

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

    @Test
    public void includeGlobalStateTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path location = randomRepoPath();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", location)));

        logger.info("-->  creating test template");
        assertThat(client.admin().indices().preparePutTemplate("test-template").setTemplate("te*").addMapping("test-mapping", "{}").get().isAcknowledged(), equalTo(true));

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

        logger.info("-->  delete test template");
        cluster().wipeTemplates("test-template");
        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> try restoring cluster state from snapshot without global state");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        logger.info("--> check that template wasn't restored");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> restore cluster state");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-with-global-state").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        logger.info("--> check that template is restored");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateExists(getIndexTemplatesResponse, "test-template");

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> snapshot without global state but with indices");
        createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-no-global-state-with-index").setIndices("test-idx").setIncludeGlobalState(false).setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-no-global-state-with-index").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("-->  delete test template and index ");
        cluster().wipeIndices("test-idx");
        cluster().wipeTemplates("test-template");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> try restoring index and cluster state from snapshot without global state");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state-with-index").setWaitForCompletion(true).setRestoreGlobalState(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        logger.info("--> check that template wasn't restored but index was");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

    }

    @Test
    public void snapshotFileFailureDuringSnapshotTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.settingsBuilder()
                                .put("location", randomRepoPath())
                                .put("random", randomAsciiOfLength(10))
                                .put("random_control_io_exception_rate", 0.2))
                .setVerify(false));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

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
            assertThat(getFailureCount("test-repo"), greaterThan(0L));
            assertThat(ExceptionsHelper.detailedMessage(ex), containsString("IOException"));
        }
    }

    @Test
    public void dataFileFailureDuringSnapshotTest() throws Exception {
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.settingsBuilder()
                                .put("location", randomRepoPath())
                                .put("random", randomAsciiOfLength(10))
                                .put("random_data_file_io_exception_rate", 0.3)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

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

    @Test
    public void dataFileFailureDuringRestoreTest() throws Exception {
        Path repositoryLocation = randomRepoPath();
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", repositoryLocation)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(createSnapshotResponse.getSnapshotInfo().successfulShards()));

        logger.info("-->  update repository with mock version");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.settingsBuilder()
                                .put("location", repositoryLocation)
                                .put("random", randomAsciiOfLength(10))
                                .put("random_data_file_io_exception_rate", 0.3)));

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        CountResponse countResponse = client.prepareCount("test-idx").get();
        assertThat(countResponse.getCount(), equalTo(100L));
        logger.info("--> total number of simulated failures during restore: [{}]", getFailureCount("test-repo"));
    }


    @Test
    public void deletionOfFailingToRecoverIndexShouldStopRestore() throws Exception {
        Path repositoryLocation = randomRepoPath();
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", repositoryLocation)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(createSnapshotResponse.getSnapshotInfo().successfulShards()));

        logger.info("-->  update repository with mock version");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.settingsBuilder()
                                .put("location", repositoryLocation)
                                .put("random", randomAsciiOfLength(10))
                                .put("random_data_file_io_exception_rate", 1.0) // Fail completely
                ));

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore index after deletion");
        ListenableActionFuture<RestoreSnapshotResponse> restoreSnapshotResponseFuture =
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
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", repositoryLocation)));

        logger.info("--> trying to restore index again");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        CountResponse countResponse = client.prepareCount("test-idx").get();
        assertThat(countResponse.getCount(), equalTo(100L));

    }

    @Test
    public void unallocatedShardsTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())));

        logger.info("-->  creating index that cannot be allocated");
        prepareCreate("test-idx", 2, Settings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + ".tag", "nowhere").put("index.number_of_shards", 3)).get();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.FAILED));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(3));
        assertThat(createSnapshotResponse.getSnapshotInfo().reason(), startsWith("Indices don't have primary shards"));
    }

    @Test
    public void deleteSnapshotTest() throws Exception {
        final int numberOfSnapshots = between(5, 15);
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at " + repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
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
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(10L * numberOfSnapshots));
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

        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(10L * numberOfSnapshots));

        logger.info("--> delete the last snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", lastSnapshot).get();
        logger.info("--> make sure that number of files is back to what it was when the first snapshot was made");
        assertThat(numberOfFiles(repo), equalTo(numberOfFiles[0]));
    }

    @Test
    public void deleteSnapshotWithMissingIndexAndShardMetadataTest() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at " + repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        ensureYellow();
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

    @Test
    public void deleteSnapshotWithMissingMetadataTest() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at " + repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        ensureYellow();
        logger.info("--> indexing some data");
        indexRandom(true,
                client().prepareIndex("test-idx-1", "doc").setSource("foo", "bar"),
                client().prepareIndex("test-idx-2", "doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete index metadata and shard metadata");
        Path metadata = repo.resolve("meta-test-snap-1.dat");
        Files.delete(metadata);

        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1"), SnapshotMissingException.class);
    }

    @Test
    public void deleteSnapshotWithCorruptedSnapshotFileTest() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        logger.info("-->  creating repository at " + repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx-1", "test-idx-2");
        ensureYellow();
        logger.info("--> indexing some data");
        indexRandom(true,
                client().prepareIndex("test-idx-1", "doc").setSource("foo", "bar"),
                client().prepareIndex("test-idx-2", "doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> truncate snapshot file to make it unreadable");
        Path snapshotPath = repo.resolve("snap-test-snap-1.dat");
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


    @Test
    public void snapshotClosedIndexTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())));

        createIndex("test-idx", "test-idx-closed");
        ensureGreen();
        logger.info("-->  closing index test-idx-closed");
        assertAcked(client.admin().indices().prepareClose("test-idx-closed"));
        ClusterStateResponse stateResponse = client.admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metaData().index("test-idx-closed").state(), equalTo(IndexMetaData.State.CLOSE));
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

    @Test
    public void snapshotSingleClosedIndexTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())));

        createIndex("test-idx");
        ensureGreen();
        logger.info("-->  closing index test-idx");
        assertAcked(client.admin().indices().prepareClose("test-idx"));

        logger.info("--> snapshot");
        assertBlocked(client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setWaitForCompletion(true).setIndices("test-idx"), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
    }

    @Test
    public void renameOnRestoreTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
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
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-2").get().getCount(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-1", "test-idx-2").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> restore indices with different names");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareCount("test-idx-1-copy").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-2-copy").get().getCount(), equalTo(100L));

        logger.info("--> close just restored indices");
        client.admin().indices().prepareClose("test-idx-1-copy", "test-idx-2-copy").get();

        logger.info("--> and try to restore these indices again");
        restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareCount("test-idx-1-copy").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-2-copy").get().getCount(), equalTo(100L));


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

    @Test
    public void moveShardWhileSnapshottingTest() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.settingsBuilder()
                                .put("location", repositoryLocation)
                                .put("random", randomAsciiOfLength(10))
                                .put("wait_after_unblock", 200)));

        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_replicas", 0)));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], moving shards away from this node", blockedNode);
        Settings.Builder excludeSettings = Settings.builder().put("index.routing.allocation.exclude._name", blockedNode);
        client().admin().indices().prepareUpdateSettings("test-idx").setSettings(excludeSettings).get();

        logger.info("--> unblocking blocked node");
        unblockNode(blockedNode);
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
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", repositoryLocation)));

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));
    }

    @Test
    public void deleteRepositoryWhileSnapshottingTest() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.settingsBuilder()
                                .put("location", repositoryLocation)
                                .put("random", randomAsciiOfLength(10))
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
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-idx");

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
                    .setType("fs").setSettings(Settings.settingsBuilder().put("location", repositoryLocation.resolve("test"))
            ).get();
            fail("shouldn't be able to replace in-use repository");
        } catch (Exception ex) {
            logger.info("--> in-use repository replacement failed");
        }

        logger.info("--> trying to create a repository with different name");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo-2")
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", repositoryLocation.resolve("test"))));

        logger.info("--> unblocking blocked node");
        unblockNode(blockedNode);
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
                .setType("fs").setSettings(Settings.settingsBuilder().put("location", repositoryLocation)));

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));
    }

    @Test
    public void urlRepositoryTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
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
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> create read-only URL repository");
        assertAcked(client.admin().cluster().preparePutRepository("url-repo")
                .setType("url").setSettings(Settings.settingsBuilder()
                        .put("url", repositoryLocation.toUri().toURL())
                        .put("list_directories", randomBoolean())));
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("url-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> list available shapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));

        logger.info("--> delete snapshot");
        DeleteSnapshotResponse deleteSnapshotResponse = client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").get();
        assertAcked(deleteSnapshotResponse);

        logger.info("--> list available shapshot again, no snapshots should be returned");
        getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(0));
    }


    @Test
    public void readonlyRepositoryTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
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
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", repositoryLocation)
                        .put("compress", randomBoolean())
                        .put("readonly", true)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("readonly-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> list available shapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("readonly-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));

        logger.info("--> try deleting snapshot");
        assertThrows(client.admin().cluster().prepareDeleteSnapshot("readonly-repo", "test-snap"), RepositoryException.class, "cannot delete snapshot from a readonly repository");

        logger.info("--> try making another snapshot");
        assertThrows(client.admin().cluster().prepareCreateSnapshot("readonly-repo", "test-snap-2").setWaitForCompletion(true).setIndices("test-idx"), RepositoryException.class, "cannot create snapshot in a readonly repository");
    }

    @Test
    public void throttlingTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        boolean throttleSnapshot = randomBoolean();
        boolean throttleRestore = randomBoolean();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", repositoryLocation)
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                        .put("max_restore_bytes_per_sec", throttleRestore ? "0.5k" : "0")
                        .put("max_snapshot_bytes_per_sec", throttleSnapshot ? "0.5k" : "0")));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        long snapshotPause = 0L;
        long restorePause = 0L;
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            snapshotPause += repositoriesService.repository("test-repo").snapshotThrottleTimeInNanos();
            restorePause += repositoriesService.repository("test-repo").restoreThrottleTimeInNanos();
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


    @Test
    public void snapshotStatusTest() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.settingsBuilder()
                                .put("location", repositoryLocation)
                                .put("random", randomAsciiOfLength(10))
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
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-idx");

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

        logger.info("--> checking snapshot status for all currently running and snapshot with empty repository", blockedNode);
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

        logger.info("--> checking that _current returns the currently running snapshot", blockedNode);
        GetSnapshotsResponse getResponse = client.admin().cluster().prepareGetSnapshots("test-repo").setCurrentSnapshot().execute().actionGet();
        assertThat(getResponse.getSnapshots().size(), equalTo(1));
        SnapshotInfo snapshotInfo = getResponse.getSnapshots().get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.IN_PROGRESS));

        logger.info("--> unblocking blocked node");
        unblockNode(blockedNode);

        snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");


        logger.info("--> checking snapshot status again after snapshot is done", blockedNode);
        response = client.admin().cluster().prepareSnapshotStatus("test-repo").addSnapshots("test-snap").execute().actionGet();
        snapshotStatus = response.getSnapshots().get(0);
        assertThat(snapshotStatus.getIndices().size(), equalTo(1));
        SnapshotIndexStatus indexStatus = snapshotStatus.getIndices().get("test-idx");
        assertThat(indexStatus, notNullValue());
        assertThat(indexStatus.getShardsStats().getInitializingShards(), equalTo(0));
        assertThat(indexStatus.getShardsStats().getFailedShards(), equalTo(snapshotInfo.failedShards()));
        assertThat(indexStatus.getShardsStats().getDoneShards(), equalTo(snapshotInfo.successfulShards()));
        assertThat(indexStatus.getShards().size(), equalTo(snapshotInfo.totalShards()));

        logger.info("--> checking snapshot status after it is done with empty repository", blockedNode);
        response = client.admin().cluster().prepareSnapshotStatus().execute().actionGet();
        assertThat(response.getSnapshots().size(), equalTo(0));

        logger.info("--> checking that _current no longer returns the snapshot", blockedNode);
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("_current").execute().actionGet().getSnapshots().isEmpty(), equalTo(true));

        try {
            client.admin().cluster().prepareSnapshotStatus("test-repo").addSnapshots("test-snap-doesnt-exist").execute().actionGet();
            fail();
        } catch (SnapshotMissingException ex) {
            // Expected
        }
    }


    @Test
    public void snapshotRelocatingPrimary() throws Exception {
        Client client = client();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
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
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        // Update settings to make sure that relocation is slow so we can start snapshot before relocation is finished
        assertAcked(client.admin().indices().prepareUpdateSettings("test-idx").setSettings(Settings.builder()
                        .put(IndexStore.INDEX_STORE_THROTTLE_TYPE, "all")
                        .put(IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, 100, ByteSizeUnit.BYTES)
        ));

        logger.info("--> start relocations");
        allowNodes("test-idx", internalCluster().numDataNodes());

        logger.info("--> wait for relocations to start");

        waitForRelocationsToStart("test-idx", TimeValue.timeValueMillis(300));

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        // Update settings to back to normal
        assertAcked(client.admin().indices().prepareUpdateSettings("test-idx").setSettings(Settings.builder()
                        .put(IndexStore.INDEX_STORE_THROTTLE_TYPE, "node")
        ));

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
                .setType("fs").setSettings(Settings.settingsBuilder()
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
        assertNoFailures(client().admin().indices().prepareOptimize("test").setFlush(true).setMaxNumSegments(1).get());

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

    @Test
    public void changeSettingsOnRestoreTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        logger.info("--> create test index with synonyms search analyzer");

        Settings.Builder indexSettings = Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))
                .put(INDEX_REFRESH_INTERVAL, "10s")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                .putArray("index.analysis.analyzer.my_analyzer.filter", "lowercase", "my_synonym")
                .put("index.analysis.filter.my_synonym.type", "synonym")
                .put("index.analysis.filter.my_synonym.synonyms", "foo => bar");

        assertAcked(prepareCreate("test-idx", 2, indexSettings));

        int numberOfShards = getNumShards("test-idx").numPrimaries;
        assertAcked(client().admin().indices().preparePutMapping("test-idx").setType("type1").setSource("field1", "type=string,analyzer=standard,search_analyzer=my_analyzer"));
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx", "type1", Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        assertHitCount(client.prepareCount("test-idx").setQuery(matchQuery("field1", "foo")).get(), numdocs);
        assertHitCount(client.prepareCount("test-idx").setQuery(matchQuery("field1", "bar")).get(), numdocs);

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
        assertThat(getSettingsResponse.getSetting("test-idx", INDEX_REFRESH_INTERVAL), equalTo("5s"));
        // Make sure that number of shards didn't change
        assertThat(getSettingsResponse.getSetting("test-idx", SETTING_NUMBER_OF_SHARDS), equalTo("" + numberOfShards));
        assertThat(getSettingsResponse.getSetting("test-idx", "index.analysis.analyzer.my_analyzer.type"), equalTo("standard"));
        assertThat(getSettingsResponse.getSetting("test-idx", "index.analysis.filter.my_synonym.type"), nullValue());

        assertHitCount(client.prepareCount("test-idx").setQuery(matchQuery("field1", "foo")).get(), 0);
        assertHitCount(client.prepareCount("test-idx").setQuery(matchQuery("field1", "bar")).get(), numdocs);

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
        assertThat(getSettingsResponse.getSetting("test-idx", INDEX_REFRESH_INTERVAL), equalTo("5s"));
        // Make sure that number of shards didn't change
        assertThat(getSettingsResponse.getSetting("test-idx", SETTING_NUMBER_OF_SHARDS), equalTo("" + numberOfShards));

        assertHitCount(client.prepareCount("test-idx").setQuery(matchQuery("field1", "foo")).get(), 0);
        assertHitCount(client.prepareCount("test-idx").setQuery(matchQuery("field1", "bar")).get(), numdocs);

    }

    @Test
    public void deleteIndexDuringSnapshotTest() throws Exception {
        Client client = client();

        boolean allowPartial = randomBoolean();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                        .put("block_on_init", true)
                ));

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-2").get().getCount(), equalTo(100L));
        assertThat(client.prepareCount("test-idx-3").get().getCount(), equalTo(100L));

        logger.info("--> snapshot allow partial {}", allowPartial);
        ListenableActionFuture<CreateSnapshotResponse> future = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                .setIndices("test-idx-*").setWaitForCompletion(true).setPartial(allowPartial).execute();
        logger.info("--> wait for block to kick in");
        waitForBlock(internalCluster().getMasterName(), "test-repo", TimeValue.timeValueMinutes(1));
        logger.info("--> delete some indices while snapshot is running");
        client.admin().indices().prepareDelete("test-idx-1", "test-idx-2").get();
        logger.info("--> unblock running master node");
        unblockNode(internalCluster().getMasterName());
        logger.info("--> waiting for snapshot to finish");
        CreateSnapshotResponse createSnapshotResponse = future.get();

        if (allowPartial) {
            logger.info("Deleted index during snapshot, but allow partial");
            assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo((SnapshotState.PARTIAL)));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().failedShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), lessThan(createSnapshotResponse.getSnapshotInfo().totalShards()));
        } else {
            logger.info("Deleted index during snapshot and doesn't allow partial");
            assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo((SnapshotState.FAILED)));
        }
    }


    @Test
    public void deleteOrphanSnapshotTest() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(Settings.settingsBuilder()
                                .put("location", randomRepoPath())
                                .put("compress", randomBoolean())
                                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                ));

        createIndex("test-idx");
        ensureGreen();

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> emulate an orphan snapshot");

        clusterService.submitStateUpdateTask("orphan snapshot test", new ProcessedClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                // Simulate orphan snapshot
                ImmutableMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableMap.builder();
                shards.put(new ShardId("test-idx", 0), new ShardSnapshotStatus("unknown-node", State.ABORTED));
                shards.put(new ShardId("test-idx", 1), new ShardSnapshotStatus("unknown-node", State.ABORTED));
                shards.put(new ShardId("test-idx", 2), new ShardSnapshotStatus("unknown-node", State.ABORTED));
                List<Entry> entries = new ArrayList<>();
                entries.add(new Entry(new SnapshotId("test-repo", "test-snap"), true, State.ABORTED, Collections.singletonList("test-idx"), System.currentTimeMillis(), shards.build()));
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(Collections.unmodifiableList(entries))).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                fail();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        logger.info("--> try deleting the orphan snapshot");

        assertAcked(client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").get("10s"));

    }

    private boolean waitForIndex(final String index, TimeValue timeout) throws InterruptedException {
        return awaitBusy(() -> client().admin().indices().prepareExists(index).execute().actionGet().isExists(), timeout.millis(), TimeUnit.MILLISECONDS);
    }

    private boolean waitForRelocationsToStart(final String index, TimeValue timeout) throws InterruptedException {
        return awaitBusy(() -> client().admin().cluster().prepareHealth(index).execute().actionGet().getRelocatingShards() > 0, timeout.millis(), TimeUnit.MILLISECONDS);
    }

    @Test
    @TestLogging("cluster:DEBUG")
    public void batchingShardUpdateTaskTest() throws Exception {

        final Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        assertAcked(prepareCreate("test-idx", 0, settingsBuilder().put("number_of_shards", between(1, 20))
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx", "type1", Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        final ClusterService clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        BlockingClusterStateListener snapshotListener = new BlockingClusterStateListener(clusterService, "update_snapshot [", "update snapshot state", Priority.HIGH);
        try {
            clusterService.addFirst(snapshotListener);
            logger.info("--> snapshot");
            ListenableActionFuture<CreateSnapshotResponse> snapshotFuture = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").execute();

            // Await until shard updates are in pending state.
            assertBusyPendingTasks("update snapshot state", numberOfShards);
            snapshotListener.unblock();

            // Check that the snapshot was successful
            CreateSnapshotResponse createSnapshotResponse = snapshotFuture.actionGet();
            assertEquals(SnapshotState.SUCCESS, createSnapshotResponse.getSnapshotInfo().state());
            assertEquals(numberOfShards, createSnapshotResponse.getSnapshotInfo().totalShards());
            assertEquals(numberOfShards, createSnapshotResponse.getSnapshotInfo().successfulShards());

        } finally {
            clusterService.remove(snapshotListener);
        }

        // Check that we didn't timeout
        assertFalse(snapshotListener.timedOut());
        // Check that cluster state update task was called only once
        assertEquals(1, snapshotListener.count());

        logger.info("--> close indices");
        client.admin().indices().prepareClose("test-idx").get();

        BlockingClusterStateListener restoreListener = new BlockingClusterStateListener(clusterService, "restore_snapshot[", "update snapshot state", Priority.HIGH);

        try {
            clusterService.addFirst(restoreListener);
            logger.info("--> restore snapshot");
            ListenableActionFuture<RestoreSnapshotResponse> futureRestore = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute();

            // Await until shard updates are in pending state.
            assertBusyPendingTasks("update snapshot state", numberOfShards);
            restoreListener.unblock();

            RestoreSnapshotResponse restoreSnapshotResponse = futureRestore.actionGet();
            assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(numberOfShards));

        } finally {
            clusterService.remove(restoreListener);
        }

        // Check that we didn't timeout
        assertFalse(restoreListener.timedOut());
        // Check that cluster state update task was called only once
        assertEquals(1, restoreListener.count());
    }

    @Test
    public void snapshotNameTest() throws Exception {

        final Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        try {
            client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("_foo").get();
            fail("shouldn't be here");
        } catch (InvalidSnapshotNameException ex) {
            assertThat(ex.getMessage(), containsString("Invalid snapshot name"));
        }

        try {
            client.admin().cluster().prepareCreateSnapshot("test-repo", "_foo").get();
            fail("shouldn't be here");
        } catch (InvalidSnapshotNameException ex) {
            assertThat(ex.getMessage(), containsString("Invalid snapshot name"));
        }

        try {
            client.admin().cluster().prepareDeleteSnapshot("test-repo", "_foo").get();
            fail("shouldn't be here");
        } catch (InvalidSnapshotNameException ex) {
            assertThat(ex.getMessage(), containsString("Invalid snapshot name"));
        }

        try {
            client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("_foo").get();
            fail("shouldn't be here");
        } catch (InvalidSnapshotNameException ex) {
            assertThat(ex.getMessage(), containsString("Invalid snapshot name"));
        }
    }
}