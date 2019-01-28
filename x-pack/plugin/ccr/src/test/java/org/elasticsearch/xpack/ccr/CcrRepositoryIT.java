/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.action.repositories.GetCcrRestoreFileChunkAction;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.snapshots.RestoreService.restoreInProgress;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

// TODO: Fold this integration test into a more expansive integration test as more bootstrap from remote work
// TODO: is completed.
public class CcrRepositoryIT extends CcrIntegTestCase {

    private final IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    public void testThatRepositoryIsPutAndRemovedWhenRemoteClusterIsUpdated() throws Exception {
        String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";
        final RepositoriesService repositoriesService =
            getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
        try {
            Repository repository = repositoriesService.repository(leaderClusterRepoName);
            assertEquals(CcrRepository.TYPE, repository.getMetadata().type());
            assertEquals(leaderClusterRepoName, repository.getMetadata().name());
        } catch (RepositoryMissingException e) {
            fail("need repository");
        }

        ClusterUpdateSettingsRequest putSecondCluster = new ClusterUpdateSettingsRequest();
        String address = getFollowerCluster().getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        putSecondCluster.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(putSecondCluster).actionGet());

        String followerCopyRepoName = CcrRepository.NAME_PREFIX + "follower_cluster_copy";
        try {
            Repository repository = repositoriesService.repository(followerCopyRepoName);
            assertEquals(CcrRepository.TYPE, repository.getMetadata().type());
            assertEquals(followerCopyRepoName, repository.getMetadata().name());
        } catch (RepositoryMissingException e) {
            fail("need repository");
        }

        ClusterUpdateSettingsRequest deleteLeaderCluster = new ClusterUpdateSettingsRequest();
        deleteLeaderCluster.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(deleteLeaderCluster).actionGet());

        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(leaderClusterRepoName));

        ClusterUpdateSettingsRequest deleteSecondCluster = new ClusterUpdateSettingsRequest();
        deleteSecondCluster.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(deleteSecondCluster).actionGet());

        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(followerCopyRepoName));

        ClusterUpdateSettingsRequest putLeaderRequest = new ClusterUpdateSettingsRequest();
        address = getLeaderCluster().getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        putLeaderRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(putLeaderRequest).actionGet());
    }

    public void testThatRepositoryRecoversEmptyIndexBasedOnLeaderSettings() throws IOException {
        String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";
        String leaderIndex = "index1";
        String followerIndex = "index2";

        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen(leaderIndex);

        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followerIndex)
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(leaderIndex).indicesOptions(indicesOptions).renamePattern("^(.*)$")
            .renameReplacement(followerIndex).masterNodeTimeout(new TimeValue(1L, TimeUnit.HOURS))
            .indexSettings(settingsBuilder);

        PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));
        RestoreInfo restoreInfo = future.actionGet();

        assertEquals(restoreInfo.totalShards(), restoreInfo.successfulShards());
        assertEquals(0, restoreInfo.failedShards());

        ClusterStateResponse leaderState = leaderClient()
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetaData(true)
            .setIndices(leaderIndex)
            .get();
        ClusterStateResponse followerState = followerClient()
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetaData(true)
            .setIndices(followerIndex)
            .get();

        IndexMetaData leaderMetadata = leaderState.getState().metaData().index(leaderIndex);
        IndexMetaData followerMetadata = followerState.getState().metaData().index(followerIndex);
        assertEquals(leaderMetadata.getNumberOfShards(), followerMetadata.getNumberOfShards());
        Map<String, String> ccrMetadata = followerMetadata.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
        assertEquals(leaderIndex, ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY));
        assertEquals(leaderMetadata.getIndexUUID(), ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY));
        assertEquals("leader_cluster", ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY));
        assertEquals(followerIndex, followerMetadata.getSettings().get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(true, IndexSettings.INDEX_SOFT_DELETES_SETTING.get(followerMetadata.getSettings()));

        // UUID is changed so that we can follow indexes on same cluster
        assertNotEquals(leaderMetadata.getIndexUUID(), followerMetadata.getIndexUUID());
    }

    public void testDocsAreRecovered() throws Exception {
        String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";
        String leaderIndex = "index1";
        String followerIndex = "index2";

        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen(leaderIndex);

        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final int firstBatchNumDocs = randomIntBetween(1, 64);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();

        AtomicBoolean isRunning = new AtomicBoolean(true);

        // Concurrently index new docs with mapping changes
        Thread thread = new Thread(() -> {
            char[] chars = "abcdeghijklmnopqrstuvwxyz".toCharArray();
            for (char c : chars) {
                if (isRunning.get() == false) {
                    break;
                }
                final String source;
                long l = randomLongBetween(0, 50000);
                if (randomBoolean()) {
                    source = String.format(Locale.ROOT, "{\"%c\":%d}", c, l);
                } else {
                    source = String.format(Locale.ROOT, "{\"%c\":\"%d\"}", c, l);
                }
                for (int i = 64; i < 150; i++) {
                    if (isRunning.get() == false) {
                        break;
                    }
                    leaderClient().prepareIndex("index1", "doc", Long.toString(i)).setSource(source, XContentType.JSON).get();
                    if (rarely()) {
                        leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).get();
                    }
                }
                leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();
            }
        });
        thread.start();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followerIndex)
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(leaderIndex).indicesOptions(indicesOptions).renamePattern("^(.*)$")
            .renameReplacement(followerIndex).masterNodeTimeout(new TimeValue(1L, TimeUnit.HOURS))
            .indexSettings(settingsBuilder);

        PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));
        RestoreInfo restoreInfo = future.actionGet();

        assertEquals(restoreInfo.totalShards(), restoreInfo.successfulShards());
        assertEquals(0, restoreInfo.failedShards());
        for (int i = 0; i < firstBatchNumDocs; ++i) {
            assertExpectedDocument(followerIndex, i);
        }

        isRunning.set(false);
        thread.join();
    }

    public void testRateLimitingIsEmployed() throws Exception {
        boolean followerRateLimiting = randomBoolean();

        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(Settings.builder().put(CcrSettings.RECOVERY_MAX_BYTES_PER_SECOND.getKey(), "10K"));
        if (followerRateLimiting) {
            assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());
        } else {
            assertAcked(leaderClient().admin().cluster().updateSettings(settingsRequest).actionGet());
        }

        String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";
        String leaderIndex = "index1";
        String followerIndex = "index2";

        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen(leaderIndex);

        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        List<CcrRepository> repositories = new ArrayList<>();
        List<CcrRestoreSourceService> restoreSources = new ArrayList<>();

        for (RepositoriesService repositoriesService : getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class)) {
            Repository repository = repositoriesService.repository(leaderClusterRepoName);
            repositories.add((CcrRepository) repository);
        }
        for (CcrRestoreSourceService restoreSource : getLeaderCluster().getDataOrMasterNodeInstances(CcrRestoreSourceService.class)) {
            restoreSources.add(restoreSource);
        }

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followerIndex)
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(leaderIndex).indicesOptions(indicesOptions).renamePattern("^(.*)$")
            .renameReplacement(followerIndex).masterNodeTimeout(new TimeValue(1L, TimeUnit.HOURS))
            .indexSettings(settingsBuilder);

        PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));
        future.actionGet();

        if (followerRateLimiting) {
            assertTrue(repositories.stream().anyMatch(cr -> cr.getRestoreThrottleTimeInNanos() > 0));
        } else {
            assertTrue(restoreSources.stream().anyMatch(cr -> cr.getThrottleTime() > 0));
        }

        settingsRequest = new ClusterUpdateSettingsRequest();
        ByteSizeValue nullValue = null;
        settingsRequest.persistentSettings(Settings.builder().put(CcrSettings.RECOVERY_MAX_BYTES_PER_SECOND.getKey(), nullValue));
        if (followerRateLimiting) {
            assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());
        } else {
            assertAcked(leaderClient().admin().cluster().updateSettings(settingsRequest).actionGet());
        }
    }

    public void testIndividualActionsTimeout() throws Exception {
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        TimeValue timeValue = TimeValue.timeValueMillis(100);
        settingsRequest.persistentSettings(Settings.builder().put(CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.getKey(), timeValue));
        assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());

        String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";
        String leaderIndex = "index1";
        String followerIndex = "index2";

        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen(leaderIndex);

        List<MockTransportService> transportServices = new ArrayList<>();

        for (TransportService transportService : getFollowerCluster().getDataOrMasterNodeInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            transportServices.add(mockTransportService);
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(GetCcrRestoreFileChunkAction.NAME) == false) {
                    connection.sendRequest(requestId, action, request, options);
                }
            });
        }

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followerIndex)
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(leaderIndex).indicesOptions(indicesOptions).renamePattern("^(.*)$")
            .renameReplacement(followerIndex).masterNodeTimeout(new TimeValue(1L, TimeUnit.HOURS))
            .indexSettings(settingsBuilder);

        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);
        PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));

        // Depending on when the timeout occurs this can fail in two ways. If it times-out when fetching
        // metadata this will throw an exception. If it times-out when restoring a shard, the shard will
        // be marked as failed. Either one is a success for the purpose of this test.
        try {
            RestoreInfo restoreInfo = future.actionGet();
            assertEquals(0, restoreInfo.successfulShards());
            assertEquals(numberOfPrimaryShards, restoreInfo.failedShards());
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ElasticsearchTimeoutException.class));
        }


        for (MockTransportService transportService : transportServices) {
            transportService.clearAllRules();
        }

        settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(Settings.builder().put(CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.getKey(),
            (TimeValue) null));
        assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37887")
    public void testFollowerMappingIsUpdated() throws IOException {
        String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";
        String leaderIndex = "index1";
        String followerIndex = "index2";

        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen(leaderIndex);

        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followerIndex)
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(leaderIndex).indicesOptions(indicesOptions).renamePattern("^(.*)$")
            .renameReplacement(followerIndex).masterNodeTimeout(new TimeValue(1L, TimeUnit.HOURS))
            .indexSettings(settingsBuilder);

        // TODO: Eventually when the file recovery work is complete, we should test updated mappings by
        //  indexing to the leader while the recovery is happening. However, into order to that test mappings
        //  are updated prior to that work, we index documents in the clear session callback. This will
        //  ensure a mapping change prior to the final mapping check on the follower side.
        for (CcrRestoreSourceService restoreSourceService : getLeaderCluster().getDataNodeInstances(CcrRestoreSourceService.class)) {
            restoreSourceService.addCloseSessionListener(s -> {
                final String source = String.format(Locale.ROOT, "{\"k\":%d}", 1);
                leaderClient().prepareIndex("index1", "doc", Long.toString(1)).setSource(source, XContentType.JSON).get();
            });
        }

        PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));
        RestoreInfo restoreInfo = future.actionGet();

        assertEquals(restoreInfo.totalShards(), restoreInfo.successfulShards());
        assertEquals(0, restoreInfo.failedShards());

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);
        clusterStateRequest.indices(followerIndex);
        ClusterStateResponse clusterState = followerClient().admin().cluster().state(clusterStateRequest).actionGet();
        IndexMetaData followerIndexMetadata = clusterState.getState().metaData().index(followerIndex);
        assertEquals(2, followerIndexMetadata.getMappingVersion());

        MappingMetaData mappingMetaData = followerClient().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.k.type", mappingMetaData.sourceAsMap()), equalTo("long"));
    }

    private void assertExpectedDocument(String followerIndex, final int value) {
        final GetResponse getResponse = followerClient().prepareGet(followerIndex, "doc", Integer.toString(value)).get();
        assertTrue("Doc with id [" + value + "] is missing", getResponse.isExists());
        assertTrue((getResponse.getSource().containsKey("f")));
        assertThat(getResponse.getSource().get("f"), equalTo(value));
    }

    private ActionListener<RestoreService.RestoreCompletionResponse> waitForRestore(ClusterService clusterService,
                                                                                    ActionListener<RestoreInfo> listener) {
        return new ActionListener<RestoreService.RestoreCompletionResponse>() {
            @Override
            public void onResponse(RestoreService.RestoreCompletionResponse restoreCompletionResponse) {
                if (restoreCompletionResponse.getRestoreInfo() == null) {
                    final Snapshot snapshot = restoreCompletionResponse.getSnapshot();
                    final String uuid = restoreCompletionResponse.getUuid();

                    ClusterStateListener clusterStateListener = new ClusterStateListener() {
                        @Override
                        public void clusterChanged(ClusterChangedEvent changedEvent) {
                            final RestoreInProgress.Entry prevEntry = restoreInProgress(changedEvent.previousState(), uuid);
                            final RestoreInProgress.Entry newEntry = restoreInProgress(changedEvent.state(), uuid);
                            if (prevEntry == null) {
                                // When there is a master failure after a restore has been started, this listener might not be registered
                                // on the current master and as such it might miss some intermediary cluster states due to batching.
                                // Clean up listener in that case and acknowledge completion of restore operation to client.
                                clusterService.removeListener(this);
                                listener.onResponse(null);
                            } else if (newEntry == null) {
                                clusterService.removeListener(this);
                                ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards = prevEntry.shards();
                                RestoreInfo ri = new RestoreInfo(prevEntry.snapshot().getSnapshotId().getName(),
                                    prevEntry.indices(),
                                    shards.size(),
                                    shards.size() - RestoreService.failedShards(shards));
                                logger.debug("restore of [{}] completed", snapshot);
                                listener.onResponse(ri);
                            } else {
                                // restore not completed yet, wait for next cluster state update
                            }
                        }
                    };

                    clusterService.addListener(clusterStateListener);
                } else {
                    listener.onResponse(restoreCompletionResponse.getRestoreInfo());
                }
            }

            @Override
            public void onFailure(Exception t) {
                listener.onFailure(t);
            }
        };
    }
}
