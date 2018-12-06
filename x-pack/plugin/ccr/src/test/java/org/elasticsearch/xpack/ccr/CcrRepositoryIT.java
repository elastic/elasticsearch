/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

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

        final RestoreService restoreService = getFollowerCluster().getMasterNodeInstance(RestoreService.class);

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followerIndex)
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(leaderClusterRepoName,
            leaderIndex, new String[]{leaderIndex}, indicesOptions,
            "^(.*)$", followerIndex, Settings.EMPTY, new TimeValue(1, TimeUnit.HOURS), false,
            false, true, settingsBuilder.build(), new String[0],
            "restore_snapshot[" + leaderClusterRepoName + ":" + leaderIndex + "]");

        PlainActionFuture<RestoreService.RestoreCompletionResponse> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, future);
        future.actionGet();

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
}
