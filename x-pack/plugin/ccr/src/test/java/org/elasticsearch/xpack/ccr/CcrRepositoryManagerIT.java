/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

// TODO: Fold this integration test into a more expansive integration test as more bootstrap from remote work
// TODO: is completed.
public class CcrRepositoryManagerIT extends CcrIntegTestCase {

    public void testThatRepositoryIsPutAndRemovedWhenRemoteClusterIsUpdated() throws Exception {
        String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";
        assertBusy(() -> {
            final RepositoriesService repositoriesService =
                getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
            try {
                Repository repository = repositoriesService.repository(leaderClusterRepoName);
                assertEquals(CcrRepository.TYPE, repository.getMetadata().type());
                assertEquals(leaderClusterRepoName, repository.getMetadata().name());
            } catch (RepositoryMissingException e) {
                fail("need repository");
            }
        });

        ClusterUpdateSettingsRequest putFollowerRequest = new ClusterUpdateSettingsRequest();
        String address = getFollowerCluster().getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        putFollowerRequest.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(putFollowerRequest).actionGet());

        String followerCopyRepoName = CcrRepository.NAME_PREFIX + "follower_cluster_copy";
        assertBusy(() -> {
            final RepositoriesService repositoriesService =
                getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
            try {
                Repository repository = repositoriesService.repository(followerCopyRepoName);
                assertEquals(CcrRepository.TYPE, repository.getMetadata().type());
                assertEquals(followerCopyRepoName, repository.getMetadata().name());
            } catch (RepositoryMissingException e) {
                fail("need repository");
            }
        });

        ClusterUpdateSettingsRequest deleteLeaderRequest = new ClusterUpdateSettingsRequest();
        deleteLeaderRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(deleteLeaderRequest).actionGet());

        assertBusy(() -> {
            final RepositoriesService repositoriesService =
                getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
            expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(leaderClusterRepoName));
        });

        ClusterUpdateSettingsRequest deleteFollowerRequest = new ClusterUpdateSettingsRequest();
        deleteFollowerRequest.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(deleteFollowerRequest).actionGet());

        assertBusy(() -> {
            final RepositoriesService repositoriesService =
                getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
            expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(followerCopyRepoName));
        });
    }
}
