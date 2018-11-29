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

public class CcrRepositoryManagerIT extends CcrIntegTestCase {

    public void testThatRepositoryIsPutAndRemovedWhenRemoteClusterIsUpdated() throws Exception {
        assertBusy(() -> {
            final RepositoriesService repositoriesService =
                getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
            try {
                Repository repository = repositoriesService.repository("leader_cluster");
                assertEquals(CcrRepository.TYPE, repository.getMetadata().type());
                assertEquals("leader_cluster", repository.getMetadata().name());
            } catch (RepositoryMissingException e) {
                fail("need repository");
            }
        });

        ClusterUpdateSettingsRequest putFollowerRequest = new ClusterUpdateSettingsRequest();
        String address = getFollowerCluster().getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        putFollowerRequest.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(putFollowerRequest).actionGet());

        assertBusy(() -> {
            final RepositoriesService repositoriesService =
                getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
            try {
                Repository repository = repositoriesService.repository("follower_cluster_copy");
                assertEquals(CcrRepository.TYPE, repository.getMetadata().type());
                assertEquals("follower_cluster_copy", repository.getMetadata().name());
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
            expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository("leader_cluster"));
        });

        ClusterUpdateSettingsRequest deleteFollowerRequest = new ClusterUpdateSettingsRequest();
        deleteFollowerRequest.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(deleteFollowerRequest).actionGet());

        assertBusy(() -> {
            final RepositoriesService repositoriesService =
                getFollowerCluster().getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();
            expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository("follower_cluster_copy"));
        });
    }
}
