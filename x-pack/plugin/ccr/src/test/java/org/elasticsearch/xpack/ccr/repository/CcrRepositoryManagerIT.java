/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CcrRepositoryManagerIT extends CcrIntegTestCase {

    public void testThatRepositoryIsPutAndRemovedWhenRemoteClusterIsUpdated() throws Exception {
        assertBusy(() -> {
            GetRepositoriesResponse response = followerClient()
                .admin()
                .cluster()
                .prepareGetRepositories()
                .get();
            assertEquals(1, response.repositories().size());
            assertEquals(CcrRepository.TYPE, response.repositories().get(0).type());
            assertEquals("leader_cluster", response.repositories().get(0).name());
        });

        ClusterUpdateSettingsRequest putFollowerRequest = new ClusterUpdateSettingsRequest();
        String address = getFollowerCluster().getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        putFollowerRequest.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(putFollowerRequest).actionGet());

        assertBusy(() -> {
            GetRepositoriesResponse response = followerClient()
                .admin()
                .cluster()
                .prepareGetRepositories()
                .get();
            assertEquals(2, response.repositories().size());
        });

        ClusterUpdateSettingsRequest deleteLeaderRequest = new ClusterUpdateSettingsRequest();
        deleteLeaderRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(deleteLeaderRequest).actionGet());

        assertBusy(() -> {
            GetRepositoriesResponse response = followerClient()
                .admin()
                .cluster()
                .prepareGetRepositories()
                .get();
            assertEquals(1, response.repositories().size());
            assertEquals(CcrRepository.TYPE, response.repositories().get(0).type());
            assertEquals("follower_cluster_copy", response.repositories().get(0).name());
        });

        ClusterUpdateSettingsRequest deleteFollowerRequest = new ClusterUpdateSettingsRequest();
        deleteFollowerRequest.persistentSettings(Settings.builder().put("cluster.remote.follower_cluster_copy.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(deleteFollowerRequest).actionGet());

        assertBusy(() -> {
            GetRepositoriesResponse response = followerClient()
                .admin()
                .cluster()
                .prepareGetRepositories()
                .get();
            assertEquals(0, response.repositories().size());
        });
    }
}
