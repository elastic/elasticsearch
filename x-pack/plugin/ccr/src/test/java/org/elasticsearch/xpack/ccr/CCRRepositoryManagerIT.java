/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.repository.CCRRepository;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CCRRepositoryManagerIT extends CcrIntegTestCase {

    public void testThatRepositoryIsPutAndRemovedWhenRemoteClusterIsUpdated() throws Exception {
        assertBusy(() -> {
            GetRepositoriesResponse response = followerClient()
                .admin()
                .cluster()
                .prepareGetRepositories()
                .get();
            assertEquals(1, response.repositories().size());
            assertEquals(CCRRepository.TYPE, response.repositories().get(0).type());
            assertEquals("leader_cluster", response.repositories().get(0).name());
        });

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", ""));
        assertAcked(followerClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

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
