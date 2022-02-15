/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateApiTests extends ESSingleNodeTestCase {

    public void testWaitForMetadataVersion() throws Exception {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForTimeout(TimeValue.timeValueHours(1));
        ClusterStateResponse response = client().admin().cluster().state(clusterStateRequest).get(10L, TimeUnit.SECONDS);
        assertThat(response.isWaitForTimedOut(), is(false));
        long metadataVersion = response.getState().getMetadata().version();

        // Verify that cluster state api returns after the cluster settings have been updated:
        clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);

        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(clusterStateRequest);
        assertThat(future2.isDone(), is(false));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        // Pick an arbitrary dynamic cluster setting and change it. Just to get metadata version incremented:
        updateSettingsRequest.transientSettings(Settings.builder().put("cluster.max_shards_per_node", 999));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        response = future2.get(10L, TimeUnit.SECONDS);
        assertThat(response.isWaitForTimedOut(), is(false));
        assertThat(response.getState().metadata().version(), equalTo(metadataVersion + 1));

        // Verify that the timed out property has been set"
        metadataVersion = response.getState().getMetadata().version();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);
        clusterStateRequest.waitForTimeout(TimeValue.timeValueMillis(500)); // Fail fast
        ActionFuture<ClusterStateResponse> future3 = client().admin().cluster().state(clusterStateRequest);
        response = future3.get(10L, TimeUnit.SECONDS);
        assertThat(response.isWaitForTimedOut(), is(true));
        assertThat(response.getState(), nullValue());

        // Remove transient setting, otherwise test fails with the reason that this test leaves state behind:
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(Settings.builder().put("cluster.max_shards_per_node", (String) null));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

}
