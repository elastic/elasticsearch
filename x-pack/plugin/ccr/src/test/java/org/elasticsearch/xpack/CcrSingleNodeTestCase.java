/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensesMetaData;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.LocalStateCcr;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.CcrIntegTestCase.removeCCRRelatedMetadataFromClusterState;
import static org.hamcrest.Matchers.equalTo;

public abstract class CcrSingleNodeTestCase extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        builder.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        builder.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        // Let cluster state api return quickly in order to speed up auto follow tests:
        builder.put(CcrSettings.CCR_AUTO_FOLLOW_WAIT_FOR_METADATA_TIMEOUT.getKey(), TimeValue.timeValueMillis(100));
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(LocalStateCcr.class);
    }

    @Before
    public void setupLocalRemote() {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        String address = getInstanceFromNode(TransportService.class).boundAddress().publishAddress().toString();
        updateSettingsRequest.transientSettings(Settings.builder().put("cluster.remote.local.seeds", address));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    @Before
    public void waitForTrialLicenseToBeGenerated() throws Exception {
        assertBusy(() -> assertNotNull(getInstanceFromNode(ClusterService.class).state().metaData().custom(LicensesMetaData.TYPE)));
    }

    @After
    public void purgeCCRMetadata() throws Exception {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        removeCCRRelatedMetadataFromClusterState(clusterService);
    }

    @After
    public void removeLocalRemote() {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(Settings.builder().put("cluster.remote.local.seeds", (String) null));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    protected ResumeFollowAction.Request getResumeFollowRequest(String followerIndex) {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setFollowerIndex(followerIndex);
        request.setMaxRetryDelay(TimeValue.timeValueMillis(1));
        request.setReadPollTimeout(TimeValue.timeValueMillis(1));
        return request;
    }

    protected PutFollowAction.Request getPutFollowRequest(String leaderIndex, String followerIndex) {
        PutFollowAction.Request request = new PutFollowAction.Request();
        request.setRemoteCluster("local");
        request.setLeaderIndex(leaderIndex);
        request.setFollowRequest(getResumeFollowRequest(followerIndex));
        return request;
    }

    protected void ensureEmptyWriteBuffers() throws Exception {
        assertBusy(() -> {
            FollowStatsAction.StatsResponses statsResponses =
                client().execute(FollowStatsAction.INSTANCE, new FollowStatsAction.StatsRequest()).actionGet();
            for (FollowStatsAction.StatsResponse statsResponse : statsResponses.getStatsResponses()) {
                ShardFollowNodeTaskStatus status = statsResponse.status();
                assertThat(status.writeBufferOperationCount(), equalTo(0));
                assertThat(status.writeBufferSizeInBytes(), equalTo(0L));
            }
        });
    }

}
