/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.LocalStateCcr;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public abstract class CCRSingleNodeTestCase extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        builder.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        builder.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
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

    @After
    public void remoteLocalRemote() {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(Settings.builder().put("cluster.remote.local.seeds", (String) null));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    protected ResumeFollowAction.Request getFollowRequest() {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setLeaderCluster("local");
        request.setLeaderIndex("leader");
        request.setFollowerIndex("follower");
        request.setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.setPollTimeout(TimeValue.timeValueMillis(10));
        return request;
    }

}
