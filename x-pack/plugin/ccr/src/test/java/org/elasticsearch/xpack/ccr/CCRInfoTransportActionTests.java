/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CCRInfoTransportActionTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private ClusterService clusterService;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
        clusterService = mock(ClusterService.class);
    }

    public void testAvailable() {
        CCRInfoTransportAction featureSet = new CCRInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);

        when(licenseState.isCcrAllowed()).thenReturn(false);
        assertThat(featureSet.available(), equalTo(false));

        when(licenseState.isCcrAllowed()).thenReturn(true);
        assertThat(featureSet.available(), equalTo(true));
    }

    public void testEnabled() {
        Settings.Builder settings = Settings.builder().put("xpack.ccr.enabled", false);
        CCRInfoTransportAction featureSet = new CCRInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), settings.build(), licenseState);
        assertThat(featureSet.enabled(), equalTo(false));

        settings = Settings.builder().put("xpack.ccr.enabled", true);
        featureSet = new CCRInfoTransportAction(mock(TransportService.class), mock(ActionFilters.class), settings.build(), licenseState);
        assertThat(featureSet.enabled(), equalTo(true));
    }

    public void testName() {
        CCRInfoTransportAction featureSet = new CCRInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);
        assertThat(featureSet.name(), equalTo("ccr"));
    }

    public void testUsageStats() throws Exception {
        MetaData.Builder metaData = MetaData.builder();

        int numFollowerIndices = randomIntBetween(0, 32);
        for (int i = 0; i < numFollowerIndices; i++) {
            IndexMetaData.Builder followerIndex = IndexMetaData.builder("follow_index" + i)
                .settings(settings(Version.CURRENT).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(i)
                .putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());
            metaData.put(followerIndex);
        }

        // Add a regular index, to check that we do not take that one into account:
        IndexMetaData.Builder regularIndex = IndexMetaData.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(numFollowerIndices);
        metaData.put(regularIndex);

        int numAutoFollowPatterns = randomIntBetween(0, 32);
        Map<String, AutoFollowMetadata.AutoFollowPattern> patterns = new HashMap<>(numAutoFollowPatterns);
        for (int i = 0; i < numAutoFollowPatterns; i++) {
            AutoFollowMetadata.AutoFollowPattern pattern = new AutoFollowMetadata.AutoFollowPattern("remote_cluser",
                Collections.singletonList("logs" + i + "*"), null, true, null, null, null, null, null, null, null, null, null, null);
            patterns.put("pattern" + i, pattern);
        }
        metaData.putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metaData(metaData).build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        var usageAction = new CCRUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null, Settings.EMPTY, licenseState);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, clusterState, future);
        CCRInfoTransportAction.Usage ccrUsage = (CCRInfoTransportAction.Usage) future.get().getUsage();
        assertThat(ccrUsage.enabled(), equalTo(true));
        assertThat(ccrUsage.available(), equalTo(false));

        assertThat(ccrUsage.getNumberOfFollowerIndices(), equalTo(numFollowerIndices));
        if (numFollowerIndices != 0) {
            assertThat(ccrUsage.getLastFollowTimeInMillis(), greaterThanOrEqualTo(0L));
        } else {
            assertThat(ccrUsage.getLastFollowTimeInMillis(), nullValue());
        }
        assertThat(ccrUsage.getNumberOfAutoFollowPatterns(), equalTo(numAutoFollowPatterns));
    }

}
