/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.CcrConstants;
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

    private MockLicenseState licenseState;
    private ClusterService clusterService;

    @Before
    public void init() {
        licenseState = mock(MockLicenseState.class);
        clusterService = mock(ClusterService.class);
    }

    public void testAvailable() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        CCRInfoTransportAction featureSet = new CCRInfoTransportAction(
            transportService,
            mock(ActionFilters.class),
            Settings.EMPTY,
            licenseState
        );

        when(licenseState.isAllowed(CcrConstants.CCR_FEATURE)).thenReturn(false);
        assertThat(featureSet.available(), equalTo(false));

        when(licenseState.isAllowed(CcrConstants.CCR_FEATURE)).thenReturn(true);
        assertThat(featureSet.available(), equalTo(true));
    }

    public void testEnabled() {
        Settings.Builder settings = Settings.builder().put("xpack.ccr.enabled", false);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        CCRInfoTransportAction featureSet = new CCRInfoTransportAction(
            transportService,
            mock(ActionFilters.class),
            settings.build(),
            licenseState
        );
        assertThat(featureSet.enabled(), equalTo(false));

        settings = Settings.builder().put("xpack.ccr.enabled", true);
        featureSet = new CCRInfoTransportAction(transportService, mock(ActionFilters.class), settings.build(), licenseState);
        assertThat(featureSet.enabled(), equalTo(true));
    }

    public void testName() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        CCRInfoTransportAction featureSet = new CCRInfoTransportAction(
            transportService,
            mock(ActionFilters.class),
            Settings.EMPTY,
            licenseState
        );
        assertThat(featureSet.name(), equalTo("ccr"));
    }

    public void testUsageStats() throws Exception {
        Metadata.Builder metadata = Metadata.builder();

        int numFollowerIndices = randomIntBetween(0, 32);
        for (int i = 0; i < numFollowerIndices; i++) {
            IndexMetadata.Builder followerIndex = IndexMetadata.builder("follow_index" + i)
                .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(i)
                .putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());
            metadata.put(followerIndex);
        }

        // Add a regular index, to check that we do not take that one into account:
        IndexMetadata.Builder regularIndex = IndexMetadata.builder("my_index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(numFollowerIndices);
        metadata.put(regularIndex);

        int numAutoFollowPatterns = randomIntBetween(0, 32);
        Map<String, AutoFollowMetadata.AutoFollowPattern> patterns = Maps.newMapWithExpectedSize(numAutoFollowPatterns);
        for (int i = 0; i < numAutoFollowPatterns; i++) {
            AutoFollowMetadata.AutoFollowPattern pattern = new AutoFollowMetadata.AutoFollowPattern(
                "remote_cluser",
                Collections.singletonList("logs" + i + "*"),
                Collections.emptyList(),
                null,
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );
            patterns.put("pattern" + i, pattern);
        }
        metadata.putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        var usageAction = new CCRUsageTransportAction(
            transportService,
            null,
            threadPool,
            mock(ActionFilters.class),
            null,
            Settings.EMPTY,
            licenseState
        );
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
