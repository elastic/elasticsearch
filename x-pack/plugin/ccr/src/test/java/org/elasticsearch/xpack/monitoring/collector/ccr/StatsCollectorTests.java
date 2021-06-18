/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // regardless of CCR being enabled
        final Settings settings = randomFrom(ccrEnabledSettings(), ccrDisabledSettings());

        when(licenseState.checkFeature(XPackLicenseState.Feature.CCR)).thenReturn(randomBoolean());
        // this controls the blockage
        final boolean isElectedMaster = false;

        final StatsCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsFalseIfCCRIsDisabled() {
        // this is controls the blockage
        final Settings settings = ccrDisabledSettings();

        when(licenseState.checkFeature(XPackLicenseState.Feature.CCR)).thenReturn(randomBoolean());

        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final StatsCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsFalseIfCCRIsNotAllowed() {
        final Settings settings = randomFrom(ccrEnabledSettings(), ccrDisabledSettings());

        // this is controls the blockage
        when(licenseState.checkFeature(XPackLicenseState.Feature.CCR)).thenReturn(false);
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final StatsCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsTrue() {
        final Settings settings = ccrEnabledSettings();

        when(licenseState.checkFeature(XPackLicenseState.Feature.CCR)).thenReturn(true);
        final boolean isElectedMaster = true;

        final StatsCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(true));
    }

    public void testDoCollect() throws Exception {
        final String clusterUuid = randomAlphaOfLength(5);
        whenClusterStateWithUUID(clusterUuid);

        final MonitoringDoc.Node node = randomMonitoringNode(random());
        final Client client = mock(Client.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final List<FollowStatsAction.StatsResponse> statuses = mockStatuses();

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(StatsCollector.CCR_STATS_TIMEOUT, timeout);

        final AutoFollowStats autoFollowStats = mock(AutoFollowStats.class);
        final FollowStatsAction.StatsResponses statsResponse = mock(FollowStatsAction.StatsResponses.class);
        when(statsResponse.getStatsResponses()).thenReturn(statuses);

        @SuppressWarnings("unchecked")
        final ActionFuture<CcrStatsAction.Response> future = (ActionFuture<CcrStatsAction.Response>) mock(ActionFuture.class);
        final CcrStatsAction.Response response = new CcrStatsAction.Response(autoFollowStats, statsResponse);

        when(client.execute(eq(CcrStatsAction.INSTANCE), any(CcrStatsAction.Request.class))).thenReturn(future);
        when(future.actionGet(timeout)).thenReturn(response);

        final StatsCollector collector = new StatsCollector(settings, clusterService, licenseState, client, threadContext);
        assertEquals(timeout, collector.getCollectionTimeout());

        final long interval = randomNonNegativeLong();
        final List<MonitoringDoc> documents = new ArrayList<>(collector.doCollect(node, interval, clusterState));
        verify(clusterState).metadata();
        verify(metadata).clusterUUID();

        assertThat(documents, hasSize(statuses.size() + 1));

        for (int i = 0; i < documents.size() - 1; i++) {
            final FollowStatsMonitoringDoc document = (FollowStatsMonitoringDoc) documents.get(i);
            final FollowStatsAction.StatsResponse status = statuses.get(i);

            assertThat(document.getCluster(), is(clusterUuid));
            assertThat(document.getTimestamp(), greaterThan(0L));
            assertThat(document.getIntervalMillis(), equalTo(interval));
            assertThat(document.getNode(), equalTo(node));
            assertThat(document.getSystem(), is(MonitoredSystem.ES));
            assertThat(document.getType(), is(FollowStatsMonitoringDoc.TYPE));
            assertThat(document.getId(), nullValue());
            assertThat(document.status(), is(status.status()));
        }

        final AutoFollowStatsMonitoringDoc document = (AutoFollowStatsMonitoringDoc) documents.get(documents.size() - 1);
        assertThat(document, notNullValue());
        assertThat(document.getCluster(), is(clusterUuid));
        assertThat(document.getTimestamp(), greaterThan(0L));
        assertThat(document.getIntervalMillis(), equalTo(interval));
        assertThat(document.getNode(), equalTo(node));
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(AutoFollowStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());
        assertThat(document.stats(), is(autoFollowStats));
    }

    private List<FollowStatsAction.StatsResponse> mockStatuses() {
        final int count = randomIntBetween(1, 8);
        final List<FollowStatsAction.StatsResponse> statuses = new ArrayList<>(count);

        for (int i = 0; i < count; ++i) {
            FollowStatsAction.StatsResponse statsResponse = mock(FollowStatsAction.StatsResponse.class);
            ShardFollowNodeTaskStatus status = mock(ShardFollowNodeTaskStatus.class);
            when(status.followerIndex()).thenReturn("follow_index");
            when(statsResponse.status()).thenReturn(status);
            statuses.add(statsResponse);
        }

        return statuses;
    }

    private StatsCollector createCollector(Settings settings,
                                           ClusterService clusterService,
                                           XPackLicenseState licenseState,
                                           Client client) {
        return new StatsCollector(settings, clusterService, licenseState, client);
    }

    private Settings ccrEnabledSettings() {
        // since it's the default, we want to ensure we test both with/without it
        return randomBoolean() ? Settings.EMPTY : Settings.builder().put(XPackSettings.CCR_ENABLED_SETTING.getKey(), true).build();
    }

    private Settings ccrDisabledSettings() {
        return Settings.builder().put(XPackSettings.CCR_ENABLED_SETTING.getKey(), false).build();
    }

}
