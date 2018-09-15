/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.client.CcrClient;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CcrStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfMonitoringNotAllowed() {
        final Settings settings = ESTestCase.randomFrom(ccrEnabledSettings(), ccrDisabledSettings());
        final boolean ccrAllowed = ESTestCase.randomBoolean();
        final boolean isElectedMaster = ESTestCase.randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        // this controls the blockage
        when(licenseState.isMonitoringAllowed()).thenReturn(false);
        when(licenseState.isCcrAllowed()).thenReturn(ccrAllowed);

        final CcrStatsCollector collector = new CcrStatsCollector(settings, clusterService, licenseState, client);

        Assert.assertThat(collector.shouldCollect(isElectedMaster), is(false));
        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // regardless of CCR being enabled
        final Settings settings = ESTestCase.randomFrom(ccrEnabledSettings(), ccrDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(ESTestCase.randomBoolean());
        when(licenseState.isCcrAllowed()).thenReturn(ESTestCase.randomBoolean());
        // this controls the blockage
        final boolean isElectedMaster = false;

        final CcrStatsCollector collector = new CcrStatsCollector(settings, clusterService, licenseState, client);

        Assert.assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsFalseIfCCRIsDisabled() {
        // this is controls the blockage
        final Settings settings = ccrDisabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(ESTestCase.randomBoolean());
        when(licenseState.isCcrAllowed()).thenReturn(ESTestCase.randomBoolean());

        final boolean isElectedMaster = ESTestCase.randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final CcrStatsCollector collector = new CcrStatsCollector(settings, clusterService, licenseState, client);

        Assert.assertThat(collector.shouldCollect(isElectedMaster), is(false));

        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsFalseIfCCRIsNotAllowed() {
        final Settings settings = ESTestCase.randomFrom(ccrEnabledSettings(), ccrDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(ESTestCase.randomBoolean());
        // this is controls the blockage
        when(licenseState.isCcrAllowed()).thenReturn(false);
        final boolean isElectedMaster = ESTestCase.randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final CcrStatsCollector collector = new CcrStatsCollector(settings, clusterService, licenseState, client);

        Assert.assertThat(collector.shouldCollect(isElectedMaster), is(false));

        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsTrue() {
        final Settings settings = ccrEnabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        when(licenseState.isCcrAllowed()).thenReturn(true);
        final boolean isElectedMaster = true;

        final CcrStatsCollector collector = new CcrStatsCollector(settings, clusterService, licenseState, client);

        Assert.assertThat(collector.shouldCollect(isElectedMaster), is(true));

        verify(licenseState).isMonitoringAllowed();
    }

    public void testDoCollect() throws Exception {
        final String clusterUuid = ESTestCase.randomAlphaOfLength(5);
        whenClusterStateWithUUID(clusterUuid);

        final MonitoringDoc.Node node = MonitoringTestUtils.randomMonitoringNode(LuceneTestCase.random());
        final CcrClient client = mock(CcrClient.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final TimeValue timeout = TimeValue.timeValueSeconds(ESTestCase.randomIntBetween(1, 120));
        withCollectionTimeout(CcrStatsCollector.CCR_STATS_TIMEOUT, timeout);

        final CcrStatsCollector collector = new CcrStatsCollector(Settings.EMPTY, clusterService, licenseState, client, threadContext);
        Assert.assertEquals(timeout, collector.getCollectionTimeout());

        final List<CcrStatsAction.StatsResponse> statuses = mockStatuses();

        @SuppressWarnings("unchecked")
        final ActionFuture<CcrStatsAction.StatsResponses> future = (ActionFuture<CcrStatsAction.StatsResponses>)mock(ActionFuture.class);
        final CcrStatsAction.StatsResponses responses = new CcrStatsAction.StatsResponses(emptyList(), emptyList(), statuses);

        final CcrStatsAction.StatsRequest request = new CcrStatsAction.StatsRequest();
        request.setIndices(Strings.EMPTY_ARRAY);
        when(client.stats(statsRequestEq(request))).thenReturn(future);
        when(future.actionGet(timeout)).thenReturn(responses);

        final long interval = ESTestCase.randomNonNegativeLong();

        final Collection<MonitoringDoc> documents = collector.doCollect(node, interval, clusterState);
        verify(clusterState).metaData();
        verify(metaData).clusterUUID();

        Assert.assertThat(documents, hasSize(statuses.size()));

        int index = 0;
        for (final Iterator<MonitoringDoc> it = documents.iterator(); it.hasNext(); index++) {
            final CcrStatsMonitoringDoc document = (CcrStatsMonitoringDoc)it.next();
            final CcrStatsAction.StatsResponse status = statuses.get(index);

            Assert.assertThat(document.getCluster(), is(clusterUuid));
            Assert.assertThat(document.getTimestamp(), greaterThan(0L));
            Assert.assertThat(document.getIntervalMillis(), equalTo(interval));
            Assert.assertThat(document.getNode(), equalTo(node));
            Assert.assertThat(document.getSystem(), is(MonitoredSystem.ES));
            Assert.assertThat(document.getType(), Matchers.is(CcrStatsMonitoringDoc.TYPE));
            Assert.assertThat(document.getId(), nullValue());
            Assert.assertThat(document.status(), is(status.status()));
        }
    }

    private List<CcrStatsAction.StatsResponse> mockStatuses() {
        final int count = ESTestCase.randomIntBetween(1, 8);
        final List<CcrStatsAction.StatsResponse> statuses = new ArrayList<>(count);

        for (int i = 0; i < count; ++i) {
            CcrStatsAction.StatsResponse statsResponse = mock(CcrStatsAction.StatsResponse.class);
            ShardFollowNodeTaskStatus status = mock(ShardFollowNodeTaskStatus.class);
            when(statsResponse.status()).thenReturn(status);
            statuses.add(statsResponse);
        }

        return statuses;
    }

    private Settings ccrEnabledSettings() {
        // since it's the default, we want to ensure we test both with/without it
        return ESTestCase.randomBoolean() ? Settings.EMPTY : Settings.builder().put(XPackSettings.CCR_ENABLED_SETTING.getKey(), true).build();
    }

    private Settings ccrDisabledSettings() {
        return Settings.builder().put(XPackSettings.CCR_ENABLED_SETTING.getKey(), false).build();
    }

    private static CcrStatsAction.StatsRequest statsRequestEq(CcrStatsAction.StatsRequest expected) {
        return argThat(new StatsRequestMatches(expected));
    }

    private static class StatsRequestMatches extends ArgumentMatcher<CcrStatsAction.StatsRequest> {

        private final CcrStatsAction.StatsRequest expected;

        private StatsRequestMatches(CcrStatsAction.StatsRequest expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object o) {
            CcrStatsAction.StatsRequest actual = (CcrStatsAction.StatsRequest) o;
            return Arrays.equals(expected.indices(), actual.indices());
        }
    }

}
