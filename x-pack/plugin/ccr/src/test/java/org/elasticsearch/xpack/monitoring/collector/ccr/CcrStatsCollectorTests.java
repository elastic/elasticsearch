/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.client.CcrClient;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CcrStatsCollectorTests extends AbstractCcrCollectorTestCase {

    @Override
    AbstractCcrCollector createCollector(Settings settings, ClusterService clusterService, XPackLicenseState licenseState, Client client) {
        return new CcrStatsCollector(settings, clusterService, licenseState, client);
    }

    public void testDoCollect() throws Exception {
        final String clusterUuid = randomAlphaOfLength(5);
        whenClusterStateWithUUID(clusterUuid);

        final MonitoringDoc.Node node = randomMonitoringNode(random());
        final CcrClient client = mock(CcrClient.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(CcrStatsCollector.CCR_STATS_TIMEOUT, timeout);

        final CcrStatsCollector collector = new CcrStatsCollector(Settings.EMPTY, clusterService, licenseState, client, threadContext);
        assertEquals(timeout, collector.getCollectionTimeout());

        final List<CcrStatsAction.StatsResponse> statuses = mockStatuses();

        @SuppressWarnings("unchecked")
        final ActionFuture<CcrStatsAction.StatsResponses> future = (ActionFuture<CcrStatsAction.StatsResponses>)mock(ActionFuture.class);
        final CcrStatsAction.StatsResponses responses = new CcrStatsAction.StatsResponses(emptyList(), emptyList(), statuses);

        final CcrStatsAction.StatsRequest request = new CcrStatsAction.StatsRequest();
        request.setIndices(Strings.EMPTY_ARRAY);
        when(client.stats(statsRequestEq(request))).thenReturn(future);
        when(future.actionGet(timeout)).thenReturn(responses);

        final long interval = randomNonNegativeLong();

        final Collection<MonitoringDoc> documents = collector.doCollect(node, interval, clusterState);
        verify(clusterState).metaData();
        verify(metaData).clusterUUID();

        assertThat(documents, hasSize(statuses.size()));

        int index = 0;
        for (final Iterator<MonitoringDoc> it = documents.iterator(); it.hasNext(); index++) {
            final CcrStatsMonitoringDoc document = (CcrStatsMonitoringDoc)it.next();
            final CcrStatsAction.StatsResponse status = statuses.get(index);

            assertThat(document.getCluster(), is(clusterUuid));
            assertThat(document.getTimestamp(), greaterThan(0L));
            assertThat(document.getIntervalMillis(), equalTo(interval));
            assertThat(document.getNode(), equalTo(node));
            assertThat(document.getSystem(), is(MonitoredSystem.ES));
            assertThat(document.getType(), is(CcrStatsMonitoringDoc.TYPE));
            assertThat(document.getId(), nullValue());
            assertThat(document.status(), is(status.status()));
        }
    }

    private List<CcrStatsAction.StatsResponse> mockStatuses() {
        final int count = randomIntBetween(1, 8);
        final List<CcrStatsAction.StatsResponse> statuses = new ArrayList<>(count);

        for (int i = 0; i < count; ++i) {
            CcrStatsAction.StatsResponse statsResponse = mock(CcrStatsAction.StatsResponse.class);
            ShardFollowNodeTaskStatus status = mock(ShardFollowNodeTaskStatus.class);
            when(statsResponse.status()).thenReturn(status);
            statuses.add(statsResponse);
        }

        return statuses;
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
