/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.AutoFollowStatsAction;
import org.elasticsearch.xpack.core.ccr.client.CcrClient;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.util.Collection;

import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CcrAutoFollowStatsCollectorTests extends AbstractCcrCollectorTestCase {

    @Override
    AbstractCcrCollector createCollector(Settings settings, ClusterService clusterService, XPackLicenseState licenseState, Client client) {
        return new CcrAutoFollowStatsCollector(settings, clusterService, licenseState, client);
    }

    public void testDoCollect() throws Exception {
        final String clusterUuid = randomAlphaOfLength(5);
        whenClusterStateWithUUID(clusterUuid);

        final MonitoringDoc.Node node = randomMonitoringNode(random());
        final CcrClient client = mock(CcrClient.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(CcrAutoFollowStatsCollector.CCR_AUTO_FOLLOW_STATS_TIMEOUT, timeout);

        final CcrAutoFollowStatsCollector collector =
            new CcrAutoFollowStatsCollector(Settings.EMPTY, clusterService, licenseState, client, threadContext);
        assertEquals(timeout, collector.getCollectionTimeout());

        final AutoFollowStats autoFollowStats = mock(AutoFollowStats.class);

        @SuppressWarnings("unchecked")
        final ActionFuture<AutoFollowStatsAction.Response> future = (ActionFuture<AutoFollowStatsAction.Response>)mock(ActionFuture.class);
        final AutoFollowStatsAction.Response response = new AutoFollowStatsAction.Response(autoFollowStats);

        when(client.autoFollowStats(any())).thenReturn(future);
        when(future.actionGet(timeout)).thenReturn(response);

        final long interval = randomNonNegativeLong();

        final Collection<MonitoringDoc> documents = collector.doCollect(node, interval, clusterState);
        verify(clusterState).metaData();
        verify(metaData).clusterUUID();

        assertThat(documents, hasSize(1));
        final AutoFollowStatsMonitoringDoc document = (AutoFollowStatsMonitoringDoc) documents.iterator().next();

        assertThat(document.getCluster(), is(clusterUuid));
        assertThat(document.getTimestamp(), greaterThan(0L));
        assertThat(document.getIntervalMillis(), equalTo(interval));
        assertThat(document.getNode(), equalTo(node));
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(AutoFollowStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());
        assertThat(document.stats(), is(autoFollowStats));
    }

}
