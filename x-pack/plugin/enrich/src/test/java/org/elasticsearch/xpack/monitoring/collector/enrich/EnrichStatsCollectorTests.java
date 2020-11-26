/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.enrich;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutingPolicy;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.enrich.action.EnrichStatsResponseTests.randomTaskInfo;
import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EnrichStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfMonitoringNotAllowed() {
        final boolean enrichAllowed = randomBoolean();
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        // this controls the blockage
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING)).thenReturn(false);
        when(licenseState.checkFeature(XPackLicenseState.Feature.ENRICH)).thenReturn(enrichAllowed);

        final EnrichStatsCollector collector = createCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
        if (isElectedMaster) {
            verify(licenseState).checkFeature(XPackLicenseState.Feature.MONITORING);
        }
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING)).thenReturn(randomBoolean());
        when(licenseState.checkFeature(XPackLicenseState.Feature.ENRICH)).thenReturn(randomBoolean());
        // this controls the blockage
        final boolean isElectedMaster = false;

        final EnrichStatsCollector collector = createCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsFalseIfEnrichIsNotAllowed() {
        boolean isMonitoringAllowed = randomBoolean();
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING)).thenReturn(isMonitoringAllowed);
        // this is controls the blockage
        when(licenseState.checkFeature(XPackLicenseState.Feature.ENRICH)).thenReturn(false);
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final EnrichStatsCollector collector = createCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));

        if (isElectedMaster) {
            verify(licenseState).checkFeature(XPackLicenseState.Feature.MONITORING);
        }
    }

    public void testShouldCollectReturnsTrue() {
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING)).thenReturn(true);
        when(licenseState.checkFeature(XPackLicenseState.Feature.ENRICH)).thenReturn(true);
        final boolean isElectedMaster = true;

        final EnrichStatsCollector collector = createCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(true));

        verify(licenseState).checkFeature(XPackLicenseState.Feature.MONITORING);
    }

    public void testDoCollect() throws Exception {
        final String clusterUuid = randomAlphaOfLength(5);
        whenClusterStateWithUUID(clusterUuid);

        final MonitoringDoc.Node node = randomMonitoringNode(random());
        final Client client = mock(Client.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(EnrichStatsCollector.STATS_TIMEOUT, timeout);

        int numExecutingPolicies = randomIntBetween(0, 8);
        List<ExecutingPolicy> executingPolicies = new ArrayList<>(numExecutingPolicies);
        for (int i = 0; i < numExecutingPolicies; i++) {
            executingPolicies.add(new ExecutingPolicy(randomAlphaOfLength(4), randomTaskInfo()));
        }
        int numCoordinatorStats = randomIntBetween(0, 8);
        List<CoordinatorStats> coordinatorStats = new ArrayList<>(numCoordinatorStats);
        for (int i = 0; i < numCoordinatorStats; i++) {
            coordinatorStats.add(
                new CoordinatorStats(
                    randomAlphaOfLength(4),
                    randomIntBetween(0, Integer.MAX_VALUE),
                    randomIntBetween(0, Integer.MAX_VALUE),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );
        }

        @SuppressWarnings("unchecked")
        final ActionFuture<EnrichStatsAction.Response> future = (ActionFuture<EnrichStatsAction.Response>) mock(ActionFuture.class);
        final EnrichStatsAction.Response response = new EnrichStatsAction.Response(executingPolicies, coordinatorStats);

        when(client.execute(eq(EnrichStatsAction.INSTANCE), any(EnrichStatsAction.Request.class))).thenReturn(future);
        when(future.actionGet(timeout)).thenReturn(response);

        final EnrichStatsCollector collector = new EnrichStatsCollector(clusterService, licenseState, client, threadContext);
        assertEquals(timeout, collector.getCollectionTimeout());

        final long interval = randomNonNegativeLong();
        final List<MonitoringDoc> documents = new ArrayList<>(collector.doCollect(node, interval, clusterState));
        verify(clusterState).metadata();
        verify(metadata).clusterUUID();

        assertThat(documents, hasSize(executingPolicies.size() + coordinatorStats.size()));

        for (int i = 0; i < coordinatorStats.size(); i++) {
            final EnrichCoordinatorDoc actual = (EnrichCoordinatorDoc) documents.get(i);
            final CoordinatorStats expected = coordinatorStats.get(i);

            assertThat(actual.getCluster(), is(clusterUuid));
            assertThat(actual.getTimestamp(), greaterThan(0L));
            assertThat(actual.getIntervalMillis(), equalTo(interval));
            assertThat(actual.getNode(), equalTo(node));
            assertThat(actual.getSystem(), is(MonitoredSystem.ES));
            assertThat(actual.getType(), is(EnrichCoordinatorDoc.TYPE));
            assertThat(actual.getId(), nullValue());
            assertThat(actual.getCoordinatorStats(), equalTo(expected));
        }

        for (int i = coordinatorStats.size(); i < documents.size(); i++) {
            final ExecutingPolicyDoc actual = (ExecutingPolicyDoc) documents.get(i);
            final ExecutingPolicy expected = executingPolicies.get(i - coordinatorStats.size());

            assertThat(actual.getCluster(), is(clusterUuid));
            assertThat(actual.getTimestamp(), greaterThan(0L));
            assertThat(actual.getIntervalMillis(), equalTo(interval));
            assertThat(actual.getNode(), equalTo(node));
            assertThat(actual.getSystem(), is(MonitoredSystem.ES));
            assertThat(actual.getType(), is(ExecutingPolicyDoc.TYPE));
            assertThat(actual.getId(), nullValue());
            assertThat(actual.getExecutingPolicy(), equalTo(expected));
        }
    }

    private EnrichStatsCollector createCollector(ClusterService clusterService, XPackLicenseState licenseState, Client client) {
        return new EnrichStatsCollector(clusterService, licenseState, client);
    }

}
