/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.enrich;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EnrichStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // this controls the blockage
        final boolean isElectedMaster = false;

        final EnrichStatsCollector collector = createCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsTrue() {
        final boolean isElectedMaster = true;

        final EnrichStatsCollector collector = createCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(true));
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
        List<EnrichStatsAction.Response.CacheStats> cacheStats = new ArrayList<>(numCoordinatorStats);
        for (int i = 0; i < numCoordinatorStats; i++) {
            String nodeId = randomAlphaOfLength(4);
            coordinatorStats.add(
                new CoordinatorStats(
                    nodeId,
                    randomIntBetween(0, Integer.MAX_VALUE),
                    randomIntBetween(0, Integer.MAX_VALUE),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );
            cacheStats.add(
                new EnrichStatsAction.Response.CacheStats(
                    nodeId,
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );
        }

        @SuppressWarnings("unchecked")
        final ActionFuture<EnrichStatsAction.Response> future = (ActionFuture<EnrichStatsAction.Response>) mock(ActionFuture.class);
        final EnrichStatsAction.Response response = new EnrichStatsAction.Response(executingPolicies, coordinatorStats, cacheStats);

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

        assertWarnings(
            "[xpack.monitoring.collection.enrich.stats.timeout] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release."
        );
    }

    private EnrichStatsCollector createCollector(ClusterService clusterService, XPackLicenseState licenseState, Client client) {
        return new EnrichStatsCollector(clusterService, licenseState, client);
    }

}
