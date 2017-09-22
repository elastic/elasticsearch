/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsIndices;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodes;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.action.XPackUsageAction;
import org.elasticsearch.xpack.action.XPackUsageRequest;
import org.elasticsearch.xpack.action.XPackUsageResponse;
import org.elasticsearch.xpack.logstash.Logstash;
import org.elasticsearch.xpack.logstash.LogstashFeatureSet;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.elasticsearch.xpack.monitoring.collector.BaseCollectorTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.Collection;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterStatsCollectorTests extends BaseCollectorTestCase {

    private LicenseService licenseService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        licenseService = mock(LicenseService.class);
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // this controls the blockage
        whenLocalNodeElectedMaster(false);

        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client, licenseService);

        assertThat(collector.shouldCollect(), is(false));
        verify(nodes).isLocalNodeElectedMaster();
    }

    public void testShouldCollectReturnsTrue() {
        whenLocalNodeElectedMaster(true);

        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client, licenseService);

        assertThat(collector.shouldCollect(), is(true));
        verify(nodes).isLocalNodeElectedMaster();
    }

    public void testDoCollect() throws Exception {
        whenLocalNodeElectedMaster(true);

        final String clusterName = randomAlphaOfLength(10);
        whenClusterStateWithName(clusterName);

        final String clusterUUID = UUID.randomUUID().toString();
        whenClusterStateWithUUID(clusterUUID);

        final MonitoringDoc.Node node = randomMonitoringNode(random());

        final License license = License.builder()
                                        .uid(UUID.randomUUID().toString())
                                        .type("trial")
                                        .issuer("elasticsearch")
                                        .issuedTo("elastic")
                                        .issueDate(System.currentTimeMillis())
                                        .expiryDate(System.currentTimeMillis() + TimeValue.timeValueHours(24L).getMillis())
                                        .maxNodes(2)
                                        .build();
        when(licenseService.getLicense()).thenReturn(license);

        final TimeValue timeout = mock(TimeValue.class);
        when(monitoringSettings.clusterStatsTimeout()).thenReturn(timeout);

        final ClusterStatsResponse mockClusterStatsResponse = mock(ClusterStatsResponse.class);

        final ClusterHealthStatus clusterStatus = randomFrom(ClusterHealthStatus.values());
        when(mockClusterStatsResponse.getStatus()).thenReturn(clusterStatus);
        when(mockClusterStatsResponse.getNodesStats()).thenReturn(mock(ClusterStatsNodes.class));

        final ClusterStatsIndices mockClusterStatsIndices = mock(ClusterStatsIndices.class);

        final int nbIndices = randomIntBetween(0, 100);
        when(mockClusterStatsIndices.getIndexCount()).thenReturn(nbIndices);
        when(mockClusterStatsResponse.getIndicesStats()).thenReturn(mockClusterStatsIndices);

        final ClusterStatsRequestBuilder clusterStatsRequestBuilder = mock(ClusterStatsRequestBuilder.class);
        when(clusterStatsRequestBuilder.get(eq(timeout))).thenReturn(mockClusterStatsResponse);

        final ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(clusterAdminClient.prepareClusterStats()).thenReturn(clusterStatsRequestBuilder);

        final AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        final Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final XPackUsageResponse xPackUsageResponse = new XPackUsageResponse(singletonList(new LogstashFeatureSet.Usage(true, true)));

        @SuppressWarnings("unchecked")
        final ActionFuture<XPackUsageResponse> xPackUsageFuture = (ActionFuture<XPackUsageResponse>) mock(ActionFuture.class);
        when(client.execute(same(XPackUsageAction.INSTANCE), any(XPackUsageRequest.class))).thenReturn(xPackUsageFuture);
        when(xPackUsageFuture.actionGet()).thenReturn(xPackUsageResponse);

        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client, licenseService);

        final Collection<MonitoringDoc> results = collector.doCollect(node);
        assertEquals(1, results.size());

        final MonitoringDoc monitoringDoc = results.iterator().next();
        assertThat(monitoringDoc, instanceOf(ClusterStatsMonitoringDoc.class));

        final ClusterStatsMonitoringDoc document = (ClusterStatsMonitoringDoc) monitoringDoc;
        assertThat(document.getCluster(), equalTo(clusterUUID));
        assertThat(document.getTimestamp(), greaterThan(0L));
        assertThat(document.getNode(), equalTo(node));
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), equalTo(ClusterStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getClusterName(), equalTo(clusterName));
        assertThat(document.getVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(document.getLicense(), equalTo(license));
        assertThat(document.getStatus(), equalTo(clusterStatus));

        assertThat(document.getClusterStats(), notNullValue());
        assertThat(document.getClusterStats().getStatus(), equalTo(clusterStatus));
        assertThat(document.getClusterStats().getIndicesStats().getIndexCount(), equalTo(nbIndices));

        assertThat(document.getUsages(), hasSize(1));
        assertThat(document.getUsages().iterator().next().name(), equalTo(Logstash.NAME));

        assertThat(document.getClusterState().getClusterName().value(), equalTo(clusterName));
        assertThat(document.getClusterState().stateUUID(), equalTo(clusterState.stateUUID()));

        verify(clusterService, times(1)).getClusterName();
        verify(clusterService, times(2)).state();
        verify(licenseService, times(1)).getLicense();
        verify(clusterAdminClient).prepareClusterStats();
        verify(client).execute(same(XPackUsageAction.INSTANCE), any(XPackUsageRequest.class));
    }
}
