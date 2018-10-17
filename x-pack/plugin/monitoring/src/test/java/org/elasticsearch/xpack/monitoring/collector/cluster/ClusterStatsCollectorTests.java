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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.junit.Assert;

import java.util.Collection;
import java.util.Locale;
import java.util.UUID;

import static java.util.Collections.singletonList;
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
        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, licenseState, client, licenseService);

        assertThat(collector.shouldCollect(false), is(false));
    }

    public void testShouldCollectReturnsTrue() {
        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, licenseState, client, licenseService);

        assertThat(collector.shouldCollect(true), is(true));
    }

    public void testDoAPMIndicesExistReturnsBasedOnIndices() {
        final boolean apmIndicesExist = randomBoolean();
        final Index[] indices = new Index[apmIndicesExist ? randomIntBetween(1, 3) : 0];
        final IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(clusterState, IndicesOptions.lenientExpandOpen(), "apm-*")).thenReturn(indices);

        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, licenseState, client, licenseService, resolver);

        assertThat(collector.doAPMIndicesExist(clusterState), is(apmIndicesExist));
    }

    public void testDoAPMIndicesExistReturnsFalseForExpectedExceptions() {
        final Exception exception = randomFrom(new IndexNotFoundException("TEST - expected"), new IllegalArgumentException());
        final IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(clusterState, IndicesOptions.lenientExpandOpen(), "apm-*")).thenThrow(exception);

        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, licenseState, client, licenseService, resolver);

        assertThat(collector.doAPMIndicesExist(clusterState), is(false));
    }

    public void testDoAPMIndicesExistRethrowsUnexpectedExceptions() {
        final RuntimeException exception = new RuntimeException();
        final IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(clusterState, IndicesOptions.lenientExpandOpen(), "apm-*")).thenThrow(exception);

        final ClusterStatsCollector collector =
                new ClusterStatsCollector(Settings.EMPTY, clusterService, licenseState, client, licenseService, resolver);

        expectThrows(RuntimeException.class, () -> collector.doAPMIndicesExist(clusterState));
    }

    public void testDoCollect() throws Exception {
        final Settings.Builder settings = Settings.builder();
        final License.OperationMode mode =
                randomValueOtherThan(License.OperationMode.MISSING, () -> randomFrom(License.OperationMode.values()));
        final boolean securityEnabled = randomBoolean();
        final boolean transportTLSEnabled;

        if (securityEnabled) {
            switch (mode) {
                case TRIAL:
                    transportTLSEnabled = randomBoolean();
                    break;
                case BASIC:
                    transportTLSEnabled = false;
                    break;
                case STANDARD:
                case GOLD:
                case PLATINUM:
                    transportTLSEnabled = true;
                    break;
                default:
                    throw new AssertionError("Unknown mode [" + mode + "]");
            }

            if (randomBoolean()) {
                settings.put(XPackSettings.SECURITY_ENABLED.getKey(), true);
            }
            settings.put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), transportTLSEnabled);
        } else {
            transportTLSEnabled = false;

            settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        }

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(ClusterStatsCollector.CLUSTER_STATS_TIMEOUT, timeout);

        whenLocalNodeElectedMaster(true);

        final String clusterName = randomAlphaOfLength(10);
        whenClusterStateWithName(clusterName);

        final String clusterUUID = UUID.randomUUID().toString();
        whenClusterStateWithUUID(clusterUUID);

        final MonitoringDoc.Node node = MonitoringTestUtils.randomMonitoringNode(random());

        final License license = License.builder()
                                        .uid(UUID.randomUUID().toString())
                                        .type(mode.name().toLowerCase(Locale.ROOT))
                                        .issuer("elasticsearch")
                                        .issuedTo("elastic")
                                        .issueDate(System.currentTimeMillis())
                                        .expiryDate(System.currentTimeMillis() + TimeValue.timeValueHours(24L).getMillis())
                                        .maxNodes(2)
                                        .build();
        when(licenseService.getLicense()).thenReturn(license);

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

        final IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        final boolean apmIndicesExist = randomBoolean();
        final Index[] indices = new Index[apmIndicesExist ? randomIntBetween(1, 5) : 0];
        when(indexNameExpressionResolver.concreteIndices(clusterState, IndicesOptions.lenientExpandOpen(), "apm-*"))
            .thenReturn(indices);

        final XPackUsageResponse xPackUsageResponse = new XPackUsageResponse(
            singletonList(new MonitoringFeatureSetUsage(true, true, false, null)));

        @SuppressWarnings("unchecked")
        final ActionFuture<XPackUsageResponse> xPackUsageFuture = (ActionFuture<XPackUsageResponse>) mock(ActionFuture.class);
        when(client.execute(same(XPackUsageAction.INSTANCE), any(XPackUsageRequest.class))).thenReturn(xPackUsageFuture);
        when(xPackUsageFuture.actionGet()).thenReturn(xPackUsageResponse);

        final ClusterStatsCollector collector =
                new ClusterStatsCollector(settings.build(), clusterService, licenseState,
                                          client, licenseService, indexNameExpressionResolver);

        Assert.assertEquals(timeout, collector.getCollectionTimeout());

        final long interval = randomNonNegativeLong();

        final Collection<MonitoringDoc> results = collector.doCollect(node, interval, clusterState);
        assertEquals(1, results.size());

        final MonitoringDoc monitoringDoc = results.iterator().next();
        assertThat(monitoringDoc, instanceOf(ClusterStatsMonitoringDoc.class));

        final ClusterStatsMonitoringDoc document = (ClusterStatsMonitoringDoc) monitoringDoc;
        assertThat(document.getCluster(), equalTo(clusterUUID));
        assertThat(document.getTimestamp(), greaterThan(0L));
        assertThat(document.getIntervalMillis(), equalTo(interval));
        assertThat(document.getNode(), equalTo(node));
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), equalTo(ClusterStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getClusterName(), equalTo(clusterName));
        assertThat(document.getVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(document.getLicense(), equalTo(license));
        assertThat(document.getStatus(), equalTo(clusterStatus));

        final boolean securitySettingDefined = settings.build().hasValue(XPackSettings.SECURITY_ENABLED.getKey());
        assertThat(document.getClusterNeedsTLSEnabled(),
                   equalTo(mode == License.OperationMode.TRIAL && securitySettingDefined && securityEnabled
                           && transportTLSEnabled == false));

        assertThat(document.getClusterStats(), notNullValue());
        assertThat(document.getClusterStats().getStatus(), equalTo(clusterStatus));
        assertThat(document.getClusterStats().getIndicesStats().getIndexCount(), equalTo(nbIndices));

        assertThat(document.getAPMIndicesExist(), is(apmIndicesExist));
        assertThat(document.getUsages(), hasSize(1));
        assertThat(document.getUsages().iterator().next().name(), equalTo(XPackField.MONITORING));

        assertThat(document.getClusterState().getClusterName().value(), equalTo(clusterName));
        assertThat(document.getClusterState().stateUUID(), equalTo(clusterState.stateUUID()));

        verify(clusterService, times(1)).getClusterName();
        verify(clusterState, times(1)).metaData();
        verify(metaData, times(1)).clusterUUID();
        verify(licenseService, times(1)).getLicense();
        verify(clusterAdminClient).prepareClusterStats();
        verify(client).execute(same(XPackUsageAction.INSTANCE), any(XPackUsageRequest.class));
    }
}
