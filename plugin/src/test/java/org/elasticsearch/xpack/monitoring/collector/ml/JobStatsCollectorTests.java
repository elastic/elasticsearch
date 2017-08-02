/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Request;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Response;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.client.MachineLearningClient;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link JobStatsCollector}.
 */
public class JobStatsCollectorTests extends ESTestCase {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final ClusterState clusterState = mock(ClusterState.class);
    private final DiscoveryNodes nodes = mock(DiscoveryNodes.class);
    private final MonitoringSettings monitoringSettings = mock(MonitoringSettings.class);
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private final InternalClient client = mock(InternalClient.class);

    public void testShouldCollectReturnsFalseIfMonitoringNotAllowed() {
        final Settings settings = randomFrom(mlEnabledSettings(), mlDisabledSettings());
        final boolean mlAllowed = randomBoolean();

        // this controls the blockage
        when(licenseState.isMonitoringAllowed()).thenReturn(false);
        when(licenseState.isMachineLearningAllowed()).thenReturn(mlAllowed);

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(false));

        verify(licenseState).isMonitoringAllowed();
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // regardless of ML being enabled
        final Settings settings = randomFrom(mlEnabledSettings(), mlDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        when(licenseState.isMachineLearningAllowed()).thenReturn(randomBoolean());
        // this controls the blockage
        whenLocalNodeElectedMaster(false);

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(false));

        verify(licenseState).isMonitoringAllowed();
    }

    public void testShouldCollectReturnsFalseIfMLIsDisabled() {
        // this is controls the blockage
        final Settings settings = mlDisabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        when(licenseState.isMachineLearningAllowed()).thenReturn(randomBoolean());
        whenLocalNodeElectedMaster(randomBoolean());

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(false));

        verify(licenseState).isMonitoringAllowed();
    }

    public void testShouldCollectReturnsFalseIfMLIsNotAllowed() {
        final Settings settings = randomFrom(mlEnabledSettings(), mlDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        // this is controls the blockage
        when(licenseState.isMachineLearningAllowed()).thenReturn(false);
        whenLocalNodeElectedMaster(randomBoolean());

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(false));

        verify(licenseState).isMonitoringAllowed();
    }

    public void testShouldCollectReturnsTrue() {
        final Settings settings = mlEnabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        when(licenseState.isMachineLearningAllowed()).thenReturn(true);
        whenLocalNodeElectedMaster(true);

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(true));

        verify(licenseState).isMonitoringAllowed();
    }

    public void testDoCollect() throws Exception {
        final TimeValue timeout = mock(TimeValue.class);
        final MetaData metaData = mock(MetaData.class);
        final String clusterUuid = randomAlphaOfLength(5);
        final String nodeUuid = randomAlphaOfLength(5);
        final DiscoveryNode localNode = localNode(nodeUuid);
        final MachineLearningClient client = mock(MachineLearningClient.class);

        when(monitoringSettings.jobStatsTimeout()).thenReturn(timeout);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(metaData.clusterUUID()).thenReturn(clusterUuid);

        when(clusterService.localNode()).thenReturn(localNode);

        final JobStatsCollector collector =
                new JobStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client);

        final List<JobStats> jobStats = mockJobStats();

        @SuppressWarnings("unchecked")
        final ActionFuture<Response> future = (ActionFuture<Response>)mock(ActionFuture.class);
        final Response response = new Response(new QueryPage<>(jobStats, jobStats.size(), Job.RESULTS_FIELD));

        when(client.getJobsStats(eq(new Request(MetaData.ALL)))).thenReturn(future);
        when(future.actionGet(timeout)).thenReturn(response);

        final List<MonitoringDoc> monitoringDocs = collector.doCollect();

        assertThat(monitoringDocs, hasSize(jobStats.size()));

        for (int i = 0; i < monitoringDocs.size(); ++i) {
            final JobStatsMonitoringDoc jobStatsMonitoringDoc = (JobStatsMonitoringDoc)monitoringDocs.get(i);
            final JobStats jobStat = jobStats.get(i);

            assertThat(jobStatsMonitoringDoc.getType(), is(JobStatsMonitoringDoc.TYPE));
            assertThat(jobStatsMonitoringDoc.getSourceNode(), notNullValue());
            assertThat(jobStatsMonitoringDoc.getSourceNode().getUUID(), is(nodeUuid));
            assertThat(jobStatsMonitoringDoc.getClusterUUID(), is(clusterUuid));

            assertThat(jobStatsMonitoringDoc.getJobStats(), is(jobStat));
        }
    }

    private List<JobStats> mockJobStats() {
        final int jobs = randomIntBetween(1, 5);
        final List<JobStats> jobStats = new ArrayList<>(jobs);

        for (int i = 0; i < jobs; ++i) {
            jobStats.add(mock(JobStats.class));
        }

        return jobStats;
    }

    private DiscoveryNode localNode(final String uuid) {
        return new DiscoveryNode(uuid, new TransportAddress(TransportAddress.META_ADDRESS, 9300), Version.CURRENT);
    }

    private void whenLocalNodeElectedMaster(final boolean electedMaster) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(electedMaster);
    }

    private Settings mlEnabledSettings() {
        // since it's the default, we want to ensure we test both with/without it
        return randomBoolean() ? Settings.EMPTY : Settings.builder().put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true).build();
    }

    private Settings mlDisabledSettings() {
        return Settings.builder().put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false).build();
    }

}
