/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
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
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link JobStatsCollector}.
 */
public class JobStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfMonitoringNotAllowed() {
        final Settings settings = randomFrom(mlEnabledSettings(), mlDisabledSettings());
        final boolean mlAllowed = randomBoolean();
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        // this controls the blockage
        when(licenseState.isMonitoringAllowed()).thenReturn(false);
        when(licenseState.isMachineLearningAllowed()).thenReturn(mlAllowed);

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // regardless of ML being enabled
        final Settings settings = randomFrom(mlEnabledSettings(), mlDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        when(licenseState.isMachineLearningAllowed()).thenReturn(randomBoolean());
        // this controls the blockage
        final boolean isElectedMaster = false;

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsFalseIfMLIsDisabled() {
        // this is controls the blockage
        final Settings settings = mlDisabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        when(licenseState.isMachineLearningAllowed()).thenReturn(randomBoolean());

        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));

        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsFalseIfMLIsNotAllowed() {
        final Settings settings = randomFrom(mlEnabledSettings(), mlDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        // this is controls the blockage
        when(licenseState.isMachineLearningAllowed()).thenReturn(false);
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));

        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsTrue() {
        final Settings settings = mlEnabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        when(licenseState.isMachineLearningAllowed()).thenReturn(true);
        final boolean isElectedMaster = true;

        final JobStatsCollector collector = new JobStatsCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(true));

        verify(licenseState).isMonitoringAllowed();
    }

    public void testDoCollect() throws Exception {
        final String clusterUuid = randomAlphaOfLength(5);
        whenClusterStateWithUUID(clusterUuid);

        final MonitoringDoc.Node node = randomMonitoringNode(random());
        final Client client = mock(Client.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(JobStatsCollector.JOB_STATS_TIMEOUT, timeout);

        final JobStatsCollector collector = new JobStatsCollector(Settings.EMPTY, clusterService, licenseState, client, threadContext);
        assertEquals(timeout, collector.getCollectionTimeout());

        final List<JobStats> jobStats = mockJobStats();

        @SuppressWarnings("unchecked")
        final ActionFuture<Response> future = (ActionFuture<Response>)mock(ActionFuture.class);
        final Response response = new Response(new QueryPage<>(jobStats, jobStats.size(), Job.RESULTS_FIELD));

        when(client.execute(eq(GetJobsStatsAction.INSTANCE), eq(new Request(MetaData.ALL)))).thenReturn(future);
        when(future.actionGet(timeout)).thenReturn(response);

        final long interval = randomNonNegativeLong();

        final List<MonitoringDoc> monitoringDocs = collector.doCollect(node, interval, clusterState);
        verify(clusterState).metaData();
        verify(metaData).clusterUUID();

        assertThat(monitoringDocs, hasSize(jobStats.size()));

        for (int i = 0; i < monitoringDocs.size(); ++i) {
            final JobStatsMonitoringDoc jobStatsMonitoringDoc = (JobStatsMonitoringDoc)monitoringDocs.get(i);
            final JobStats jobStat = jobStats.get(i);

            assertThat(jobStatsMonitoringDoc.getCluster(), is(clusterUuid));
            assertThat(jobStatsMonitoringDoc.getTimestamp(), greaterThan(0L));
            assertThat(jobStatsMonitoringDoc.getIntervalMillis(), equalTo(interval));
            assertThat(jobStatsMonitoringDoc.getNode(), equalTo(node));
            assertThat(jobStatsMonitoringDoc.getSystem(), is(MonitoredSystem.ES));
            assertThat(jobStatsMonitoringDoc.getType(), is(JobStatsMonitoringDoc.TYPE));
            assertThat(jobStatsMonitoringDoc.getId(), nullValue());

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

    private Settings mlEnabledSettings() {
        // since it's the default, we want to ensure we test both with/without it
        return randomBoolean() ? Settings.EMPTY : Settings.builder().put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true).build();
    }

    private Settings mlDisabledSettings() {
        return Settings.builder().put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false).build();
    }

}
