/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.job;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.ForecastTelemetry;
import org.elasticsearch.xpack.ml.job.task.JobTask;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.NativeStorageProvider;
import org.junit.Before;

import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportForecastJobActionTelemetryTests extends ESTestCase {

    private static final String JOB_ID = "forecast-telemetry-job";

    private RecordingMeterRegistry meterRegistry;
    private JobManager jobManager;
    private JobResultsProvider jobResultsProvider;
    private AutodetectProcessManager processManager;
    private TransportForecastJobAction action;
    private JobTask jobTask;

    @Before
    public void setUpAction() {
        meterRegistry = new RecordingMeterRegistry();
        jobManager = mock(JobManager.class);
        jobResultsProvider = mock(JobResultsProvider.class);
        processManager = mock(AutodetectProcessManager.class);
        jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn(JOB_ID);
        // The transport and cluster services are only touched by the base class constructor.
        action = new TransportForecastJobAction(
            mock(TransportService.class),
            Settings.EMPTY,
            mock(ClusterService.class),
            new ActionFilters(Set.of()),
            jobResultsProvider,
            processManager,
            jobManager,
            mock(NativeStorageProvider.class),
            mock(AnomalyDetectionAuditor.class),
            new ForecastTelemetry(meterRegistry)
        );
    }

    public void testAcceptedRequestShouldCountRequestedOnce() {
        stubJob(buildJob(MlConfigVersion.CURRENT));
        stubProcessHandsOffForecast(null);
        stubForecastStats(ForecastRequestStats.ForecastRequestStatus.FINISHED);

        PlainActionFuture<ForecastJobAction.Response> future = runForecast(new ForecastJobAction.Request(JOB_ID));

        assertThat(future.actionGet().isAcknowledged(), equalTo(true));
        assertThat(count(ForecastTelemetry.REQUESTED_METRIC), equalTo(1L));
        // Process-side outcomes belong to the results processor, not to this action.
        assertThat(count(ForecastTelemetry.FINISHED_METRIC), equalTo(0L));
        assertThat(count(ForecastTelemetry.FAILED_METRIC), equalTo(0L));
    }

    public void testRejectedRequestShouldNotCountAnything() {
        stubJob(buildJob(MlConfigVersion.fromString("6.0.1")));

        PlainActionFuture<ForecastJobAction.Response> future = runForecast(new ForecastJobAction.Request(JOB_ID));

        expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(count(ForecastTelemetry.REQUESTED_METRIC), equalTo(0L));
        assertThat(count(ForecastTelemetry.FAILED_METRIC), equalTo(0L));
    }

    public void testProcessHandOffFailureShouldCountRequestedAndLeaveOutcomeToProcessSide() {
        stubJob(buildJob(MlConfigVersion.CURRENT));
        stubProcessHandsOffForecast(new ElasticsearchStatusException("flush failed", RestStatus.INTERNAL_SERVER_ERROR));

        PlainActionFuture<ForecastJobAction.Response> future = runForecast(new ForecastJobAction.Request(JOB_ID));

        assertThat(expectThrows(Exception.class, future::actionGet), instanceOf(ElasticsearchStatusException.class));
        assertThat(count(ForecastTelemetry.REQUESTED_METRIC), equalTo(1L));
        assertThat(count(ForecastTelemetry.FAILED_METRIC), equalTo(0L));
        assertThat(count(ForecastTelemetry.FINISHED_METRIC), equalTo(0L));
    }

    public void testProcessSideFailureShouldNotCountFailedInAction() {
        stubJob(buildJob(MlConfigVersion.CURRENT));
        stubProcessHandsOffForecast(null);
        stubForecastStats(ForecastRequestStats.ForecastRequestStatus.FAILED);

        PlainActionFuture<ForecastJobAction.Response> future = runForecast(new ForecastJobAction.Request(JOB_ID));

        expectThrows(Exception.class, future::actionGet);
        assertThat(count(ForecastTelemetry.REQUESTED_METRIC), equalTo(1L));
        assertThat(count(ForecastTelemetry.FAILED_METRIC), equalTo(0L));
    }

    private PlainActionFuture<ForecastJobAction.Response> runForecast(ForecastJobAction.Request request) {
        PlainActionFuture<ForecastJobAction.Response> future = new PlainActionFuture<>();
        action.taskOperation(mock(CancellableTask.class), request, jobTask, future);
        return future;
    }

    @SuppressWarnings("unchecked")
    private void stubJob(Job job) {
        doAnswer(invocation -> {
            invocation.getArgument(1, ActionListener.class).onResponse(job);
            return null;
        }).when(jobManager).getJob(anyString(), any());
    }

    @SuppressWarnings("unchecked")
    private void stubProcessHandsOffForecast(Exception failure) {
        doAnswer(invocation -> {
            invocation.getArgument(2, Consumer.class).accept(failure);
            return null;
        }).when(processManager).forecastJob(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private void stubForecastStats(ForecastRequestStats.ForecastRequestStatus status) {
        ForecastRequestStats stats = new ForecastRequestStats(JOB_ID, "forecast_1");
        stats.setStatus(status);
        stats.setMessages(Collections.singletonList("forecast failed in the process"));
        doAnswer(invocation -> {
            invocation.getArgument(2, Consumer.class).accept(stats);
            return null;
        }).when(jobResultsProvider).getForecastRequestStats(anyString(), anyString(), any(), any());
    }

    private long count(String metricName) {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, metricName)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static Job buildJob(MlConfigVersion jobVersion) {
        Detector.Builder detector = new Detector.Builder("mean", "value");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder jobBuilder = new Job.Builder(JOB_ID).setCreateTime(new Date())
            .setAnalysisConfig(analysisConfig)
            .setDataDescription(dataDescription);
        jobBuilder.setJobVersion(jobVersion);
        return jobBuilder.build();
    }
}
