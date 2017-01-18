/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.action.StartSchedulerAction;
import org.elasticsearch.xpack.ml.action.UpdateSchedulerStatusAction;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.DataCounts;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.Detector;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.JobStatus;
import org.elasticsearch.xpack.ml.job.audit.Auditor;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.scheduler.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.scheduler.extractor.DataExtractorFactory;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.action.UpdateSchedulerStatusAction.INSTANCE;
import static org.elasticsearch.xpack.ml.action.UpdateSchedulerStatusAction.Request;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScheduledJobRunnerTests extends ESTestCase {

    private Client client;
    private ActionFuture<PostDataAction.Response> jobDataFuture;
    private ActionFuture<FlushJobAction.Response> flushJobFuture;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private DataExtractorFactory dataExtractorFactory;
    private ScheduledJobRunner scheduledJobRunner;
    private long currentTime = 120000;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        client = mock(Client.class);
        jobDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        clusterService = mock(ClusterService.class);
        doAnswer(invocation -> {
            @SuppressWarnings("rawtypes")
            ActionListener<Object> actionListener = (ActionListener) invocation.getArguments()[2];
            actionListener.onResponse(new UpdateSchedulerStatusAction.Response());
            return null;
        }).when(client).execute(same(UpdateSchedulerStatusAction.INSTANCE), any(), any());

        JobProvider jobProvider = mock(JobProvider.class);
        Mockito.doAnswer(invocationOnMock -> {
            String jobId = (String) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            handler.accept(new DataCounts(jobId));
            return null;
        }).when(jobProvider).dataCounts(any(), any(), any());
        dataExtractorFactory = mock(DataExtractorFactory.class);
        Auditor auditor = mock(Auditor.class);
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(MlPlugin.SCHEDULED_RUNNER_THREAD_POOL_NAME)).thenReturn(executorService);
        when(client.execute(same(PostDataAction.INSTANCE), any())).thenReturn(jobDataFuture);
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);

        scheduledJobRunner = new ScheduledJobRunner(threadPool, client, clusterService, jobProvider, () -> currentTime) {
            @Override
            DataExtractorFactory createDataExtractorFactory(SchedulerConfig schedulerConfig, Job job) {
                return dataExtractorFactory;
            }
        };

        when(jobProvider.audit(anyString())).thenReturn(auditor);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            Consumer consumer = (Consumer) invocationOnMock.getArguments()[3];
            consumer.accept(new ResourceNotFoundException("dummy"));
            return null;
        }).when(jobProvider).buckets(any(), any(), any(), any());
    }

    public void testStart_GivenNewlyCreatedJobLoopBack() throws Exception {
        Job.Builder jobBuilder = createScheduledJob();
        SchedulerConfig schedulerConfig = createSchedulerConfig("scheduler1", "foo").build();
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = jobBuilder.build();
        MlMetadata mlMetadata = new MlMetadata.Builder()
                .putJob(job, false)
                .putScheduler(schedulerConfig)
                .updateStatus("foo", JobStatus.OPENED, null)
                .build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata))
                .build());

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(jobDataFuture.get()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        StartSchedulerAction.SchedulerTask task = mock(StartSchedulerAction.SchedulerTask.class);
        scheduledJobRunner.run("scheduler1", 0L, 60000L, task, handler);

        verify(threadPool, times(1)).executor(MlPlugin.SCHEDULED_RUNNER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client).execute(same(PostDataAction.INSTANCE), eq(createExpectedPostDataRequest(job)));
        verify(client).execute(same(FlushJobAction.INSTANCE), any());
        verify(client).execute(same(INSTANCE), eq(new Request("scheduler1", SchedulerStatus.STARTED)), any());
        verify(client).execute(same(INSTANCE), eq(new Request("scheduler1", SchedulerStatus.STOPPED)), any());
    }

    private static PostDataAction.Request createExpectedPostDataRequest(Job job) {
        DataDescription.Builder expectedDataDescription = new DataDescription.Builder();
        expectedDataDescription.setTimeFormat("epoch_ms");
        expectedDataDescription.setFormat(DataDescription.DataFormat.JSON);
        PostDataAction.Request expectedPostDataRequest = new PostDataAction.Request(job.getId());
        expectedPostDataRequest.setDataDescription(expectedDataDescription.build());
        return expectedPostDataRequest;
    }

    public void testStart_extractionProblem() throws Exception {
        Job.Builder jobBuilder = createScheduledJob();
        SchedulerConfig schedulerConfig = createSchedulerConfig("scheduler1", "foo").build();
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = jobBuilder.build();
        MlMetadata mlMetadata = new MlMetadata.Builder()
                .putJob(job, false)
                .putScheduler(schedulerConfig)
                .updateStatus("foo", JobStatus.OPENED, null)
                .build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata))
                .build());

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenThrow(new RuntimeException("dummy"));
        when(jobDataFuture.get()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        StartSchedulerAction.SchedulerTask task = mock(StartSchedulerAction.SchedulerTask.class);
        scheduledJobRunner.run("scheduler1", 0L, 60000L, task, handler);

        verify(threadPool, times(1)).executor(MlPlugin.SCHEDULED_RUNNER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client, never()).execute(same(PostDataAction.INSTANCE), eq(new PostDataAction.Request("foo")));
        verify(client, never()).execute(same(FlushJobAction.INSTANCE), any());
        verify(client).execute(same(INSTANCE), eq(new Request("scheduler1", SchedulerStatus.STARTED)), any());
        verify(client).execute(same(INSTANCE), eq(new Request("scheduler1", SchedulerStatus.STOPPED)), any());
    }

    public void testStart_GivenNewlyCreatedJobLoopBackAndRealtime() throws Exception {
        Job.Builder jobBuilder = createScheduledJob();
        SchedulerConfig schedulerConfig = createSchedulerConfig("scheduler1", "foo").build();
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = jobBuilder.build();
        MlMetadata mlMetadata = new MlMetadata.Builder()
                .putJob(job, false)
                .putScheduler(schedulerConfig)
                .updateStatus("foo", JobStatus.OPENED, null)
                .build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata))
                .build());

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(jobDataFuture.get()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        boolean cancelled = randomBoolean();
        StartSchedulerAction.SchedulerTask task = new StartSchedulerAction.SchedulerTask(1, "type", "action", null, "scheduler1");
        scheduledJobRunner.run("scheduler1", 0L, null, task, handler);

        verify(threadPool, times(1)).executor(MlPlugin.SCHEDULED_RUNNER_THREAD_POOL_NAME);
        if (cancelled) {
            task.stop();
            verify(client).execute(same(INSTANCE), eq(new Request("scheduler1", SchedulerStatus.STOPPED)), any());
        } else {
            verify(client).execute(same(PostDataAction.INSTANCE), eq(createExpectedPostDataRequest(job)));
            verify(client).execute(same(FlushJobAction.INSTANCE), any());
            verify(threadPool, times(1)).schedule(eq(new TimeValue(480100)), eq(MlPlugin.SCHEDULED_RUNNER_THREAD_POOL_NAME), any());
        }
    }

    public static SchedulerConfig.Builder createSchedulerConfig(String schedulerId, String jobId) {
        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(schedulerId, jobId);
        schedulerConfig.setIndexes(Arrays.asList("myIndex"));
        schedulerConfig.setTypes(Arrays.asList("myType"));
        return schedulerConfig;
    }

    public static Job.Builder createScheduledJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(3600L);
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));

        Job.Builder builder = new Job.Builder("foo");
        builder.setAnalysisConfig(acBuilder);
        return builder;
    }

    public void testValidate() {
        Job job1 = createScheduledJob().build();
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> ScheduledJobRunner.validate("some-scheduler", mlMetadata1));
        assertThat(e.getMessage(), equalTo("No scheduler with id [some-scheduler] exists"));

        SchedulerConfig schedulerConfig1 = createSchedulerConfig("foo-scheduler", "foo").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putScheduler(schedulerConfig1)
                .build();
        e = expectThrows(ElasticsearchStatusException.class,
                () -> ScheduledJobRunner.validate("foo-scheduler", mlMetadata2));
        assertThat(e.getMessage(), equalTo("cannot start scheduler, expected job status [OPENED], but got [CLOSED]"));

        MlMetadata mlMetadata3 = new MlMetadata.Builder(mlMetadata2)
                .updateStatus("foo", JobStatus.OPENED, null)
                .updateSchedulerStatus("foo-scheduler", SchedulerStatus.STARTED)
                .build();
        e = expectThrows(ElasticsearchStatusException.class,
                () -> ScheduledJobRunner.validate("foo-scheduler", mlMetadata3));
        assertThat(e.getMessage(), equalTo("scheduler already started, expected scheduler status [STOPPED], but got [STARTED]"));
    }

    @SuppressWarnings("unchecked")
    private Consumer<Exception> mockConsumer() {
        return mock(Consumer.class);
    }
}