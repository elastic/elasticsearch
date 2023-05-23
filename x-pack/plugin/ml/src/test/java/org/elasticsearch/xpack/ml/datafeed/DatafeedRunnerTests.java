/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedAction.DatafeedTask;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedActionTests;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.addJobTask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedRunnerTests extends ESTestCase {

    private static final String DATAFEED_ID = "datafeed_id";
    private static final String JOB_ID = "job_id";

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private DatafeedJob datafeedJob;
    private DatafeedRunner datafeedRunner;
    private DatafeedContextProvider datafeedContextProvider;
    private DatafeedJobBuilder datafeedJobBuilder;
    private long currentTime = 120000;
    private AnomalyDetectionAuditor auditor;
    private final ArgumentCaptor<ClusterStateListener> capturedClusterStateListener = ArgumentCaptor.forClass(ClusterStateListener.class);
    private AtomicBoolean hasOpenAutodetectCommunicator;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        Job.Builder job = createDatafeedJob().setCreateTime(new Date());

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                TestDiscoveryNode.create(
                    "node_name",
                    "node_id",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    Collections.emptySet()
                )
            )
            .build();
        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks))
            .nodes(nodes);

        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(cs.build());

        DiscoveryNode dNode = mock(DiscoveryNode.class);
        when(dNode.getName()).thenReturn("this_node_has_a_name");
        when(clusterService.localNode()).thenReturn(dNode);
        auditor = mock(AnomalyDetectionAuditor.class);
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return mock(Future.class);
        }).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(MachineLearning.DATAFEED_THREAD_POOL_NAME)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        datafeedJob = mock(DatafeedJob.class);
        when(datafeedJob.isRunning()).thenReturn(true);
        when(datafeedJob.stop()).thenReturn(true);
        when(datafeedJob.getJobId()).thenReturn(job.getId());
        when(datafeedJob.getMaxEmptySearches()).thenReturn(null);
        when(datafeedJob.numberOfSearchesIn24Hours()).thenReturn(24L);
        datafeedJobBuilder = mock(DatafeedJobBuilder.class);
        doAnswer(invocationOnMock -> {
            ActionListener<DatafeedJob> listener = (ActionListener<DatafeedJob>) invocationOnMock.getArguments()[2];
            listener.onResponse(datafeedJob);
            return null;
        }).when(datafeedJobBuilder).build(any(), any(), any());

        hasOpenAutodetectCommunicator = new AtomicBoolean(true);
        AutodetectProcessManager autodetectProcessManager = mock(AutodetectProcessManager.class);
        doAnswer(invocation -> hasOpenAutodetectCommunicator.get()).when(autodetectProcessManager).hasOpenAutodetectCommunicator(anyLong());

        datafeedContextProvider = mock(DatafeedContextProvider.class);

        DatafeedConfig.Builder datafeedConfig = createDatafeedConfig(DATAFEED_ID, job.getId());
        givenDatafeedHasNeverRunBefore(job.build(), datafeedConfig.build());

        datafeedRunner = new DatafeedRunner(
            threadPool,
            mock(Client.class),
            clusterService,
            datafeedJobBuilder,
            () -> currentTime,
            auditor,
            autodetectProcessManager,
            datafeedContextProvider
        );

        verify(clusterService).addListener(capturedClusterStateListener.capture());
    }

    public void testLookbackOnly_WarnsWhenNoDataIsRetrieved() throws Exception {
        when(datafeedJob.runLookBack(0L, 60000L)).thenThrow(new DatafeedJob.EmptyDataCountException(0L, false));
        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(auditor).warning(JOB_ID, "Datafeed lookback retrieved no data");
    }

    public void testStart_GivenNewlyCreatedJobLookback() throws Exception {
        when(datafeedJob.runLookBack(0L, 60000L)).thenReturn(null);
        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
    }

    public void testStart_extractionProblem() throws Exception {
        when(datafeedJob.runLookBack(0, 60000L)).thenThrow(new DatafeedJob.ExtractionProblemException(0L, new RuntimeException("dummy")));
        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(auditor, times(1)).error(eq(JOB_ID), anyString());
    }

    public void testStart_emptyDataCountException() throws Exception {
        currentTime = 6000000;
        int[] counter = new int[] { 0 };
        doAnswer(invocationOnMock -> {
            if (counter[0]++ < 10) {
                Runnable r = (Runnable) invocationOnMock.getArguments()[0];
                currentTime += 600000;
                r.run();
            }
            return mock(Scheduler.ScheduledCancellable.class);
        }).when(threadPool).schedule(any(), any(), any());

        when(datafeedJob.runLookBack(anyLong(), anyLong())).thenThrow(new DatafeedJob.EmptyDataCountException(0L, false));
        when(datafeedJob.runRealtime()).thenThrow(new DatafeedJob.EmptyDataCountException(0L, false));

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, null);
        datafeedRunner.run(task, handler);

        verify(threadPool, times(11)).schedule(any(), any(), eq(MachineLearning.DATAFEED_THREAD_POOL_NAME));
        verify(auditor, times(1)).warning(eq(JOB_ID), anyString());
    }

    public void testRealTime_GivenStoppingAnalysisProblem() throws Exception {
        Exception cause = new RuntimeException("stopping");
        when(datafeedJob.runLookBack(anyLong(), nullable(Long.class))).thenThrow(new DatafeedJob.AnalysisProblemException(0L, true, cause));

        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams(DATAFEED_ID, 0L);
        DatafeedTask task = TransportStartDatafeedActionTests.createDatafeedTask(1, "type", "action", null, params, datafeedRunner);
        task = spyDatafeedTask(task);
        datafeedRunner.run(task, handler);

        ArgumentCaptor<DatafeedJob.AnalysisProblemException> analysisProblemCaptor = ArgumentCaptor.forClass(
            DatafeedJob.AnalysisProblemException.class
        );
        verify(handler).accept(analysisProblemCaptor.capture());
        assertThat(analysisProblemCaptor.getValue().getCause(), equalTo(cause));
        verify(auditor).error(JOB_ID, "Datafeed is encountering errors submitting data for analysis: stopping");
        assertThat(datafeedRunner.isRunning(task), is(false));
    }

    public void testRealTime_GivenNonStoppingAnalysisProblem() throws Exception {
        Exception cause = new RuntimeException("non-stopping");
        when(datafeedJob.runLookBack(anyLong(), nullable(Long.class))).thenThrow(
            new DatafeedJob.AnalysisProblemException(0L, false, cause)
        );

        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams(DATAFEED_ID, 0L);
        DatafeedTask task = TransportStartDatafeedActionTests.createDatafeedTask(1, "type", "action", null, params, datafeedRunner);
        task = spyDatafeedTask(task);
        datafeedRunner.run(task, handler);

        verify(auditor).error(JOB_ID, "Datafeed is encountering errors submitting data for analysis: non-stopping");
        assertThat(datafeedRunner.isRunning(task), is(true));
    }

    public void testStart_GivenNewlyCreatedJobLookBackAndRealtime() throws Exception {
        when(datafeedJob.runLookBack(anyLong(), anyLong())).thenReturn(1L);
        when(datafeedJob.runRealtime()).thenReturn(1L);

        Consumer<Exception> handler = mockConsumer();
        boolean cancelled = randomBoolean();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams(DATAFEED_ID, 0L);
        DatafeedTask task = TransportStartDatafeedActionTests.createDatafeedTask(1, "type", "action", null, params, datafeedRunner);
        task = spyDatafeedTask(task);
        datafeedRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        if (cancelled) {
            task.stop("test", StopDatafeedAction.DEFAULT_TIMEOUT);
            verify(handler).accept(null);
            assertThat(datafeedRunner.isRunning(task), is(false));
        } else {
            verify(threadPool, times(1)).schedule(any(), eq(new TimeValue(1)), eq(MachineLearning.DATAFEED_THREAD_POOL_NAME));
            assertThat(datafeedRunner.isRunning(task), is(true));
        }
    }

    public void testDatafeedTaskWaitsUntilJobIsOpened() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENING, tasksBuilder);
        ClusterState cs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(cs);

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        // Verify datafeed has not started running yet as job is still opening
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENING, tasksBuilder);
        addJobTask("another_job", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState anotherJobCs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", anotherJobCs, cs));

        // Still no run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder jobOpenedCs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", jobOpenedCs.build(), anotherJobCs));

        // Now it should run as the job state changed to OPENED
        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public void testDatafeedTaskWaitsUntilAutodetectCommunicatorIsOpen() {

        hasOpenAutodetectCommunicator.set(false);

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder);
        ClusterState cs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(cs);

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        // Verify datafeed has not started running yet as job doesn't have an open autodetect communicator
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("another_job", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState anotherJobCs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", anotherJobCs, cs));

        // Still no run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        hasOpenAutodetectCommunicator.set(true);

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", cs, anotherJobCs));

        // Now it should run as the autodetect communicator is open
        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public void testDatafeedTaskWaitsUntilJobIsNotStale() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder, true);
        ClusterState cs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(cs);

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        // Verify datafeed has not started running yet as job is stale (i.e. even though opened it is part way through relocating)
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder, true);
        addJobTask("another_job", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState anotherJobCs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", anotherJobCs, cs));

        // Still no run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder jobOpenedCs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", jobOpenedCs.build(), anotherJobCs));

        // Now it should run as the job state chanded to OPENED
        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public void testDatafeedTaskStopsBecauseJobFailedWhileOpening() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENING, tasksBuilder);
        ClusterState cs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(cs);

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        // Verify datafeed has not started running yet as job is still opening
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.FAILED, tasksBuilder);
        ClusterState.Builder updatedCs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", updatedCs.build(), cs));

        // Verify task never run and got stopped
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(task).stop("job_never_opened", TimeValue.timeValueSeconds(20));
    }

    public void testDatafeedGetsStoppedWhileWaitingForJobToOpen() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENING, tasksBuilder);
        ClusterState cs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(cs);

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        datafeedRunner.run(task, handler);

        // Verify datafeed has not started running yet as job is still opening
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        // Stop the datafeed
        datafeedRunner.stopDatafeed(task, "test", StopDatafeedAction.DEFAULT_TIMEOUT);

        // Update job state to opened
        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder updatedCs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", cs, updatedCs.build()));

        // Verify no datafeed was run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public void testDatafeedGetsStoppedWhileStarting() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(JOB_ID, "node_id", JobState.OPENED, tasksBuilder);
        ClusterState cs = ClusterState.builder(clusterService.state())
            .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(cs);

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask(DATAFEED_ID, 0L, 60000L);
        when(task.getStoppedOrIsolated()).thenReturn(DatafeedTask.StoppedOrIsolated.STOPPED);
        datafeedRunner.run(task, handler);

        // Verify datafeed aborted after creating context but before doing anything else
        verify(datafeedContextProvider).buildDatafeedContext(eq(DATAFEED_ID), any());
        verify(datafeedJobBuilder, never()).build(any(), any(), any());
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public static DatafeedConfig.Builder createDatafeedConfig(String datafeedId, String jobId) {
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedConfig.setIndices(Collections.singletonList("myIndex"));
        return datafeedConfig;
    }

    public static Job.Builder createDatafeedJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(
            Collections.singletonList(new Detector.Builder("metric", "field").build())
        );
        acBuilder.setBucketSpan(TimeValue.timeValueHours(1));
        acBuilder.setDetectors(Collections.singletonList(new Detector.Builder("metric", "field").build()));

        Job.Builder builder = new Job.Builder(JOB_ID);
        builder.setAnalysisConfig(acBuilder);
        builder.setDataDescription(new DataDescription.Builder());
        builder.setCreateTime(new Date());
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static DatafeedTask createDatafeedTask(String datafeedId, long startTime, Long endTime) {
        DatafeedTask task = mock(DatafeedTask.class);
        final DatafeedTask.StoppedOrIsolated stoppedOrIsolated = DatafeedTask.StoppedOrIsolated.NEITHER;
        when(task.getDatafeedId()).thenReturn(datafeedId);
        when(task.getDatafeedStartTime()).thenReturn(startTime);
        when(task.getEndTime()).thenReturn(endTime);
        doAnswer(invocationOnMock -> {
            ActionListener<PersistentTask<?>> listener = (ActionListener<PersistentTask<?>>) invocationOnMock.getArguments()[1];
            listener.onResponse(mock(PersistentTask.class));
            return null;
        }).when(task).updatePersistentTaskState(any(), any());
        when(task.getStoppedOrIsolated()).thenReturn(stoppedOrIsolated);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
            runnable.run();
            return stoppedOrIsolated;
        }).when(task).executeIfNotStoppedOrIsolated(any(Runnable.class));
        return task;
    }

    @SuppressWarnings("unchecked")
    private Consumer<Exception> mockConsumer() {
        return mock(Consumer.class);
    }

    @SuppressWarnings("unchecked")
    private DatafeedTask spyDatafeedTask(DatafeedTask task) {
        task = spy(task);
        doAnswer(invocationOnMock -> {
            ActionListener<PersistentTask<?>> listener = (ActionListener<PersistentTask<?>>) invocationOnMock.getArguments()[1];
            listener.onResponse(mock(PersistentTask.class));
            return null;
        }).when(task).updatePersistentTaskState(any(), any());
        return task;
    }

    private void givenDatafeedHasNeverRunBefore(Job job, DatafeedConfig datafeed) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<DatafeedContext> datafeedContextListener = (ActionListener<DatafeedContext>) invocationOnMock.getArguments()[1];
            DatafeedContext datafeedContext = DatafeedContext.builder()
                .setJob(job)
                .setDatafeedConfig(datafeed)
                .setRestartTimeInfo(new RestartTimeInfo(null, null, false))
                .setTimingStats(new DatafeedTimingStats(job.getId()))
                .build();
            datafeedContextListener.onResponse(datafeedContext);
            return null;
        }).when(datafeedContextProvider).buildDatafeedContext(eq(DATAFEED_ID), any());
    }
}
