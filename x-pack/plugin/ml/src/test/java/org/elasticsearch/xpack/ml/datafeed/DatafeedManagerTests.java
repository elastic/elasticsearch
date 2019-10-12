/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedAction.DatafeedTask;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedActionTests;
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

import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedManagerTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private DatafeedJob datafeedJob;
    private DatafeedManager datafeedManager;
    private long currentTime = 120000;
    private AnomalyDetectionAuditor auditor;
    private ArgumentCaptor<ClusterStateListener> capturedClusterStateListener = ArgumentCaptor.forClass(ClusterStateListener.class);
    private AtomicBoolean hasOpenAutodetectCommunicator;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        Job.Builder job = createDatafeedJob().setCreateTime(new Date());

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();
        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasks))
                .nodes(nodes);

        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(cs.build());

        DiscoveryNode dNode = mock(DiscoveryNode.class);
        when(dNode.getName()).thenReturn("this_node_has_a_name");
        when(clusterService.localNode()).thenReturn(dNode);
        auditor = mock(AnomalyDetectionAuditor.class);

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
        DatafeedJobBuilder datafeedJobBuilder = mock(DatafeedJobBuilder.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(datafeedJob);
            return null;
        }).when(datafeedJobBuilder).build(any(), any());

        hasOpenAutodetectCommunicator = new AtomicBoolean(true);
        AutodetectProcessManager autodetectProcessManager = mock(AutodetectProcessManager.class);
        doAnswer(invocation -> hasOpenAutodetectCommunicator.get()).when(autodetectProcessManager).hasOpenAutodetectCommunicator(anyLong());

        datafeedManager = new DatafeedManager(threadPool, mock(Client.class), clusterService, datafeedJobBuilder,
                () -> currentTime, auditor, autodetectProcessManager);

        verify(clusterService).addListener(capturedClusterStateListener.capture());
    }

    public void testLookbackOnly_WarnsWhenNoDataIsRetrieved() throws Exception {
        when(datafeedJob.runLookBack(0L, 60000L)).thenThrow(new DatafeedJob.EmptyDataCountException(0L, false));
        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(auditor).warning("job_id", "Datafeed lookback retrieved no data");
    }

    public void testStart_GivenNewlyCreatedJobLookback() throws Exception {
        when(datafeedJob.runLookBack(0L, 60000L)).thenReturn(null);
        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
    }

    public void testStart_extractionProblem() throws Exception {
        when(datafeedJob.runLookBack(0, 60000L)).thenThrow(new DatafeedJob.ExtractionProblemException(0L, new RuntimeException("dummy")));
        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(auditor, times(1)).error(eq("job_id"), anyString());
    }

    public void testStart_emptyDataCountException() throws Exception {
        currentTime = 6000000;
        int[] counter = new int[] {0};
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
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, null);
        datafeedManager.run(task, handler);

        verify(threadPool, times(11)).schedule(any(), any(), eq(MachineLearning.DATAFEED_THREAD_POOL_NAME));
        verify(auditor, times(1)).warning(eq("job_id"), anyString());
    }

    public void testRealTime_GivenStoppingAnalysisProblem() throws Exception {
        Exception cause = new RuntimeException("stopping");
        when(datafeedJob.runLookBack(anyLong(), anyLong())).thenThrow(new DatafeedJob.AnalysisProblemException(0L, true, cause));

        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("datafeed_id", 0L);
        DatafeedTask task = TransportStartDatafeedActionTests.createDatafeedTask(1, "type", "action", null,
                params, datafeedManager);
        task = spyDatafeedTask(task);
        datafeedManager.run(task, handler);

        ArgumentCaptor<DatafeedJob.AnalysisProblemException> analysisProblemCaptor =
                ArgumentCaptor.forClass(DatafeedJob.AnalysisProblemException.class);
        verify(handler).accept(analysisProblemCaptor.capture());
        assertThat(analysisProblemCaptor.getValue().getCause(), equalTo(cause));
        verify(auditor).error("job_id", "Datafeed is encountering errors submitting data for analysis: stopping");
        assertThat(datafeedManager.isRunning(task.getAllocationId()), is(false));
    }

    public void testRealTime_GivenNonStoppingAnalysisProblem() throws Exception {
        Exception cause = new RuntimeException("non-stopping");
        when(datafeedJob.runLookBack(anyLong(), anyLong())).thenThrow(new DatafeedJob.AnalysisProblemException(0L, false, cause));

        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("datafeed_id", 0L);
        DatafeedTask task = TransportStartDatafeedActionTests.createDatafeedTask(1, "type", "action", null,
                params, datafeedManager);
        task = spyDatafeedTask(task);
        datafeedManager.run(task, handler);

        verify(auditor).error("job_id", "Datafeed is encountering errors submitting data for analysis: non-stopping");
        assertThat(datafeedManager.isRunning(task.getAllocationId()), is(true));
    }

    public void testStart_GivenNewlyCreatedJobLookBackAndRealtime() throws Exception {
        when(datafeedJob.runLookBack(anyLong(), anyLong())).thenReturn(1L);
        when(datafeedJob.runRealtime()).thenReturn(1L);

        Consumer<Exception> handler = mockConsumer();
        boolean cancelled = randomBoolean();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("datafeed_id", 0L);
        DatafeedTask task = TransportStartDatafeedActionTests.createDatafeedTask(1, "type", "action", null,
                params, datafeedManager);
        task = spyDatafeedTask(task);
        datafeedManager.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        if (cancelled) {
            task.stop("test", StopDatafeedAction.DEFAULT_TIMEOUT);
            verify(handler).accept(null);
            assertThat(datafeedManager.isRunning(task.getAllocationId()), is(false));
        } else {
            verify(threadPool, times(1)).schedule(any(), eq(new TimeValue(1)), eq(MachineLearning.DATAFEED_THREAD_POOL_NAME));
            assertThat(datafeedManager.isRunning(task.getAllocationId()), is(true));
        }
    }

    public void testDatafeedTaskWaitsUntilJobIsOpened() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENING, tasksBuilder);
        ClusterState.Builder cs = ClusterState.builder(clusterService.state())
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));
        when(clusterService.state()).thenReturn(cs.build());

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        // Verify datafeed has not started running yet as job is still opening
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENING, tasksBuilder);
        addJobTask("another_job", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder anotherJobCs = ClusterState.builder(clusterService.state())
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", anotherJobCs.build(), cs.build()));

        // Still no run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder jobOpenedCs = ClusterState.builder(clusterService.state())
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(
                new ClusterChangedEvent("_source", jobOpenedCs.build(), anotherJobCs.build()));

        // Now it should run as the job state changed to OPENED
        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public void testDatafeedTaskWaitsUntilAutodetectCommunicatorIsOpen() {

        hasOpenAutodetectCommunicator.set(false);

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder cs = ClusterState.builder(clusterService.state())
            .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));
        when(clusterService.state()).thenReturn(cs.build());

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        // Verify datafeed has not started running yet as job doesn't have an open autodetect communicator
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("another_job", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder anotherJobCs = ClusterState.builder(clusterService.state())
            .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", anotherJobCs.build(), cs.build()));

        // Still no run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        hasOpenAutodetectCommunicator.set(true);

        capturedClusterStateListener.getValue().clusterChanged(
            new ClusterChangedEvent("_source", cs.build(), anotherJobCs.build()));

        // Now it should run as the autodetect communicator is open
        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public void testDatafeedTaskWaitsUntilJobIsNotStale() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasksBuilder, true);
        ClusterState.Builder cs = ClusterState.builder(clusterService.state())
            .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));
        when(clusterService.state()).thenReturn(cs.build());

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        // Verify datafeed has not started running yet as job is stale (i.e. even though opened it is part way through relocating)
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasksBuilder, true);
        addJobTask("another_job", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder anotherJobCs = ClusterState.builder(clusterService.state())
            .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", anotherJobCs.build(), cs.build()));

        // Still no run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder jobOpenedCs = ClusterState.builder(clusterService.state())
            .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(
            new ClusterChangedEvent("_source", jobOpenedCs.build(), anotherJobCs.build()));

        // Now it should run as the job state chanded to OPENED
        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public void testDatafeedTaskStopsBecauseJobFailedWhileOpening() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENING, tasksBuilder);
        ClusterState.Builder cs = ClusterState.builder(clusterService.state())
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));
        when(clusterService.state()).thenReturn(cs.build());

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        // Verify datafeed has not started running yet as job is still opening
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.FAILED, tasksBuilder);
        ClusterState.Builder updatedCs = ClusterState.builder(clusterService.state())
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", updatedCs.build(), cs.build()));

        // Verify task never run and got stopped
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
        verify(task).stop("job_never_opened", TimeValue.timeValueSeconds(20));
    }

    public void testDatafeedGetsStoppedWhileWaitingForJobToOpen() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENING, tasksBuilder);
        ClusterState.Builder cs = ClusterState.builder(clusterService.state())
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));
        when(clusterService.state()).thenReturn(cs.build());

        Consumer<Exception> handler = mockConsumer();
        DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedManager.run(task, handler);

        // Verify datafeed has not started running yet as job is still opening
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);

        // Stop the datafeed
        datafeedManager.stopDatafeed(task, "test", StopDatafeedAction.DEFAULT_TIMEOUT);

        // Update job state to opened
        tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasksBuilder);
        ClusterState.Builder updatedCs = ClusterState.builder(clusterService.state())
                .metaData(new MetaData.Builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()));

        capturedClusterStateListener.getValue().clusterChanged(new ClusterChangedEvent("_source", cs.build(), updatedCs.build()));

        // Verify no datafeed was run
        verify(threadPool, never()).executor(MachineLearning.DATAFEED_THREAD_POOL_NAME);
    }

    public static DatafeedConfig.Builder createDatafeedConfig(String datafeedId, String jobId) {
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedConfig.setIndices(Collections.singletonList("myIndex"));
        return datafeedConfig;
    }

    public static Job.Builder createDatafeedJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Collections.singletonList(
                new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(TimeValue.timeValueHours(1));
        acBuilder.setDetectors(Collections.singletonList(new Detector.Builder("metric", "field").build()));

        Job.Builder builder = new Job.Builder("job_id");
        builder.setAnalysisConfig(acBuilder);
        builder.setDataDescription(new DataDescription.Builder());
        return builder;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static DatafeedTask createDatafeedTask(String datafeedId, long startTime, Long endTime) {
        DatafeedTask task = mock(DatafeedTask.class);
        when(task.getDatafeedId()).thenReturn(datafeedId);
        when(task.getDatafeedStartTime()).thenReturn(startTime);
        when(task.getEndTime()).thenReturn(endTime);
        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(mock(PersistentTask.class));
            return null;
        }).when(task).updatePersistentTaskState(any(), any());
        return task;
    }

    @SuppressWarnings("unchecked")
    private Consumer<Exception> mockConsumer() {
        return mock(Consumer.class);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private DatafeedTask spyDatafeedTask(DatafeedTask task) {
        task = spy(task);
        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(mock(PersistentTask.class));
            return null;
        }).when(task).updatePersistentTaskState(any(), any());
        return task;
    }
}
