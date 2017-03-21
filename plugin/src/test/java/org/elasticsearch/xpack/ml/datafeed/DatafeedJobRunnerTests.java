/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.MockClientBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.notifications.AuditMessage;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.persistent.PersistentTasks;
import org.elasticsearch.xpack.persistent.PersistentTasks.PersistentTask;
import org.elasticsearch.xpack.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.xpack.persistent.UpdatePersistentTaskStatusAction.Response;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.createJobTask;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedJobRunnerTests extends ESTestCase {

    private Client client;
    private ActionFuture<PostDataAction.Response> jobDataFuture;
    private ActionFuture<FlushJobAction.Response> flushJobFuture;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private DataExtractorFactory dataExtractorFactory;
    private DatafeedJobRunner datafeedJobRunner;
    private long currentTime = 120000;
    private Auditor auditor;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createDatafeedJob().build();
        mlMetadata.putJob(job, false);
        mlMetadata.putDatafeed(createDatafeedConfig("datafeed_id", job.getId()).build());
        PersistentTask<OpenJobAction.Request> task = createJobTask(0L, job.getId(), "node_id", JobState.OPENED);
        PersistentTasks tasks = new PersistentTasks(1L, Collections.singletonMap(0L, task));
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();
        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasks.TYPE, tasks))
                .nodes(nodes);

        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(cs.build());


        ArgumentCaptor<XContentBuilder> argumentCaptor = ArgumentCaptor.forClass(XContentBuilder.class);
        client = new MockClientBuilder("foo")
                .prepareIndex(Auditor.NOTIFICATIONS_INDEX, AuditMessage.TYPE.getPreferredName(), "responseId", argumentCaptor)
                .build();

        jobDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        DiscoveryNode dNode = mock(DiscoveryNode.class);
        when(dNode.getName()).thenReturn("this_node_has_a_name");
        when(clusterService.localNode()).thenReturn(dNode);
        auditor = mock(Auditor.class);

        JobProvider jobProvider = mock(JobProvider.class);
        Mockito.doAnswer(invocationOnMock -> {
            String jobId = (String) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            handler.accept(new DataCounts(jobId));
            return null;
        }).when(jobProvider).dataCounts(any(), any(), any());
        dataExtractorFactory = mock(DataExtractorFactory.class);
        auditor = mock(Auditor.class);
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);
        when(client.execute(same(PostDataAction.INSTANCE), any())).thenReturn(jobDataFuture);
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);

        datafeedJobRunner = new DatafeedJobRunner(threadPool, client, clusterService, jobProvider, () -> currentTime, auditor) {
            @Override
            DataExtractorFactory createDataExtractorFactory(DatafeedConfig datafeedConfig, Job job) {
                return dataExtractorFactory;
            }
        };

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            Consumer consumer = (Consumer) invocationOnMock.getArguments()[3];
            consumer.accept(new ResourceNotFoundException("dummy"));
            return null;
        }).when(jobProvider).bucketsViaInternalClient(any(), any(), any(), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(new Response(true));
            return null;
        }).when(client).execute(same(UpdatePersistentTaskStatusAction.INSTANCE), any(), any());
    }

    public void testLookbackOnly_WarnsWhenNoDataIsRetrieved() throws Exception {
        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(Optional.empty());
        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedJobRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client, never()).execute(same(PostDataAction.INSTANCE), eq(new PostDataAction.Request("foo")));
        verify(client, never()).execute(same(FlushJobAction.INSTANCE), any());
        verify(auditor).warning("job_id", "Datafeed lookback retrieved no data");
    }

    public void testStart_GivenNewlyCreatedJobLoopBack() throws Exception {
        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        DataCounts dataCounts = new DataCounts("job_id", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                new Date(0), new Date(0), new Date(0), new Date(0), new Date(0));
        when(jobDataFuture.actionGet()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedJobRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client).execute(same(PostDataAction.INSTANCE), eq(createExpectedPostDataRequest("job_id")));
        verify(client).execute(same(FlushJobAction.INSTANCE), any());
    }

    private static PostDataAction.Request createExpectedPostDataRequest(String jobId) {
        DataDescription.Builder expectedDataDescription = new DataDescription.Builder();
        expectedDataDescription.setTimeFormat("epoch_ms");
        expectedDataDescription.setFormat(DataDescription.DataFormat.JSON);
        PostDataAction.Request expectedPostDataRequest = new PostDataAction.Request(jobId);
        expectedPostDataRequest.setDataDescription(expectedDataDescription.build());
        return expectedPostDataRequest;
    }

    public void testStart_extractionProblem() throws Exception {
        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenThrow(new RuntimeException("dummy"));
        DataCounts dataCounts = new DataCounts("job_id", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                new Date(0), new Date(0), new Date(0), new Date(0), new Date(0));
        when(jobDataFuture.actionGet()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedTask task = createDatafeedTask("datafeed_id", 0L, 60000L);
        datafeedJobRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client, never()).execute(same(PostDataAction.INSTANCE), eq(new PostDataAction.Request("foo")));
        verify(client, never()).execute(same(FlushJobAction.INSTANCE), any());
    }

    public void testStart_emptyDataCountException() throws Exception {
        currentTime = 6000000;
        Job.Builder jobBuilder = createDatafeedJob();
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed1", "job_id").build();
        Job job = jobBuilder.build();
        MlMetadata mlMetadata = new MlMetadata.Builder()
                .putJob(job, false)
                .putDatafeed(datafeedConfig)
                .build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata))
                .build());
        int[] counter = new int[] {0};
        doAnswer(invocationOnMock -> {
            if (counter[0]++ < 10) {
                Runnable r = (Runnable) invocationOnMock.getArguments()[2];
                currentTime += 600000;
                r.run();
            }
            return mock(ScheduledFuture.class);
        }).when(threadPool).schedule(any(), any(), any());

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(false);
        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedTask task = createDatafeedTask("datafeed_id", 0L, null);
        DatafeedJobRunner.Holder holder = datafeedJobRunner.createJobDatafeed(datafeedConfig, job, 100, 100, handler, task);
        datafeedJobRunner.doDatafeedRealtime(10L, "foo", holder);

        verify(threadPool, times(11)).schedule(any(), eq(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME), any());
        verify(auditor, times(1)).warning(eq("job_id"), anyString());
        verify(client, never()).execute(same(PostDataAction.INSTANCE), any());
        verify(client, never()).execute(same(FlushJobAction.INSTANCE), any());
    }

    public void testStart_GivenNewlyCreatedJobLoopBackAndRealtime() throws Exception {
        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        DataCounts dataCounts = new DataCounts("job_id", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                new Date(0), new Date(0), new Date(0), new Date(0), new Date(0));
        when(jobDataFuture.actionGet()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        boolean cancelled = randomBoolean();
        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request("datafeed_id", 0L);
        StartDatafeedAction.DatafeedTask task = new StartDatafeedAction.DatafeedTask(1, "type", "action", null,
                startDatafeedRequest, datafeedJobRunner);
        datafeedJobRunner.run(task, handler);

        verify(threadPool, times(1)).executor(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME);
        if (cancelled) {
            task.stop("test");
            verify(handler).accept(null);
        } else {
            verify(client).execute(same(PostDataAction.INSTANCE), eq(createExpectedPostDataRequest("job_id")));
            verify(client).execute(same(FlushJobAction.INSTANCE), any());
            verify(threadPool, times(1)).schedule(eq(new TimeValue(480100)), eq(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME), any());
        }
    }

    public static DatafeedConfig.Builder createDatafeedConfig(String datafeedId, String jobId) {
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedConfig.setIndexes(Arrays.asList("myIndex"));
        datafeedConfig.setTypes(Arrays.asList("myType"));
        return datafeedConfig;
    }

    public static Job.Builder createDatafeedJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(TimeValue.timeValueHours(1));
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));

        Job.Builder builder = new Job.Builder("job_id");
        builder.setAnalysisConfig(acBuilder);
        builder.setCreateTime(new Date());
        return builder;
    }

    private static StartDatafeedAction.DatafeedTask createDatafeedTask(String datafeedId, long startTime, Long endTime) {
        StartDatafeedAction.DatafeedTask task = mock(StartDatafeedAction.DatafeedTask.class);
        when(task.getDatafeedId()).thenReturn(datafeedId);
        when(task.getDatafeedStartTime()).thenReturn(startTime);
        when(task.getEndTime()).thenReturn(endTime);
        return task;
    }

    @SuppressWarnings("unchecked")
    private Consumer<Exception> mockConsumer() {
        return mock(Consumer.class);
    }
}
