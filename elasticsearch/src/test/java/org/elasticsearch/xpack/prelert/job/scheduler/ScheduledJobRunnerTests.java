/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.StartJobSchedulerAction;
import org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction;
import org.elasticsearch.xpack.prelert.job.AnalysisConfig;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.DataDescription;
import org.elasticsearch.xpack.prelert.job.Detector;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerConfig;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractorFactory;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction.INSTANCE;
import static org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction.Request;
import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
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
    private ThreadPool threadPool;
    private DataProcessor dataProcessor;
    private DataExtractorFactory dataExtractorFactory;
    private ScheduledJobRunner scheduledJobRunner;
    private long currentTime = 120000;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Object> actionListener = (ActionListener) invocation.getArguments()[2];
            actionListener.onResponse(new UpdateJobSchedulerStatusAction.Response());
            return null;
        }).when(client).execute(eq(UpdateJobSchedulerStatusAction.INSTANCE), any(), any());

        JobProvider jobProvider = mock(JobProvider.class);
        when(jobProvider.dataCounts(anyString())).thenReturn(new DataCounts("foo"));
        dataProcessor = mock(DataProcessor.class);
        dataExtractorFactory = mock(DataExtractorFactory.class);
        Auditor auditor = mock(Auditor.class);
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(PrelertPlugin.SCHEDULER_THREAD_POOL_NAME)).thenReturn(executorService);

        scheduledJobRunner =
                new ScheduledJobRunner(threadPool, client, jobProvider, dataProcessor, dataExtractorFactory, () -> currentTime);

        when(jobProvider.audit(anyString())).thenReturn(auditor);
        when(jobProvider.buckets(anyString(), any(BucketsQueryBuilder.BucketsQuery.class))).thenThrow(
                QueryPage.emptyQueryPage(Job.RESULTS_FIELD));
    }

    public void testStart_GivenNewlyCreatedJobLoopBack() throws IOException {
        Job.Builder builder = createScheduledJob();
        SchedulerState schedulerState = new SchedulerState(JobSchedulerStatus.STOPPED, 0L, 60000L);
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = builder.build();
        Allocation allocation =
                new Allocation(null, "foo", false, JobStatus.OPENED, null, new SchedulerState(JobSchedulerStatus.STOPPED, null, null));

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(job)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(dataProcessor.processData(anyString(), eq(in), any(), any())).thenReturn(dataCounts);
        Consumer<Exception> handler = mockConsumer();
        StartJobSchedulerAction.SchedulerTask task = mock(StartJobSchedulerAction.SchedulerTask.class);
        scheduledJobRunner.run(job, schedulerState, allocation, task, handler);

        verify(dataExtractor).newSearch(eq(0L), eq(60000L), any());
        verify(threadPool, times(1)).executor(PrelertPlugin.SCHEDULER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client).execute(same(INSTANCE), eq(new Request("foo", JobSchedulerStatus.STARTED)), any());
        verify(client).execute(same(INSTANCE), eq(new Request("foo", JobSchedulerStatus.STOPPED)), any());
    }

    public void testStart_GivenNewlyCreatedJobLoopBackAndRealtime() throws IOException {
        Job.Builder builder = createScheduledJob();
        SchedulerState schedulerState = new SchedulerState(JobSchedulerStatus.STOPPED, 0L, null);
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = builder.build();
        Allocation allocation =
                new Allocation(null, "foo", false, JobStatus.OPENED, null, new SchedulerState(JobSchedulerStatus.STOPPED, null, null));

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(job)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(dataProcessor.processData(anyString(), eq(in), any(), any())).thenReturn(dataCounts);
        Consumer<Exception> handler = mockConsumer();
        boolean cancelled = randomBoolean();
        StartJobSchedulerAction.SchedulerTask task = new StartJobSchedulerAction.SchedulerTask(1, "type", "action", null, "foo");
        scheduledJobRunner.run(job, schedulerState, allocation, task, handler);

        verify(dataExtractor).newSearch(eq(0L), eq(60000L), any());
        verify(threadPool, times(1)).executor(PrelertPlugin.SCHEDULER_THREAD_POOL_NAME);
        if (cancelled) {
            task.stop();
            verify(client).execute(same(INSTANCE), eq(new Request("foo", JobSchedulerStatus.STOPPED)), any());
        } else {
            verify(threadPool, times(1)).schedule(eq(new TimeValue(480100)), eq(PrelertPlugin.SCHEDULER_THREAD_POOL_NAME), any());
        }
    }

    public static Job.Builder createScheduledJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(3600L);
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));

        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(Arrays.asList("myIndex"), Arrays.asList("myType"));

        Job.Builder builder = new Job.Builder("foo");
        builder.setAnalysisConfig(acBuilder);
        builder.setSchedulerConfig(schedulerConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.ELASTICSEARCH);
        builder.setDataDescription(dataDescription);
        return builder;
    }

    public void testValidate() {
        Job job1 = buildJobBuilder("foo").build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> ScheduledJobRunner.validate(job1, null));
        assertThat(e.getMessage(), equalTo("job [foo] is not a scheduled job"));

        Job job2 = createScheduledJob().build();
        Allocation allocation1 =
                new Allocation("_id", "_id", false, JobStatus.CLOSED, null, new SchedulerState(JobSchedulerStatus.STOPPED, null, null));
        e = expectThrows(ElasticsearchStatusException.class, () -> ScheduledJobRunner.validate(job2, allocation1));
        assertThat(e.getMessage(), equalTo("cannot start scheduler, expected job status [OPENED], but got [CLOSED]"));

        Job job3 = createScheduledJob().build();
        Allocation allocation2 =
                new Allocation("_id", "_id", false, JobStatus.OPENED, null, new SchedulerState(JobSchedulerStatus.STARTED, null, null));
        e = expectThrows(ElasticsearchStatusException.class, () -> ScheduledJobRunner.validate(job3, allocation2));
        assertThat(e.getMessage(), equalTo("scheduler already started, expected scheduler status [STOPPED], but got [STARTED]"));
    }

    @SuppressWarnings("unchecked")
    private Consumer<Exception> mockConsumer() {
        return mock(Consumer.class);
    }
}