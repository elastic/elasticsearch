/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
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
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction.Request;
import static org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction.INSTANCE;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.eq;

public class ScheduledJobServiceTests extends ESTestCase {

    private Client client;
    private ThreadPool threadPool;
    private JobProvider jobProvider;
    private JobManager jobManager;
    private DataProcessor dataProcessor;
    private DataExtractorFactory dataExtractorFactory;
    private Auditor auditor;
    private ScheduledJobService scheduledJobService;
    private long currentTime = 120000;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        jobProvider = mock(JobProvider.class);
        when(jobProvider.dataCounts(anyString())).thenReturn(new DataCounts("foo"));
        jobManager = mock(JobManager.class);
        dataProcessor = mock(DataProcessor.class);
        dataExtractorFactory = mock(DataExtractorFactory.class);
        auditor = mock(Auditor.class);
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(PrelertPlugin.THREAD_POOL_NAME)).thenReturn(executorService);

        scheduledJobService =
                new ScheduledJobService(threadPool, client, jobProvider, dataProcessor, dataExtractorFactory, () -> currentTime);

        when(jobProvider.audit(anyString())).thenReturn(auditor);
        when(jobProvider.buckets(anyString(), any(BucketsQueryBuilder.BucketsQuery.class))).thenThrow(
                QueryPage.emptyQueryPage(Job.RESULTS_FIELD));
    }

    public void testStart_GivenNewlyCreatedJobLoopBack() throws IOException {
        Job.Builder builder = createScheduledJob();
        Allocation allocation =
                new Allocation("_nodeId", "foo", JobStatus.RUNNING, new SchedulerState(JobSchedulerStatus.STARTING, 0L, 60000L));
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));

        when(jobManager.getJobAllocation("foo")).thenReturn(allocation);

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(builder.build())).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(dataProcessor.processData(anyString(), eq(in), any())).thenReturn(dataCounts);
        scheduledJobService.start(builder.build(), allocation);

        verify(dataExtractor).newSearch(eq(0L), eq(60000L), any());
        verify(threadPool, times(1)).executor(PrelertPlugin.THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client).execute(same(INSTANCE), eq(new Request("foo", JobSchedulerStatus.STARTED)), any());
        verify(client).execute(same(INSTANCE), eq(new Request("foo", JobSchedulerStatus.STOPPING)), any());
    }

    public void testStart_GivenNewlyCreatedJobLoopBackAndRealtime() throws IOException {
        Job.Builder builder = createScheduledJob();
        Allocation allocation =
                new Allocation("_nodeId", "foo", JobStatus.RUNNING, new SchedulerState(JobSchedulerStatus.STARTING, 0L, null));
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        when(jobManager.getJobAllocation("foo")).thenReturn(allocation);

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(builder.build())).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(dataProcessor.processData(anyString(), eq(in), any())).thenReturn(dataCounts);
        scheduledJobService.start(builder.build(), allocation);

        verify(dataExtractor).newSearch(eq(0L), eq(60000L), any());
        verify(threadPool, times(1)).executor(PrelertPlugin.THREAD_POOL_NAME);
        verify(threadPool, times(1)).schedule(eq(new TimeValue(480100)), eq(PrelertPlugin.THREAD_POOL_NAME), any());

        allocation = new Allocation(allocation.getNodeId(), allocation.getJobId(), allocation.getStatus(),
                new SchedulerState(JobSchedulerStatus.STOPPING, 0L, 60000L));
        scheduledJobService.stop(allocation);
        verify(dataProcessor).closeJob("foo");
    }

    public void testStop_GivenNonScheduledJob() {
        Allocation allocation = new Allocation.Builder().build();
        expectThrows(IllegalStateException.class, () -> scheduledJobService.stop(allocation));
    }

    public void testStop_GivenStartedScheduledJob() throws IOException {
        Job.Builder builder = createScheduledJob();
        Allocation allocation1 =
                new Allocation("_nodeId", "foo", JobStatus.RUNNING, new SchedulerState(JobSchedulerStatus.STARTED, 0L, null));
        when(jobManager.getJobAllocation("foo")).thenReturn(allocation1);

        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(builder.build())).thenReturn(dataExtractor);

        Exception e = expectThrows(IllegalStateException.class, () -> scheduledJobService.start(builder.build(), allocation1));
        assertThat(e.getMessage(), equalTo("expected job scheduler status [STARTING], but got [STARTED] instead"));

        e = expectThrows(IllegalStateException.class, () -> scheduledJobService.stop(allocation1));
        assertThat(e.getMessage(), equalTo("expected job scheduler status [STOPPING], but got [STARTED] instead"));

        // Properly stop it to avoid leaking threads in the test
        Allocation allocation2 =
                new Allocation("_nodeId", "foo", JobStatus.RUNNING, new SchedulerState(JobSchedulerStatus.STOPPING, 0L, null));
        scheduledJobService.registry.put("foo", scheduledJobService.createJobScheduler(builder.build()));
        scheduledJobService.stop(allocation2);

        // We stopped twice but the first time should have been ignored. We can assert that indirectly
        // by verifying that the job was closed only once.
        verify(dataProcessor, times(1)).closeJob("foo");
    }

    private static Job.Builder createScheduledJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(3600L);
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));

        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(SchedulerConfig.DataSource.ELASTICSEARCH);
        schedulerConfig.setBaseUrl("http://localhost");
        schedulerConfig.setIndexes(Arrays.asList("myIndex"));
        schedulerConfig.setTypes(Arrays.asList("myType"));

        Job.Builder builder = new Job.Builder("foo");
        builder.setAnalysisConfig(acBuilder);
        builder.setSchedulerConfig(schedulerConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.ELASTICSEARCH);
        builder.setDataDescription(dataDescription);
        return builder;
    }
}