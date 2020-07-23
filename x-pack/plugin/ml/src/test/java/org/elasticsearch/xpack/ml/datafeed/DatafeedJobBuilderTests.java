/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedJobBuilderTests extends ESTestCase {

    private Client client;
    private AnomalyDetectionAuditor auditor;
    private AnnotationPersister annotationPersister;
    private Consumer<Exception> taskHandler;
    private JobResultsProvider jobResultsProvider;
    private JobConfigProvider jobConfigProvider;
    private DatafeedConfigProvider datafeedConfigProvider;
    private JobResultsPersister jobResultsPersister;

    private DatafeedJobBuilder datafeedJobBuilder;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.settings()).thenReturn(Settings.EMPTY);
        auditor = mock(AnomalyDetectionAuditor.class);
        annotationPersister = mock(AnnotationPersister.class);
        taskHandler = mock(Consumer.class);
        jobResultsPersister = mock(JobResultsPersister.class);

        jobResultsProvider = mock(JobResultsProvider.class);
        Mockito.doAnswer(invocationOnMock -> {
            String jobId = (String) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            handler.accept(new DataCounts(jobId));
            return null;
        }).when(jobResultsProvider).dataCounts(any(), any(), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<ResourceNotFoundException> consumer = (Consumer<ResourceNotFoundException>) invocationOnMock.getArguments()[3];
            consumer.accept(new ResourceNotFoundException("dummy"));
            return null;
        }).when(jobResultsProvider).bucketsViaInternalClient(any(), any(), any(), any());

        jobConfigProvider = mock(JobConfigProvider.class);
        datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        datafeedJobBuilder =
            new DatafeedJobBuilder(
                client,
                xContentRegistry(),
                auditor,
                annotationPersister,
                System::currentTimeMillis,
                jobConfigProvider,
                jobResultsProvider,
                datafeedConfigProvider,
                jobResultsPersister,
                Settings.EMPTY,
                "test_node");
    }

    public void testBuild_GivenScrollDatafeedAndNewJob() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedManagerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedManagerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionListener.wrap(
                datafeedJob -> {
                    assertThat(datafeedJob.isRunning(), is(true));
                    assertThat(datafeedJob.isIsolated(), is(false));
                    assertThat(datafeedJob.lastEndTimeMs(), is(nullValue()));
                    wasHandlerCalled.compareAndSet(false, true);
                }, e -> fail()
        );

        givenJob(jobBuilder);
        givenDatafeed(datafeed);

        datafeedJobBuilder.build("datafeed1", new TaskId(""), datafeedJobHandler);

        assertBusy(() -> wasHandlerCalled.get());
    }

    public void testBuild_GivenScrollDatafeedAndOldJobWithLatestRecordTimestampAfterLatestBucket() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedManagerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedManagerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());

        givenLatestTimes(7_200_000L, 3_600_000L);

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionListener.wrap(
                datafeedJob -> {
                    assertThat(datafeedJob.isRunning(), is(true));
                    assertThat(datafeedJob.isIsolated(), is(false));
                    assertThat(datafeedJob.lastEndTimeMs(), equalTo(7_200_000L));
                    wasHandlerCalled.compareAndSet(false, true);
                }, e -> fail()
        );

        givenJob(jobBuilder);
        givenDatafeed(datafeed);

        datafeedJobBuilder.build("datafeed1", new TaskId(""), datafeedJobHandler);

        assertBusy(() -> wasHandlerCalled.get());
    }

    public void testBuild_GivenScrollDatafeedAndOldJobWithLatestBucketAfterLatestRecordTimestamp() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedManagerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedManagerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());

        givenLatestTimes(3_800_000L, 3_600_000L);

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionListener.wrap(
                datafeedJob -> {
                    assertThat(datafeedJob.isRunning(), is(true));
                    assertThat(datafeedJob.isIsolated(), is(false));
                    assertThat(datafeedJob.lastEndTimeMs(), equalTo(7_199_999L));
                    wasHandlerCalled.compareAndSet(false, true);
                }, e -> fail()
        );

        givenJob(jobBuilder);
        givenDatafeed(datafeed);

        datafeedJobBuilder.build("datafeed1", new TaskId(""), datafeedJobHandler);

        assertBusy(() -> wasHandlerCalled.get());
    }

    public void testBuild_GivenBucketsRequestFails() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedManagerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedManagerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());

        Exception error = new RuntimeException("error");
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<Exception> consumer = (Consumer<Exception>) invocationOnMock.getArguments()[3];
            consumer.accept(error);
            return null;
        }).when(jobResultsProvider).bucketsViaInternalClient(any(), any(), any(), any());


        givenJob(jobBuilder);
        givenDatafeed(datafeed);

        datafeedJobBuilder.build("datafeed1", new TaskId(""), ActionListener.wrap(datafeedJob -> fail(), taskHandler));

        verify(taskHandler).accept(error);
    }

    public void testBuildGivenRemoteIndicesButNoRemoteSearching() throws Exception {
        datafeedJobBuilder =
            new DatafeedJobBuilder(
                client,
                xContentRegistry(),
                auditor,
                annotationPersister,
                System::currentTimeMillis,
                jobConfigProvider,
                jobResultsProvider,
                datafeedConfigProvider,
                jobResultsPersister,
                nonRemoteClusterClientNode(),
                "test_node");
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedManagerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedManagerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());
        datafeed.setIndices(Collections.singletonList("remotecluster:index-*"));

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionListener.wrap(
            datafeedJob -> fail("datafeed builder did not fail when remote index was given and remote clusters were not enabled"),
            e -> {
                assertThat(e.getMessage(), equalTo(Messages.getMessage(Messages.DATAFEED_NEEDS_REMOTE_CLUSTER_SEARCH,
                    "datafeed1",
                    "[remotecluster:index-*]",
                    "test_node")));
                wasHandlerCalled.compareAndSet(false, true);
            }
        );

        givenJob(jobBuilder);
        givenDatafeed(datafeed);
        datafeedJobBuilder.build("datafeed1", new TaskId(""), datafeedJobHandler);
        assertBusy(() -> wasHandlerCalled.get());
    }

    private void givenJob(Job.Builder job) {
        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job.Builder> handler = (ActionListener<Job.Builder>) invocationOnMock.getArguments()[1];
            handler.onResponse(job);
            return null;
        }).when(jobConfigProvider).getJob(eq(job.getId()), any());
    }

    private void givenDatafeed(DatafeedConfig.Builder datafeed) {
        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<DatafeedConfig.Builder> handler = (ActionListener<DatafeedConfig.Builder>) invocationOnMock.getArguments()[1];
            handler.onResponse(datafeed);
            return null;
        }).when(datafeedConfigProvider).getDatafeedConfig(eq(datafeed.getId()), any());
    }

    private void givenLatestTimes(long latestRecordTimestamp, long latestBucketTimestamp) {
        Mockito.doAnswer(invocationOnMock -> {
            String jobId = (String) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            DataCounts dataCounts = new DataCounts(jobId);
            dataCounts.setLatestRecordTimeStamp(new Date(latestRecordTimestamp));
            handler.accept(dataCounts);
            return null;
        }).when(jobResultsProvider).dataCounts(any(), any(), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<QueryPage<Bucket>> consumer = (Consumer<QueryPage<Bucket>>) invocationOnMock.getArguments()[2];
            Bucket bucket = mock(Bucket.class);
            when(bucket.getTimestamp()).thenReturn(new Date(latestBucketTimestamp));
            QueryPage<Bucket> bucketQueryPage = new QueryPage<Bucket>(Collections.singletonList(bucket), 1, Bucket.RESULTS_FIELD);
            consumer.accept(bucketQueryPage);
            return null;
        }).when(jobResultsProvider).bucketsViaInternalClient(any(), any(), any(), any());
    }
}
