/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpiredAnnotationsRemoverTests extends ESTestCase {

    private Client client;
    private OriginSettingClient originSettingClient;
    private List<DeleteByQueryRequest> capturedDeleteByQueryRequests;
    private ActionListener<Boolean> listener;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        capturedDeleteByQueryRequests = new ArrayList<>();

        client = mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        listener = mock(ActionListener.class);
    }

    public void testRemove_GivenNoJobs() {
        givenDBQRequestsSucceed();

        createExpiredAnnotationsRemover(Collections.emptyIterator()).remove(1.0f, listener, () -> false);

        verify(listener).onResponse(true);
    }

    public void testRemove_GivenJobsWithoutRetentionPolicy() {
        givenDBQRequestsSucceed();
        List<Job> jobs = Arrays.asList(
                JobTests.buildJobBuilder("foo").build(),
                JobTests.buildJobBuilder("bar").build()
        );

        createExpiredAnnotationsRemover(jobs.iterator()).remove(1.0f, listener, () -> false);

        verify(listener).onResponse(true);
    }

    public void testRemove_GivenJobsWithAndWithoutRetentionPolicy() {
        givenDBQRequestsSucceed();
        givenBucket(new Bucket("id_not_important", new Date(), 60));

        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("none").build(),
            JobTests.buildJobBuilder("annotations-1").setAnnotationsRetentionDays(10L).build(),
            JobTests.buildJobBuilder("annotations-2").setAnnotationsRetentionDays(20L).build());

        createExpiredAnnotationsRemover(jobs.iterator()).remove(1.0f, listener, () -> false);

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(2));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnnotationIndex.READ_ALIAS_NAME}));
        dbqRequest = capturedDeleteByQueryRequests.get(1);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnnotationIndex.READ_ALIAS_NAME}));
        verify(listener).onResponse(true);
    }

    public void testRemove_GivenTimeout() {
        givenDBQRequestsSucceed();
        givenBucket(new Bucket("id_not_important", new Date(), 60));

        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("annotations-1").setAnnotationsRetentionDays(10L).build(),
            JobTests.buildJobBuilder("annotations-2").setAnnotationsRetentionDays(20L).build()
        );

        final int timeoutAfter = randomIntBetween(0, 1);
        AtomicInteger attemptsLeft = new AtomicInteger(timeoutAfter);

        createExpiredAnnotationsRemover(jobs.iterator()).remove(1.0f, listener, () -> (attemptsLeft.getAndDecrement() <= 0));

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(timeoutAfter));
        verify(listener).onResponse(false);
    }

    public void testRemove_GivenClientRequestsFailed() {
        givenDBQRequestsFailed();
        givenBucket(new Bucket("id_not_important", new Date(), 60));

        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("none").build(),
            JobTests.buildJobBuilder("annotations-1").setAnnotationsRetentionDays(10L).build(),
            JobTests.buildJobBuilder("annotations-2").setAnnotationsRetentionDays(20L).build());
        createExpiredAnnotationsRemover(jobs.iterator()).remove(1.0f, listener, () -> false);

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(1));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnnotationIndex.READ_ALIAS_NAME}));
        verify(listener).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testCalcCutoffEpochMs() {
        String jobId = "calc-cutoff";
        Date latest = new Date();

        givenBucket(new Bucket(jobId, latest, 60));
        List<Job> jobs = Collections.singletonList(JobTests.buildJobBuilder(jobId).setAnnotationsRetentionDays(1L).build());

        ActionListener<AbstractExpiredJobDataRemover.CutoffDetails> cutoffListener = mock(ActionListener.class);
        createExpiredAnnotationsRemover(jobs.iterator()).calcCutoffEpochMs(jobId, 1L, cutoffListener);

        long dayInMills = 60 * 60 * 24 * 1000;
        long expectedCutoffTime = latest.getTime() - dayInMills;
        verify(cutoffListener).onResponse(eq(new AbstractExpiredJobDataRemover.CutoffDetails(latest.getTime(), expectedCutoffTime)));
    }

    private void givenDBQRequestsSucceed() {
        givenDBQRequest(true);
    }

    private void givenDBQRequestsFailed() {
        givenDBQRequest(false);
    }

    @SuppressWarnings("unchecked")
    private void givenDBQRequest(boolean shouldSucceed) {
        doAnswer(invocationOnMock -> {
                capturedDeleteByQueryRequests.add((DeleteByQueryRequest) invocationOnMock.getArguments()[1]);
                ActionListener<BulkByScrollResponse> listener =
                        (ActionListener<BulkByScrollResponse>) invocationOnMock.getArguments()[2];
                if (shouldSucceed) {
                    BulkByScrollResponse bulkByScrollResponse = mock(BulkByScrollResponse.class);
                    when(bulkByScrollResponse.getDeleted()).thenReturn(42L);
                    listener.onResponse(bulkByScrollResponse);
                } else {
                    listener.onFailure(new RuntimeException("failed"));
                }
                return null;
            }
        ).when(client).execute(same(DeleteByQueryAction.INSTANCE), any(), any());
    }

    @SuppressWarnings("unchecked")
    private void givenBucket(Bucket bucket) {
        doAnswer(invocationOnMock -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(bucket)));
            return null;
        }).when(client).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    private ExpiredAnnotationsRemover createExpiredAnnotationsRemover(Iterator<Job> jobIterator) {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executor = mock(ExecutorService.class);

        when(threadPool.executor(eq(MachineLearning.UTILITY_THREAD_POOL_NAME))).thenReturn(executor);

        doAnswer(invocationOnMock -> {
                Runnable run = (Runnable) invocationOnMock.getArguments()[0];
                run.run();
                return null;
            }
        ).when(executor).execute(any());

        return new ExpiredAnnotationsRemover(
            originSettingClient, jobIterator, new TaskId("test", 0L), mock(AnomalyDetectionAuditor.class), threadPool);
    }
}
