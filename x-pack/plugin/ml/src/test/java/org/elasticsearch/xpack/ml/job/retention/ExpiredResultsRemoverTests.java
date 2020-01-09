/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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

public class ExpiredResultsRemoverTests extends ESTestCase {

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

    public void testRemove_GivenNoJobs() throws IOException {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client, Collections.emptyList());

        createExpiredResultsRemover().remove(listener, () -> false);

        verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
        verify(listener).onResponse(true);
    }

    public void testRemove_GivenJobsWithoutRetentionPolicy() throws IOException {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
                JobTests.buildJobBuilder("foo").build(),
                JobTests.buildJobBuilder("bar").build()
        ));

        createExpiredResultsRemover().remove(listener, () -> false);

        verify(listener).onResponse(true);
        verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testRemove_GivenJobsWithAndWithoutRetentionPolicy() throws Exception {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
                JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        givenBucket(new Bucket("id_not_important", new Date(), 60));

        createExpiredResultsRemover().remove(listener, () -> false);

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(2));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("results-1")}));
        dbqRequest = capturedDeleteByQueryRequests.get(1);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("results-2")}));
        verify(listener).onResponse(true);
    }

    public void testRemove_GivenTimeout() throws Exception {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
            JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
            JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        givenBucket(new Bucket("id_not_important", new Date(), 60));

        final int timeoutAfter = randomIntBetween(0, 1);
        AtomicInteger attemptsLeft = new AtomicInteger(timeoutAfter);

        createExpiredResultsRemover().remove(listener, () -> (attemptsLeft.getAndDecrement() <= 0));

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(timeoutAfter));
        verify(listener).onResponse(false);
    }

    public void testRemove_GivenClientRequestsFailed() throws IOException {
        givenClientRequestsFailed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
                JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        givenBucket(new Bucket("id_not_important", new Date(), 60));

        createExpiredResultsRemover().remove(listener, () -> false);

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(1));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("results-1")}));
        verify(listener).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testCalcCutoffEpochMs() throws IOException {
        String jobId = "calc-cutoff";
        givenJobs(Collections.singletonList(JobTests.buildJobBuilder(jobId).setResultsRetentionDays(1L).build()));

        Date latest = new Date();
        givenBucket(new Bucket(jobId, latest, 60));

        ActionListener<Long> cutoffListener = mock(ActionListener.class);
        createExpiredResultsRemover().calcCutoffEpochMs(jobId, 1L, cutoffListener);

        long dayInMills = 60 * 60 * 24 * 1000;
        long expectedCutoffTime = latest.getTime() - dayInMills;
        verify(cutoffListener).onResponse(eq(expectedCutoffTime));
    }

    private void givenClientRequestsSucceed() {
        givenClientRequests(true);
    }

    private void givenClientRequestsFailed() {
        givenClientRequests(false);
    }

    @SuppressWarnings("unchecked")
    private void givenClientRequests(boolean shouldSucceed) {
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
    private void givenJobs(List<Job> jobs) throws IOException {
        SearchResponse response = AbstractExpiredJobDataRemoverTests.createSearchResponse(jobs);

        ActionFuture<SearchResponse> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(response);
        when(client.search(any())).thenReturn(future);
    }

    @SuppressWarnings("unchecked")
    private void givenBucket(Bucket bucket) throws IOException {
        SearchResponse searchResponse = AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(bucket));

        doAnswer(invocationOnMock -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));
    }

    private ExpiredResultsRemover createExpiredResultsRemover() {
        final String executorName = "expired-remover-test";
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executor = mock(ExecutorService.class);

        when(threadPool.executor(eq(executorName))).thenReturn(executor);

        doAnswer(invocationOnMock -> {
                Runnable run = (Runnable) invocationOnMock.getArguments()[0];
                run.run();
                return null;
            }
        ).when(executor).execute(any());

        return new ExpiredResultsRemover(originSettingClient, mock(AnomalyDetectionAuditor.class), threadPool, executorName);
    }
}
