/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.job.retention.AbstractExpiredJobDataRemoverTests.TestListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpiredModelSnapshotsRemoverTests extends ESTestCase {

    private Client client;
    private OriginSettingClient originSettingClient;
    private List<SearchRequest> capturedSearchRequests;
    private List<DeleteModelSnapshotAction.Request> capturedDeleteModelSnapshotRequests;
    private TestListener listener;

    @Before
    public void setUpTests() {
        capturedSearchRequests = new ArrayList<>();
        capturedDeleteModelSnapshotRequests = new ArrayList<>();

        client = mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);

        listener = new TestListener();
    }

    public void testRemove_GivenJobWithoutActiveSnapshot() throws IOException {
        List<SearchResponse> responses = Arrays.asList(
                AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(JobTests.buildJobBuilder("foo")
                        .setModelSnapshotRetentionDays(7L).build())),
                AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.emptyList()));

        givenClientRequestsSucceed(responses);

        createExpiredModelSnapshotsRemover().remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));
        verify(client, times(2)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testRemove_GivenJobsWithMixedRetentionPolicies() throws IOException {
        List<SearchResponse> searchResponses = new ArrayList<>();
        searchResponses.add(
                AbstractExpiredJobDataRemoverTests.createSearchResponse(Arrays.asList(
                        JobTests.buildJobBuilder("job-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
                        JobTests.buildJobBuilder("job-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        )));

        Date now = new Date();
        Date oneDayAgo = new Date(now.getTime() - TimeValue.timeValueDays(1).getMillis());
        ModelSnapshot snapshot1_1 = createModelSnapshot("job-1", "fresh-snapshot", oneDayAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(snapshot1_1)));

        // It needs to be strictly more than 7 days before the most recent snapshot, hence the extra millisecond
        Date eightDaysAndOneMsAgo = new Date(now.getTime() - TimeValue.timeValueDays(8).getMillis() - 1);
        ModelSnapshot snapshotToBeDeleted = createModelSnapshot("job-1", "old-snapshot", eightDaysAndOneMsAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(snapshotToBeDeleted)));

        ModelSnapshot snapshot2_1 = createModelSnapshot("job-1", "snapshots-1_1", eightDaysAndOneMsAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(snapshot2_1)));
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.emptyList()));

        givenClientRequestsSucceed(searchResponses);
        createExpiredModelSnapshotsRemover().remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));

        assertThat(capturedSearchRequests.size(), equalTo(5));
        SearchRequest searchRequest = capturedSearchRequests.get(1);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("job-1")}));
        searchRequest = capturedSearchRequests.get(3);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("job-2")}));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(1));
        DeleteModelSnapshotAction.Request deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(0);
        assertThat(deleteSnapshotRequest.getJobId(), equalTo("job-1"));
        assertThat(deleteSnapshotRequest.getSnapshotId(), equalTo("old-snapshot"));
    }

    public void testRemove_GivenTimeout() throws IOException {
        List<SearchResponse> searchResponses = new ArrayList<>();
        searchResponses.add(
                AbstractExpiredJobDataRemoverTests.createSearchResponse(Arrays.asList(
            JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
            JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        )));

        Date now = new Date();
        List<ModelSnapshot> snapshots1JobSnapshots = Arrays.asList(createModelSnapshot("snapshots-1", "snapshots-1_1", now),
            createModelSnapshot("snapshots-1", "snapshots-1_2", now));
        List<ModelSnapshot> snapshots2JobSnapshots = Collections.singletonList(createModelSnapshot("snapshots-2", "snapshots-2_1", now));
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots1JobSnapshots));
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots2JobSnapshots));

        givenClientRequestsSucceed(searchResponses);

        final int timeoutAfter = randomIntBetween(0, 1);
        AtomicInteger attemptsLeft = new AtomicInteger(timeoutAfter);

        createExpiredModelSnapshotsRemover().remove(1.0f, listener, () -> (attemptsLeft.getAndDecrement() <= 0));

        listener.waitToCompletion();
        assertThat(listener.success, is(false));
    }

    public void testRemove_GivenClientSearchRequestsFail() throws IOException {
        List<SearchResponse> searchResponses = new ArrayList<>();
        searchResponses.add(
                AbstractExpiredJobDataRemoverTests.createSearchResponse(Arrays.asList(
                JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
                JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        )));

        givenClientSearchRequestsFail(searchResponses);
        createExpiredModelSnapshotsRemover().remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(false));

        assertThat(capturedSearchRequests.size(), equalTo(2));
        SearchRequest searchRequest = capturedSearchRequests.get(1);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("snapshots-1")}));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(0));
    }

    public void testRemove_GivenClientDeleteSnapshotRequestsFail() throws IOException {
        List<SearchResponse> searchResponses = new ArrayList<>();
        searchResponses.add(
                AbstractExpiredJobDataRemoverTests.createSearchResponse(Arrays.asList(
                JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
                JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        )));

        Date now = new Date();
        Date oneDayAgo = new Date(new Date().getTime() - TimeValue.timeValueDays(1).getMillis());
        ModelSnapshot snapshot1_1 = createModelSnapshot("snapshots-1", "snapshots-1_1", oneDayAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(snapshot1_1)));

        // It needs to be strictly more than 7 days before the most recent snapshot, hence the extra millisecond
        Date eightDaysAndOneMsAgo = new Date(now.getTime() - TimeValue.timeValueDays(8).getMillis() - 1);
        List<ModelSnapshot> snapshots1JobSnapshots = Arrays.asList(
                snapshot1_1,
                createModelSnapshot("snapshots-1", "snapshots-1_2", eightDaysAndOneMsAgo));
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots1JobSnapshots));

        ModelSnapshot snapshot2_2 = createModelSnapshot("snapshots-2", "snapshots-2_1", eightDaysAndOneMsAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(snapshot2_2)));

        givenClientDeleteModelSnapshotRequestsFail(searchResponses);
        createExpiredModelSnapshotsRemover().remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(false));

        assertThat(capturedSearchRequests.size(), equalTo(3));
        SearchRequest searchRequest = capturedSearchRequests.get(1);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("snapshots-1")}));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(1));
        DeleteModelSnapshotAction.Request deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(0);
        assertThat(deleteSnapshotRequest.getJobId(), equalTo("snapshots-1"));
        assertThat(deleteSnapshotRequest.getSnapshotId(), equalTo("snapshots-1_2"));
    }

    @SuppressWarnings("unchecked")
    public void testCalcCutoffEpochMs() throws IOException {
        List<SearchResponse> searchResponses = new ArrayList<>();

        Date oneDayAgo = new Date(new Date().getTime() - TimeValue.timeValueDays(1).getMillis());
        ModelSnapshot snapshot1_1 = createModelSnapshot("job-1", "newest-snapshot", oneDayAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.singletonList(snapshot1_1)));

        givenClientRequests(searchResponses, true, true);

        long retentionDays = 3L;
        ActionListener<AbstractExpiredJobDataRemover.CutoffDetails> cutoffListener = mock(ActionListener.class);
        createExpiredModelSnapshotsRemover().calcCutoffEpochMs("job-1", retentionDays, cutoffListener);

        long dayInMills = 60 * 60 * 24 * 1000;
        long expectedCutoffTime = oneDayAgo.getTime() - (dayInMills * retentionDays);
        verify(cutoffListener).onResponse(eq(new AbstractExpiredJobDataRemover.CutoffDetails(oneDayAgo.getTime(), expectedCutoffTime)));
    }

    private ExpiredModelSnapshotsRemover createExpiredModelSnapshotsRemover() {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executor = mock(ExecutorService.class);

        when(threadPool.executor(eq(MachineLearning.UTILITY_THREAD_POOL_NAME))).thenReturn(executor);

        doAnswer(invocationOnMock -> {
                    Runnable run = (Runnable) invocationOnMock.getArguments()[0];
                    run.run();
                    return null;
                }
        ).when(executor).execute(any());
        return new ExpiredModelSnapshotsRemover(originSettingClient, threadPool);
    }

    private static ModelSnapshot createModelSnapshot(String jobId, String snapshotId, Date date) {
        return new ModelSnapshot.Builder(jobId).setSnapshotId(snapshotId).setTimestamp(date).build();
    }

    private void givenClientRequestsSucceed(List<SearchResponse> searchResponses)  {
        givenClientRequests(searchResponses, true, true);
    }

    private void givenClientSearchRequestsFail(List<SearchResponse> searchResponses) {
        givenClientRequests(searchResponses, false, true);
    }

    private void givenClientDeleteModelSnapshotRequestsFail(List<SearchResponse> searchResponses) {
        givenClientRequests(searchResponses, true, false);
    }

    @SuppressWarnings("unchecked")
    private void givenClientRequests(List<SearchResponse> searchResponses,
                                     boolean shouldSearchRequestsSucceed, boolean shouldDeleteSnapshotRequestsSucceed) {

        doAnswer(new Answer<Void>() {
            AtomicInteger callCount = new AtomicInteger();

            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];

                SearchRequest searchRequest = (SearchRequest) invocationOnMock.getArguments()[1];
                capturedSearchRequests.add(searchRequest);
                // Only the last search request should fail
                if (shouldSearchRequestsSucceed || callCount.get() < searchResponses.size()) {
                    SearchResponse response = searchResponses.get(callCount.getAndIncrement());
                    listener.onResponse(response);
                } else {
                    listener.onFailure(new RuntimeException("search failed"));
                }
                return null;
            }
        }).when(client).execute(same(SearchAction.INSTANCE), any(), any());

        doAnswer(invocationOnMock -> {
                capturedDeleteModelSnapshotRequests.add((DeleteModelSnapshotAction.Request) invocationOnMock.getArguments()[1]);
                ActionListener<AcknowledgedResponse> listener =
                        (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[2];
                if (shouldDeleteSnapshotRequestsSucceed) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new RuntimeException("delete snapshot failed"));
                }
                return null;
            }
        ).when(client).execute(same(DeleteModelSnapshotAction.INSTANCE), any(), any());
    }
}
