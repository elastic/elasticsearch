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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.job.retention.AbstractExpiredJobDataRemoverTests.TestListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExpiredModelSnapshotsRemoverTests extends ESTestCase {

    private Client client;
    private OriginSettingClient originSettingClient;
    private ThreadPool threadPool;
    private List<SearchRequest> capturedSearchRequests;
    private List<DeleteModelSnapshotAction.Request> capturedDeleteModelSnapshotRequests;
    private List<SearchResponse> searchResponsesPerCall;
    private TestListener listener;

    @Before
    public void setUpTests() {
        capturedSearchRequests = new ArrayList<>();
        capturedDeleteModelSnapshotRequests = new ArrayList<>();
        searchResponsesPerCall = new ArrayList<>();

        client = mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);

        listener = new TestListener();

        // Init thread pool
        Settings settings = Settings.builder()
                .put("node.name", "expired_model_snapshots_remover_test")
                .build();
        threadPool = new ThreadPool(settings,
                new FixedExecutorBuilder(settings, MachineLearning.UTILITY_THREAD_POOL_NAME, 1, 1000, ""));
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testRemove_GivenJobsWithoutRetentionPolicy() throws IOException {
        givenClientRequestsSucceed(Arrays.asList(
                JobTests.buildJobBuilder("foo").build(),
                JobTests.buildJobBuilder("bar").build()
        ));

        createExpiredModelSnapshotsRemover().remove(listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));
        verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testRemove_GivenJobWithoutActiveSnapshot() throws IOException {
        givenClientRequestsSucceed(Collections.singletonList(JobTests.buildJobBuilder("foo").setModelSnapshotRetentionDays(7L).build()));

        createExpiredModelSnapshotsRemover().remove(listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));
        verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testRemove_GivenJobsWithMixedRetentionPolicies() throws IOException {
        givenClientRequestsSucceed(
                Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
                JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        ));

        List<ModelSnapshot> snapshots1JobSnapshots = Arrays.asList(createModelSnapshot("snapshots-1", "snapshots-1_1"),
                createModelSnapshot("snapshots-1", "snapshots-1_2"));
        List<ModelSnapshot> snapshots2JobSnapshots = Collections.singletonList(createModelSnapshot("snapshots-2", "snapshots-2_1"));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots1JobSnapshots));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots2JobSnapshots));

        createExpiredModelSnapshotsRemover().remove(listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));

        assertThat(capturedSearchRequests.size(), equalTo(2));
        SearchRequest searchRequest = capturedSearchRequests.get(0);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("snapshots-1")}));
        searchRequest = capturedSearchRequests.get(1);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("snapshots-2")}));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(3));
        DeleteModelSnapshotAction.Request deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(0);
        assertThat(deleteSnapshotRequest.getJobId(), equalTo("snapshots-1"));
        assertThat(deleteSnapshotRequest.getSnapshotId(), equalTo("snapshots-1_1"));
        deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(1);
        assertThat(deleteSnapshotRequest.getJobId(), equalTo("snapshots-1"));
        assertThat(deleteSnapshotRequest.getSnapshotId(), equalTo("snapshots-1_2"));
        deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(2);
        assertThat(deleteSnapshotRequest.getJobId(), equalTo("snapshots-2"));
        assertThat(deleteSnapshotRequest.getSnapshotId(), equalTo("snapshots-2_1"));
    }

    public void testRemove_GivenTimeout() throws IOException {
        givenClientRequestsSucceed(
                Arrays.asList(
            JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
            JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        ));

        List<ModelSnapshot> snapshots1JobSnapshots = Arrays.asList(createModelSnapshot("snapshots-1", "snapshots-1_1"),
            createModelSnapshot("snapshots-1", "snapshots-1_2"));
        List<ModelSnapshot> snapshots2JobSnapshots = Collections.singletonList(createModelSnapshot("snapshots-2", "snapshots-2_1"));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots1JobSnapshots));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots2JobSnapshots));

        final int timeoutAfter = randomIntBetween(0, 1);
        AtomicInteger attemptsLeft = new AtomicInteger(timeoutAfter);

        createExpiredModelSnapshotsRemover().remove(listener, () -> (attemptsLeft.getAndDecrement() <= 0));

        listener.waitToCompletion();
        assertThat(listener.success, is(false));
    }

    public void testRemove_GivenClientSearchRequestsFail() throws IOException {
        givenClientSearchRequestsFail(
                Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
                JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        ));

        List<ModelSnapshot> snapshots1JobSnapshots = Arrays.asList(createModelSnapshot("snapshots-1", "snapshots-1_1"),
                createModelSnapshot("snapshots-1", "snapshots-1_2"));
        List<ModelSnapshot> snapshots2JobSnapshots = Collections.singletonList(createModelSnapshot("snapshots-2", "snapshots-2_1"));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots1JobSnapshots));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots2JobSnapshots));

        createExpiredModelSnapshotsRemover().remove(listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(false));

        assertThat(capturedSearchRequests.size(), equalTo(1));
        SearchRequest searchRequest = capturedSearchRequests.get(0);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("snapshots-1")}));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(0));
    }

    public void testRemove_GivenClientDeleteSnapshotRequestsFail() throws IOException {
        givenClientDeleteModelSnapshotRequestsFail(
                Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
                JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        ));

        List<ModelSnapshot> snapshots1JobSnapshots = Arrays.asList(createModelSnapshot("snapshots-1", "snapshots-1_1"),
                createModelSnapshot("snapshots-1", "snapshots-1_2"));
        List<ModelSnapshot> snapshots2JobSnapshots = Collections.singletonList(createModelSnapshot("snapshots-2", "snapshots-2_1"));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots1JobSnapshots));
        searchResponsesPerCall.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots2JobSnapshots));

        createExpiredModelSnapshotsRemover().remove(listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(false));

        assertThat(capturedSearchRequests.size(), equalTo(1));
        SearchRequest searchRequest = capturedSearchRequests.get(0);
        assertThat(searchRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("snapshots-1")}));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(1));
        DeleteModelSnapshotAction.Request deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(0);
        assertThat(deleteSnapshotRequest.getJobId(), equalTo("snapshots-1"));
        assertThat(deleteSnapshotRequest.getSnapshotId(), equalTo("snapshots-1_1"));
    }

    private ExpiredModelSnapshotsRemover createExpiredModelSnapshotsRemover() {
        return new ExpiredModelSnapshotsRemover(originSettingClient, threadPool);
    }

    private static ModelSnapshot createModelSnapshot(String jobId, String snapshotId) {
        return new ModelSnapshot.Builder(jobId).setSnapshotId(snapshotId).build();
    }

    private void givenClientRequestsSucceed(List<Job> jobs) throws IOException {
        givenClientRequests(jobs, true, true);
    }

    private void givenClientSearchRequestsFail(List<Job> jobs) throws IOException {
        givenClientRequests(jobs, false, true);
    }

    private void givenClientDeleteModelSnapshotRequestsFail(List<Job> jobs) throws IOException {
        givenClientRequests(jobs, true, false);
    }

    @SuppressWarnings("unchecked")
    private void givenClientRequests(List<Job> jobs,
                                     boolean shouldSearchRequestsSucceed, boolean shouldDeleteSnapshotRequestsSucceed) throws IOException {
        SearchResponse response = AbstractExpiredJobDataRemoverTests.createSearchResponse(jobs);

        doAnswer(new Answer<Void>() {
            int callCount = 0;
            AtomicBoolean isJobQuery = new AtomicBoolean(true);

            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];

                if (isJobQuery.get()) {
                    listener.onResponse(response);
                    isJobQuery.set(false);
                    return null;
                }

                SearchRequest searchRequest = (SearchRequest) invocationOnMock.getArguments()[1];
                capturedSearchRequests.add(searchRequest);
                if (shouldSearchRequestsSucceed) {
                    listener.onResponse(searchResponsesPerCall.get(callCount++));
                } else {
                    listener.onFailure(new RuntimeException("search failed"));
                }
                return null;
            }
        }).when(client).execute(same(SearchAction.INSTANCE), any(), any());

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
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
        }).when(client).execute(same(DeleteModelSnapshotAction.INSTANCE), any(), any());
    }

}
