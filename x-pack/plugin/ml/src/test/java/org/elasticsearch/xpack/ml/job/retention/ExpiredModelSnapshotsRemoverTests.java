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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.job.retention.AbstractExpiredJobDataRemoverTests.TestListener;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpiredModelSnapshotsRemoverTests extends ESTestCase {

    private Client client;
    private JobResultsProvider resultsProvider;
    private OriginSettingClient originSettingClient;
    private List<String> capturedJobIds;
    private List<DeleteByQueryRequest> capturedDeleteModelSnapshotRequests;
    private TestListener listener;

    @Before
    public void setUpTests() {
        capturedJobIds = new ArrayList<>();
        capturedDeleteModelSnapshotRequests = new ArrayList<>();

        client = mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        resultsProvider = mock(JobResultsProvider.class);

        listener = new TestListener();
    }

    public void testRemove_GivenJobWithoutActiveSnapshot() throws IOException {
        List<Job> jobs = Collections.singletonList(JobTests.buildJobBuilder("foo").setModelSnapshotRetentionDays(7L).build());

        List<SearchResponse> responses = Collections.singletonList(
            AbstractExpiredJobDataRemoverTests.createSearchResponse(Collections.emptyList())
        );
        givenClientRequestsSucceed(responses, Collections.emptyMap());

        createExpiredModelSnapshotsRemover(jobs.iterator()).remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));
        verify(client, times(1)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testRemove_GivenJobsWithMixedRetentionPolicies() {
        List<SearchResponse> searchResponses = new ArrayList<>();
        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("job-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
            JobTests.buildJobBuilder("job-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        );

        Date now = new Date();
        Date oneDayAgo = new Date(now.getTime() - TimeValue.timeValueDays(1).getMillis());
        SearchHit snapshot1_1 = createModelSnapshotQueryHit("job-1", "fresh-snapshot", oneDayAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponseFromHits(Collections.singletonList(snapshot1_1)));
        SearchHit snapshot2_1 = createModelSnapshotQueryHit("job-2", "fresh-snapshot", oneDayAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponseFromHits(Collections.singletonList(snapshot2_1)));

        // It needs to be strictly more than 7 days before the most recent snapshot, hence the extra millisecond
        Date eightDaysAndOneMsAgo = new Date(now.getTime() - TimeValue.timeValueDays(8).getMillis() - 1);
        Map<String, List<ModelSnapshot>> snapshotResponses = new HashMap<>();
        snapshotResponses.put(
            "job-1",
            Arrays.asList(
                // Keeping active as its expiration is not known. We can assume "worst case" and verify it is not removed
                createModelSnapshot("job-1", "active", eightDaysAndOneMsAgo),
                createModelSnapshot("job-1", "old-snapshot", eightDaysAndOneMsAgo)
            )
        );
        // Retention days for job-2 is 17 days, consequently, its query should return anything as we don't ask for snapshots
        // created AFTER 17 days ago
        snapshotResponses.put("job-2", Collections.emptyList());
        givenClientRequestsSucceed(searchResponses, snapshotResponses);
        createExpiredModelSnapshotsRemover(jobs.iterator()).remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));

        assertThat(capturedJobIds.size(), equalTo(2));
        assertThat(capturedJobIds.get(0), equalTo("job-1"));
        assertThat(capturedJobIds.get(1), equalTo("job-2"));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(1));
        DeleteByQueryRequest deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(0);
        assertThat(
            deleteSnapshotRequest.indices(),
            arrayContainingInAnyOrder(
                AnomalyDetectorsIndex.jobResultsAliasedName("job-1"),
                AnomalyDetectorsIndex.jobStateIndexPattern(),
                AnnotationIndex.READ_ALIAS_NAME
            )
        );
        assertThat(deleteSnapshotRequest.getSearchRequest().source().query() instanceof IdsQueryBuilder, is(true));
        IdsQueryBuilder idsQueryBuilder = (IdsQueryBuilder) deleteSnapshotRequest.getSearchRequest().source().query();
        assertTrue(
            "expected ids related to [old-snapshot] but received [" + idsQueryBuilder.ids() + "]",
            idsQueryBuilder.ids().stream().allMatch(s -> s.contains("old-snapshot"))
        );
    }

    public void testRemove_GivenTimeout() throws IOException {
        List<SearchResponse> searchResponses = new ArrayList<>();
        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
            JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        );

        Date now = new Date();
        List<ModelSnapshot> snapshots1JobSnapshots = Arrays.asList(
            createModelSnapshot("snapshots-1", "snapshots-1_1", now),
            createModelSnapshot("snapshots-1", "snapshots-1_2", now)
        );
        List<ModelSnapshot> snapshots2JobSnapshots = Collections.singletonList(createModelSnapshot("snapshots-2", "snapshots-2_1", now));
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots1JobSnapshots));
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponse(snapshots2JobSnapshots));
        HashMap<String, List<ModelSnapshot>> snapshots = new HashMap<>();

        // ALl snapshots are "now" and the retention days is much longer than that.
        // So, getting the snapshots should return empty for both
        snapshots.put("snapshots-1", Collections.emptyList());
        snapshots.put("snapshots-2", Collections.emptyList());
        givenClientRequestsSucceed(searchResponses, snapshots);

        final int timeoutAfter = randomIntBetween(0, 1);
        AtomicInteger attemptsLeft = new AtomicInteger(timeoutAfter);

        createExpiredModelSnapshotsRemover(jobs.iterator()).remove(1.0f, listener, () -> (attemptsLeft.getAndDecrement() <= 0));

        listener.waitToCompletion();
        assertThat(listener.success, is(false));
    }

    public void testRemove_GivenClientSearchRequestsFail() {
        List<SearchResponse> searchResponses = new ArrayList<>();
        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
            JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        );

        givenClientSearchRequestsFail(searchResponses, Collections.emptyMap());
        createExpiredModelSnapshotsRemover(jobs.iterator()).remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(false));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(0));
    }

    public void testRemove_GivenClientDeleteSnapshotRequestsFail() {
        List<SearchResponse> searchResponses = new ArrayList<>();
        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("snapshots-1").setModelSnapshotRetentionDays(7L).setModelSnapshotId("active").build(),
            JobTests.buildJobBuilder("snapshots-2").setModelSnapshotRetentionDays(17L).setModelSnapshotId("active").build()
        );

        Date now = new Date();
        Date oneDayAgo = new Date(new Date().getTime() - TimeValue.timeValueDays(1).getMillis());
        Date eightDaysAndOneMsAgo = new Date(now.getTime() - TimeValue.timeValueDays(8).getMillis() - 1);
        SearchHit snapshot1_1 = createModelSnapshotQueryHit("snapshots-1", "snapshots-1_1", oneDayAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponseFromHits(Collections.singletonList(snapshot1_1)));
        Map<String, List<ModelSnapshot>> snapshots = new HashMap<>();
        // Should only return the one from 8 days ago
        snapshots.put("snapshots-1", Collections.singletonList(createModelSnapshot("snapshots-1", "snapshots-1_2", eightDaysAndOneMsAgo)));
        // Shouldn't return anything as retention is 17 days
        snapshots.put("snapshots-2", Collections.emptyList());

        SearchHit snapshot2_2 = createModelSnapshotQueryHit("snapshots-2", "snapshots-2_1", eightDaysAndOneMsAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponseFromHits(Collections.singletonList(snapshot2_2)));

        givenClientDeleteModelSnapshotRequestsFail(searchResponses, snapshots);
        createExpiredModelSnapshotsRemover(jobs.iterator()).remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(false));

        assertThat(capturedJobIds.size(), equalTo(1));
        assertThat(capturedJobIds.get(0), equalTo("snapshots-1"));

        assertThat(capturedDeleteModelSnapshotRequests.size(), equalTo(1));
        DeleteByQueryRequest deleteSnapshotRequest = capturedDeleteModelSnapshotRequests.get(0);
        assertThat(
            deleteSnapshotRequest.indices(),
            arrayContainingInAnyOrder(
                AnomalyDetectorsIndex.jobResultsAliasedName("snapshots-1"),
                AnomalyDetectorsIndex.jobStateIndexPattern(),
                AnnotationIndex.READ_ALIAS_NAME
            )
        );
        assertThat(deleteSnapshotRequest.getSearchRequest().source().query() instanceof IdsQueryBuilder, is(true));
        IdsQueryBuilder idsQueryBuilder = (IdsQueryBuilder) deleteSnapshotRequest.getSearchRequest().source().query();
        assertTrue(
            "expected ids related to [snapshots-1_2] but received [" + idsQueryBuilder.ids() + "]",
            idsQueryBuilder.ids().stream().allMatch(s -> s.contains("snapshots-1_2"))
        );
    }

    @SuppressWarnings("unchecked")
    public void testCalcCutoffEpochMs() {
        List<SearchResponse> searchResponses = new ArrayList<>();

        Date oneDayAgo = new Date(new Date().getTime() - TimeValue.timeValueDays(1).getMillis());
        SearchHit snapshot1_1 = createModelSnapshotQueryHit("job-1", "newest-snapshot", oneDayAgo);
        searchResponses.add(AbstractExpiredJobDataRemoverTests.createSearchResponseFromHits(Collections.singletonList(snapshot1_1)));

        givenClientRequests(
            searchResponses,
            true,
            true,
            Collections.singletonMap("job-1", Collections.singletonList(createModelSnapshot("job-1", "newest-snapshot", oneDayAgo)))
        );

        long retentionDays = 3L;
        ActionListener<AbstractExpiredJobDataRemover.CutoffDetails> cutoffListener = mock(ActionListener.class);
        createExpiredModelSnapshotsRemover(Collections.emptyIterator()).calcCutoffEpochMs("job-1", retentionDays, cutoffListener);

        long dayInMills = 60 * 60 * 24 * 1000;
        long expectedCutoffTime = oneDayAgo.getTime() - (dayInMills * retentionDays);
        verify(cutoffListener).onResponse(eq(new AbstractExpiredJobDataRemover.CutoffDetails(oneDayAgo.getTime(), expectedCutoffTime)));
    }

    private ExpiredModelSnapshotsRemover createExpiredModelSnapshotsRemover(Iterator<Job> jobIterator) {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executor = mock(ExecutorService.class);

        when(threadPool.executor(eq(MachineLearning.UTILITY_THREAD_POOL_NAME))).thenReturn(executor);

        doAnswer(invocationOnMock -> {
            Runnable run = (Runnable) invocationOnMock.getArguments()[0];
            run.run();
            return null;
        }).when(executor).execute(any());
        return new ExpiredModelSnapshotsRemover(
            originSettingClient,
            jobIterator,
            threadPool,
            new TaskId("test", 0L),
            resultsProvider,
            mock(AnomalyDetectionAuditor.class)
        );
    }

    private static ModelSnapshot createModelSnapshot(String jobId, String snapshotId, Date date) {
        return new ModelSnapshot.Builder(jobId).setSnapshotId(snapshotId).setTimestamp(date).build();
    }

    private static SearchHit createModelSnapshotQueryHit(String jobId, String snapshotId, Date date) {
        SearchHitBuilder hitBuilder = new SearchHitBuilder(0);
        hitBuilder.addField(Job.ID.getPreferredName(), Collections.singletonList(jobId));
        hitBuilder.addField(ModelSnapshotField.SNAPSHOT_ID.getPreferredName(), Collections.singletonList(snapshotId));
        String dateAsString = Long.valueOf(date.getTime()).toString();
        hitBuilder.addField(ModelSnapshot.TIMESTAMP.getPreferredName(), Collections.singletonList(dateAsString));
        return hitBuilder.build();
    }

    private void givenClientRequestsSucceed(List<SearchResponse> searchResponses, Map<String, List<ModelSnapshot>> snapshots) {
        givenClientRequests(searchResponses, true, true, snapshots);
    }

    private void givenClientSearchRequestsFail(List<SearchResponse> searchResponses, Map<String, List<ModelSnapshot>> snapshots) {
        givenClientRequests(searchResponses, false, true, snapshots);
    }

    private void givenClientDeleteModelSnapshotRequestsFail(
        List<SearchResponse> searchResponses,
        Map<String, List<ModelSnapshot>> snapshots
    ) {
        givenClientRequests(searchResponses, true, false, snapshots);
    }

    @SuppressWarnings("unchecked")
    private void givenClientRequests(
        List<SearchResponse> searchResponses,
        boolean shouldSearchRequestsSucceed,
        boolean shouldDeleteSnapshotRequestsSucceed,
        Map<String, List<ModelSnapshot>> snapshots
    ) {

        doAnswer(new Answer<Void>() {
            final AtomicInteger callCount = new AtomicInteger();

            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];

                // Only the last search request should fail
                if (shouldSearchRequestsSucceed || callCount.get() < (searchResponses.size() + snapshots.size())) {
                    SearchResponse response = searchResponses.get(callCount.getAndIncrement());
                    listener.onResponse(response);
                } else {
                    listener.onFailure(new RuntimeException("search failed"));
                }
                return null;
            }
        }).when(client).execute(same(SearchAction.INSTANCE), any(), any());

        doAnswer(invocationOnMock -> {
            capturedDeleteModelSnapshotRequests.add((DeleteByQueryRequest) invocationOnMock.getArguments()[1]);
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[2];
            if (shouldDeleteSnapshotRequestsSucceed) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new RuntimeException("delete snapshot failed"));
            }
            return null;
        }).when(client).execute(same(DeleteByQueryAction.INSTANCE), any(), any());

        for (Map.Entry<String, List<ModelSnapshot>> snapshot : snapshots.entrySet()) {
            doAnswer(new Answer<Void>() {
                final AtomicInteger callCount = new AtomicInteger();

                @Override
                public Void answer(InvocationOnMock invocationOnMock) {
                    capturedJobIds.add((String) invocationOnMock.getArguments()[0]);
                    Consumer<QueryPage<ModelSnapshot>> listener = (Consumer<QueryPage<ModelSnapshot>>) invocationOnMock.getArguments()[9];
                    Consumer<Exception> failure = (Consumer<Exception>) invocationOnMock.getArguments()[10];
                    if (shouldSearchRequestsSucceed || callCount.get() < snapshots.size()) {
                        callCount.incrementAndGet();
                        listener.accept(new QueryPage<>(snapshot.getValue(), 10, new ParseField("snapshots")));
                    } else {
                        failure.accept(new RuntimeException("search failed"));
                    }
                    return null;
                }
            }).when(resultsProvider)
                .modelSnapshots(eq(snapshot.getKey()), anyInt(), anyInt(), any(), any(), any(), anyBoolean(), any(), any(), any(), any());
        }

    }
}
