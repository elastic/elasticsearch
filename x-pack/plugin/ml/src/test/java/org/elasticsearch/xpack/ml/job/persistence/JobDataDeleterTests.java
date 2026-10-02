/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler.ScheduledCancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.retention.MockWritableIndexExpander;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobDataDeleterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";

    private Client client;
    private ArgumentCaptor<DeleteByQueryRequest> deleteRequestCaptor;

    @Before
    public void setUpTests() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        deleteRequestCaptor = ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    }

    public void testDeleteAllAnnotations() {
        MockWritableIndexExpander.create(true);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteAllAnnotations(ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));

            if (deleteUserAnnotations) {
                verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            } else {
                verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            }

            DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
            assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
            String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
            assertThat(dbqQueryString, not(containsString("timestamp")));
            assertThat(dbqQueryString, not(containsString("event")));
            if (deleteUserAnnotations) {
                assertThat(dbqQueryString, not(containsString("_xpack")));
            } else {
                assertThat(dbqQueryString, containsString("_xpack"));
            }
            assertThat(deleteRequest.getSlices(), equalTo(1));
            assertThat(deleteRequest.getScrollTime(), equalTo(TimeValue.timeValueMinutes(1)));
        });
        verify(client, times(2)).threadPool();
    }

    public void testDeleteAnnotations_TimestampFiltering() {
        MockWritableIndexExpander.create(true);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            Tuple<Long, Long> range = randomFrom(
                tuple(1_000_000_000L, 2_000_000_000L),
                tuple(1_000_000_000L, null),
                tuple(null, 2_000_000_000L)
            );
            jobDataDeleter.deleteAnnotations(range.v1(), range.v2(), null, ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));

            if (deleteUserAnnotations) {
                verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            } else {
                verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            }

            DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
            assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
            String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
            assertThat(dbqQueryString, containsString("timestamp"));
            assertThat(dbqQueryString, not(containsString("event")));
            if (deleteUserAnnotations) {
                assertThat(dbqQueryString, not(containsString("_xpack")));
            } else {
                assertThat(dbqQueryString, containsString("_xpack"));
            }
            assertThat(deleteRequest.getSlices(), equalTo(1));
            assertThat(deleteRequest.getScrollTime(), equalTo(TimeValue.timeValueMinutes(1)));
        });
        verify(client, times(2)).threadPool();
    }

    public void testDeleteAnnotations_EventFiltering() {
        MockWritableIndexExpander.create(true);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteAnnotations(
                null,
                null,
                Set.of("dummy_event"),
                ActionTestUtils.assertNoFailureListener(deleteResponse -> {})
            );

            if (deleteUserAnnotations) {
                verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            } else {
                verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            }

            DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
            assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
            String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
            assertThat(dbqQueryString, not(containsString("timestamp")));
            assertThat(dbqQueryString, containsString("event"));
            if (deleteUserAnnotations) {
                assertThat(dbqQueryString, not(containsString("_xpack")));
            } else {
                assertThat(dbqQueryString, containsString("_xpack"));
            }
            assertThat(deleteRequest.getSlices(), equalTo(1));
            assertThat(deleteRequest.getScrollTime(), equalTo(TimeValue.timeValueMinutes(1)));
        });
        verify(client, times(2)).threadPool();
    }

    public void testDeleteResultsFromTime() {
        MockWritableIndexExpander.create(true);
        long fromEpochMs = randomNonNegativeLong();
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, randomBoolean());
        jobDataDeleter.deleteResultsFromTime(fromEpochMs, ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));

        verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());

        DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
        assertThat(deleteRequest.indices(), is(arrayContaining(".ml-anomalies-my-job-id")));
        String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
        assertThat(dbqQueryString, containsString("{\"term\":{\"job_id\":{\"value\":\"my-job-id\"}}"));
        assertThat(deleteRequest.getSlices(), equalTo(1));
        assertThat(deleteRequest.getScrollTime(), equalTo(TimeValue.timeValueMinutes(1)));
        verify(client, times(1)).threadPool();
    }

    public void testDeleteDatafeedTimingStats() {
        MockWritableIndexExpander.create(true);
        ArgumentCaptor<DeleteRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        // deleteDatafeedTimingStats does not use the deleteUserAnnotations flag, so both iterations
        // produce identical client calls; we verify the cumulative counts after the loop.
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteDatafeedTimingStats(ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));
        });
        verify(client, never()).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(client, times(2)).execute(eq(TransportDeleteAction.TYPE), deleteCaptor.capture(), any());

        DeleteRequest deleteRequest = deleteCaptor.getValue();
        assertThat(deleteRequest.index(), is(AnomalyDetectorsIndex.jobResultsAliasedName(JOB_ID)));
        assertThat(deleteRequest.id(), is(DatafeedTimingStats.documentId(JOB_ID)));
        assertThat(deleteRequest.getRefreshPolicy(), is(WriteRequest.RefreshPolicy.IMMEDIATE));

        verify(client, times(2)).threadPool();
    }

    public void testDeleteDatafeedTimingStats_WhenIndexReadOnly_ShouldNotDeleteAnything() {
        MockWritableIndexExpander.create(false);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteDatafeedTimingStats(ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));
        });
        verify(client, never()).execute(eq(TransportDeleteAction.TYPE), any(DeleteRequest.class), any());
    }

    public void testDeleteInterimResultsShouldUseSingleSlice() throws Exception {
        MockWritableIndexExpander.create(true);
        PlainActionFuture<BulkByPaginatedSearchResponse> future = new PlainActionFuture<>();
        future.onResponse(emptyBulkByPaginatedSearchResponse());
        when(client.execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture())).thenReturn(future);

        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteInterimResults();

        DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
        assertThat(deleteRequest.indices(), is(arrayContaining(AnomalyDetectorsIndex.jobResultsAliasedName(JOB_ID))));
        String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
        assertThat(dbqQueryString, containsString("is_interim"));
        assertThat(deleteRequest.getSlices(), equalTo(1));
        verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture());
        verify(client).threadPool();
    }

    public void testDeleteResultsFromTimeShouldRetrySearchContextMissingWithFreshRequests() {
        MockWritableIndexExpander.create(true);
        ThreadPool threadPool = mockThreadPoolForScheduledRetry();
        when(client.threadPool()).thenReturn(threadPool);

        AtomicInteger executeCount = new AtomicInteger();
        ArgumentCaptor<Runnable> retryRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<TimeValue> retryDelayCaptor = ArgumentCaptor.forClass(TimeValue.class);
        when(threadPool.schedule(retryRunnableCaptor.capture(), retryDelayCaptor.capture(), any())).thenReturn(
            mock(ScheduledCancellable.class)
        );

        doAnswer(invocation -> {
            ActionListener<BulkByPaginatedSearchResponse> listener = invocation.getArgument(2);
            if (executeCount.incrementAndGet() == 1) {
                listener.onResponse(searchContextMissingResponse());
            } else {
                listener.onResponse(emptyBulkByPaginatedSearchResponse());
            }
            return null;
        }).when(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());

        AtomicInteger completions = new AtomicInteger();
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteResultsFromTime(1_000L, ActionTestUtils.assertNoFailureListener(r -> completions.incrementAndGet()));

        assertThat(executeCount.get(), equalTo(1));
        assertThat(retryDelayCaptor.getValue(), equalTo(TimeValue.timeValueSeconds(30)));
        retryRunnableCaptor.getValue().run();

        List<DeleteByQueryRequest> requests = deleteRequestCaptor.getAllValues();
        assertThat(requests.size(), equalTo(2));
        assertThat(requests.get(0), not(sameInstance(requests.get(1))));
        assertThat(requests.get(0).getSlices(), equalTo(1));
        assertThat(requests.get(1).getScrollTime(), equalTo(TimeValue.timeValueMinutes(1)));
        assertThat(completions.get(), equalTo(1));
        verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(client, atLeastOnce()).threadPool();
    }

    public void testDeleteAnnotationsShouldRetrySearchContextMissingWithFreshRequests() {
        MockWritableIndexExpander.create(true);
        ThreadPool threadPool = mockThreadPoolForScheduledRetry();
        when(client.threadPool()).thenReturn(threadPool);

        AtomicInteger executeCount = new AtomicInteger();
        SearchContextMissingException scm = new SearchContextMissingException(new ShardSearchContextId("s", 1L));
        ArgumentCaptor<Runnable> retryRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(threadPool.schedule(retryRunnableCaptor.capture(), any(TimeValue.class), any())).thenReturn(mock(ScheduledCancellable.class));

        doAnswer(invocation -> {
            ActionListener<BulkByPaginatedSearchResponse> listener = invocation.getArgument(2);
            if (executeCount.incrementAndGet() == 1) {
                listener.onFailure(scm);
            } else {
                listener.onResponse(emptyBulkByPaginatedSearchResponse());
            }
            return null;
        }).when(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());

        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteAnnotations(null, null, null, ActionTestUtils.assertNoFailureListener(r -> {}));

        assertThat(executeCount.get(), equalTo(1));
        retryRunnableCaptor.getValue().run();

        List<DeleteByQueryRequest> requests = deleteRequestCaptor.getAllValues();
        assertThat(requests.size(), equalTo(2));
        assertThat(requests.get(0), not(sameInstance(requests.get(1))));
        verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(client, atLeastOnce()).threadPool();
    }

    public void testDeleteResultsFromTimeShouldFailAfterBoundedSearchContextMissingRetries() {
        MockWritableIndexExpander.create(true);
        ThreadPool threadPool = mockThreadPoolForScheduledRetry();
        when(client.threadPool()).thenReturn(threadPool);

        ArgumentCaptor<Runnable> retryRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(threadPool.schedule(retryRunnableCaptor.capture(), any(TimeValue.class), any())).thenReturn(mock(ScheduledCancellable.class));

        doAnswer(invocation -> {
            ActionListener<BulkByPaginatedSearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchContextMissingResponse());
            return null;
        }).when(client).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());

        AtomicInteger failures = new AtomicInteger();
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteResultsFromTime(1_000L, new org.elasticsearch.action.ActionListener<>() {
            @Override
            public void onResponse(Boolean response) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                failures.incrementAndGet();
                assertTrue(isSearchContextMissing(e));
            }
        });

        retryRunnableCaptor.getValue().run();
        verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        assertThat(failures.get(), equalTo(1));
        verify(client, atLeastOnce()).threadPool();
    }

    public void testDeleteResultsFromTimeShouldNotRetryUnrelatedFailures() {
        MockWritableIndexExpander.create(true);
        RuntimeException failure = new RuntimeException("boom");
        doAnswer(invocation -> {
            ActionListener<BulkByPaginatedSearchResponse> listener = invocation.getArgument(2);
            listener.onFailure(failure);
            return null;
        }).when(client).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());

        AtomicInteger failures = new AtomicInteger();
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteResultsFromTime(1_000L, new org.elasticsearch.action.ActionListener<>() {
            @Override
            public void onResponse(Boolean response) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                failures.incrementAndGet();
                assertSame(failure, e);
            }
        });

        verify(client, times(1)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        assertThat(failures.get(), equalTo(1));
        verify(client, times(1)).threadPool();
    }

    private static BulkByPaginatedSearchResponse emptyBulkByPaginatedSearchResponse() {
        return new BulkByPaginatedSearchResponse(
            TimeValue.ZERO,
            new BulkByPaginatedSearchTask.Status(Collections.emptyList(), null, 0f),
            Collections.emptyList(),
            Collections.emptyList(),
            false
        );
    }

    private static BulkByPaginatedSearchResponse searchContextMissingResponse() {
        SearchContextMissingException scm = new SearchContextMissingException(new ShardSearchContextId("s", 1L));
        return new BulkByPaginatedSearchResponse(
            TimeValue.ZERO,
            new BulkByPaginatedSearchTask.Status(Collections.emptyList(), null, 0f),
            Collections.emptyList(),
            List.of(new PaginatedSearchFailure(scm)),
            false
        );
    }

    private static boolean isSearchContextMissing(Exception e) {
        return org.elasticsearch.ExceptionsHelper.unwrap(e, SearchContextMissingException.class) != null;
    }

    private ThreadPool mockThreadPoolForScheduledRetry() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        ExecutorService generic = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        when(threadPool.generic()).thenReturn(generic);
        return threadPool;
    }
}
