/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.time.RemainingTime;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.transform.action.TransportGetTransformAction.TransformIdCollector;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.transform.action.TransportGetTransformAction.TRANSFORM_ID_PAGE_SIZE;
import static org.elasticsearch.xpack.transform.action.TransportGetTransformAction.respondWithDanglingTasks;
import static org.elasticsearch.xpack.transform.action.TransportGetTransformAction.respondWithoutDanglingTaskCheckOnFailure;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class TransportGetTransformActionTests extends ESTestCase {

    private static final TaskId PARENT_TASK_ID = new TaskId("node-0", 42);
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testCollectsEveryTransformIdAcrossPages() {
        var transformIds = transformIds(randomIntBetween(0, 3 * TRANSFORM_ID_PAGE_SIZE));
        var client = new TransformIdSearchClient(threadPool, transformIds);

        var collected = collect(client, task(), () -> TIMEOUT);

        assertThat(collected, equalTo(Set.copyOf(transformIds)));
        assertThat(client.searches, hasSize(transformIds.size() / TRANSFORM_ID_PAGE_SIZE + 1));
        for (int i = 0; i < client.searches.size(); i++) {
            var search = client.searches.get(i);
            var expectedSearchAfter = i == 0 ? null : new Object[] { transformIds.get(i * TRANSFORM_ID_PAGE_SIZE - 1) };
            assertThat(search.source().searchAfter(), equalTo(expectedSearchAfter));
            assertThat(search.source().from(), equalTo(-1));
            assertThat(search.source().size(), equalTo(TRANSFORM_ID_PAGE_SIZE));
            assertThat(search.getParentTask(), equalTo(PARENT_TASK_ID));
            assertThat(((FieldSortBuilder) search.source().sorts().getFirst()).unmappedType(), equalTo("keyword"));
            assertThat(search.allowPartialSearchResults(), is(false));
            assertThat(search.indicesOptions().ignoreUnavailable(), is(true));
        }
    }

    /**
     * Offset paging fails once from + size passes index.max_result_window (10,000 by default); search_after paging does not.
     */
    public void testCollectsMoreTransformIdsThanMaxResultWindow() {
        var transformIds = transformIds(10_000 + randomIntBetween(1, 2 * TRANSFORM_ID_PAGE_SIZE));
        var client = new TransformIdSearchClient(threadPool, transformIds);

        assertThat(collect(client, task(), () -> TIMEOUT), equalTo(Set.copyOf(transformIds)));
    }

    /**
     * A config can exist in both the current and the deprecated internal index, so the same id can be returned twice and can straddle
     * a page boundary.
     */
    public void testCollectsTransformIdsDuplicatedAcrossIndices() {
        var uniqueIds = transformIds(2 * TRANSFORM_ID_PAGE_SIZE + randomIntBetween(0, 10));
        var transformIds = new ArrayList<String>();
        for (var transformId : uniqueIds) {
            transformIds.add(transformId);
            if (randomBoolean()) {
                transformIds.add(transformId);
            }
        }
        var client = new TransformIdSearchClient(threadPool, transformIds);

        assertThat(collect(client, task(), () -> TIMEOUT), equalTo(Set.copyOf(uniqueIds)));
    }

    /**
     * With partial results disallowed, a shard timeout fails the search, which must fail the collection rather than return the ids
     * collected so far.
     */
    public void testSearchTimeoutFailsCollection() {
        var client = new TransformIdSearchClient(threadPool, transformIds(10));
        client.failure = searchTimeout();

        var e = expectThrows(SearchPhaseExecutionException.class, () -> collect(client, task(), () -> TIMEOUT));
        assertThat(e, sameInstance(client.failure));
        assertThat(client.searches, hasSize(1));
    }

    public void testSkipsHitsWithoutTransformId() {
        var transformIds = transformIds(10);
        var transformIdsAndMissing = new ArrayList<String>(transformIds);
        transformIdsAndMissing.add(null);
        var client = new TransformIdSearchClient(threadPool, transformIdsAndMissing);

        assertThat(collect(client, task(), () -> TIMEOUT), equalTo(Set.copyOf(transformIds)));
    }

    public void testCancelledTaskDoesNotSearch() {
        var client = new TransformIdSearchClient(threadPool, transformIds(10));
        var task = task();
        TaskCancelHelper.cancel(task, "test");

        expectThrows(TaskCancelledException.class, () -> collect(client, task, () -> TIMEOUT));
        assertThat(client.searches, empty());
    }

    public void testCancellationBetweenPagesStopsPaging() {
        var task = task();
        var client = new TransformIdSearchClient(threadPool, transformIds(2 * TRANSFORM_ID_PAGE_SIZE)) {
            @Override
            void beforeResponse() {
                TaskCancelHelper.cancel(task, "test");
            }
        };

        expectThrows(TaskCancelledException.class, () -> collect(client, task, () -> TIMEOUT));
        assertThat(client.searches, hasSize(1));
    }

    public void testExpiredTimeoutFailsWithoutSearching() {
        var client = new TransformIdSearchClient(threadPool, transformIds(10));

        var e = expectThrows(ElasticsearchTimeoutException.class, () -> collect(client, task(), () -> TimeValue.ZERO));
        assertThat(e.getMessage(), containsString("Timed out after [30s] while collecting transform ids"));
        assertThat(client.searches, empty());
    }

    /**
     * GET _transform has always treated a negative timeout as already elapsed, not as "no timeout".
     */
    public void testNegativeTimeoutFailsWithoutSearching() {
        var client = new TransformIdSearchClient(threadPool, transformIds(10));
        var remainingTime = RemainingTime.from(Instant::now, randomFrom(TimeValue.MINUS_ONE, TimeValue.timeValueSeconds(-1)));

        expectThrows(ElasticsearchTimeoutException.class, () -> collect(client, task(), remainingTime));
        assertThat(client.searches, empty());
    }

    public void testDanglingTaskCheckTimeoutReturnsConfigsWithWarning() {
        var withoutCheck = new GetTransformAction.Response(List.of(), 2, null);
        var future = new PlainActionFuture<GetTransformAction.Response>();

        respondWithoutDanglingTaskCheckOnFailure("_all", task(), withoutCheck, future).onFailure(
            randomFrom(new ElasticsearchTimeoutException("timed out"), searchTimeout())
        );

        assertThat(future.actionGet(), sameInstance(withoutCheck));
        assertWarnings(TransformMessages.REST_WARN_DANGLING_TASK_CHECK_TIMED_OUT);
    }

    public void testDanglingTaskCheckFailureReturnsConfigsWithWarning() {
        var withoutCheck = new GetTransformAction.Response(List.of(), 2, null);
        var future = new PlainActionFuture<GetTransformAction.Response>();

        respondWithoutDanglingTaskCheckOnFailure("_all", task(), withoutCheck, future).onFailure(
            new SearchPhaseExecutionException("query", "all shards failed", ShardSearchFailure.EMPTY_ARRAY)
        );

        assertThat(future.actionGet(), sameInstance(withoutCheck));
        assertWarnings(TransformMessages.REST_WARN_DANGLING_TASK_CHECK_FAILED);
    }

    public void testDanglingTaskCheckFailureAfterCancellationFailsRequest() {
        var task = task();
        TaskCancelHelper.cancel(task, "test");
        var future = new PlainActionFuture<GetTransformAction.Response>();

        respondWithoutDanglingTaskCheckOnFailure("_all", task, new GetTransformAction.Response(List.of(), 2, null), future).onFailure(
            new TaskCancelledException("cancelled")
        );

        expectThrows(TaskCancelledException.class, future::actionGet);
    }

    public void testDanglingTaskCheckSuccessPassesThrough() {
        var withCheck = new GetTransformAction.Response(List.of(), 2, null);
        var future = new PlainActionFuture<GetTransformAction.Response>();

        respondWithoutDanglingTaskCheckOnFailure("_all", task(), new GetTransformAction.Response(List.of(), 2, null), future).onResponse(
            withCheck
        );

        assertThat(future.actionGet(), sameInstance(withCheck));
    }

    public void testRespondsWithTasksWithoutConfig() {
        var future = new PlainActionFuture<GetTransformAction.Response>();

        respondWithDanglingTasks("_all", task(), Set.of("b", "c"), List.of(), 2, future).onResponse(Set.of("a", "b"));

        var response = org.elasticsearch.common.Strings.toString(future.actionGet());
        assertThat(response, containsString("\"dangling_task\""));
        assertThat(response, containsString("Found task for transform [c], but no configuration for it"));
        assertThat(response, not(containsString("[b]")));
    }

    public void testRespondsWithoutErrorsWhenEveryTaskHasAConfig() {
        var future = new PlainActionFuture<GetTransformAction.Response>();

        respondWithDanglingTasks("_all", task(), Set.of("b"), List.of(), 2, future).onResponse(Set.of("a", "b"));

        assertThat(org.elasticsearch.common.Strings.toString(future.actionGet()), not(containsString("errors")));
    }

    public void testCollectionFailureRespondsWithWarning() {
        var future = new PlainActionFuture<GetTransformAction.Response>();

        respondWithDanglingTasks("_all", task(), Set.of("b"), List.of(), 2, future).onFailure(
            new SearchPhaseExecutionException("query", "all shards failed", ShardSearchFailure.EMPTY_ARRAY)
        );

        assertThat(org.elasticsearch.common.Strings.toString(future.actionGet()), not(containsString("errors")));
        assertWarnings(TransformMessages.REST_WARN_DANGLING_TASK_CHECK_FAILED);
    }

    /**
     * Listeners must not throw, and the listener utilities assert that. Without assertions, a throwing downstream listener must not be
     * turned into a second response by the dangling task check's fallback.
     */
    public void testThrowingDownstreamListenerIsNotRespondedToTwice() {
        var listener = new ThrowingListener<GetTransformAction.Response>();

        expectThrows(
            AssertionError.class,
            () -> respondWithDanglingTasks("_all", task(), Set.of("b"), List.of(), 2, listener).onResponse(Set.of("b"))
        );

        assertThat(listener.responses, equalTo(1));
        assertThat(listener.failures, equalTo(0));
    }

    public void testThrowingListenerOnLastPageIsNotFailed() {
        var client = new TransformIdSearchClient(threadPool, transformIds(10));
        var listener = new ThrowingListener<Set<String>>();

        expectThrows(
            AssertionError.class,
            () -> new TransformIdCollector(client, new String[] { "*" }, task(), PARENT_TASK_ID, TIMEOUT, () -> TIMEOUT).collect(listener)
        );

        assertThat(listener.responses, equalTo(1));
        assertThat(listener.failures, equalTo(0));
    }

    private static class ThrowingListener<T> implements ActionListener<T> {
        int responses;
        int failures;

        @Override
        public void onResponse(T response) {
            responses++;
            throw new IllegalStateException("downstream failure");
        }

        @Override
        public void onFailure(Exception e) {
            failures++;
        }
    }

    private static Set<String> collect(TransformIdSearchClient client, CancellableTask task, RemainingTime remainingTime) {
        var future = new PlainActionFuture<Set<String>>();
        new TransformIdCollector(client, new String[] { "*" }, task, PARENT_TASK_ID, TIMEOUT, remainingTime).collect(future);
        return future.actionGet();
    }

    /**
     * How a shard timeout surfaces when partial results are disallowed.
     */
    private static SearchPhaseExecutionException searchTimeout() {
        return new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] { new ShardSearchFailure(new SearchTimeoutException(null, "Time exceeded")) }
        );
    }

    private static CancellableTask task() {
        return new CancellableTask(1, "transport", "cluster:monitor/transform/get", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static List<String> transformIds(int count) {
        return IntStream.range(0, count).mapToObj(i -> Strings.format("transform-%06d", i)).toList();
    }

    /**
     * Answers searches from an in-memory, sorted list of transform ids, honouring {@code from}, {@code search_after}, {@code size}
     * and the default {@code index.max_result_window} the way a search of the transform internal index sorted by id would. A stub is
     * used because the collector only needs paged search responses, not a running cluster.
     */
    private static class TransformIdSearchClient extends NoOpClient {
        private final List<String> transformIds;
        final List<SearchRequest> searches = new ArrayList<>();
        Exception failure;

        TransformIdSearchClient(ThreadPool threadPool, List<String> transformIds) {
            super(threadPool);
            this.transformIds = transformIds;
        }

        void beforeResponse() {}

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(TransportSearchAction.TYPE));
            var searchRequest = (SearchRequest) request;
            searches.add(searchRequest);
            if (failure != null) {
                listener.onFailure(failure);
                return;
            }

            int size = searchRequest.source().size();
            int from = Math.max(searchRequest.source().from(), 0);
            if (from + size > IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(Settings.EMPTY)) {
                listener.onFailure(new IllegalArgumentException("Result window is too large, from + size must be <= 10000"));
                return;
            }
            var searchAfter = searchRequest.source().searchAfter();
            if (searchAfter != null) {
                while (from < transformIds.size() && transformIds.get(from).compareTo((String) searchAfter[0]) <= 0) {
                    from++;
                }
            }
            int to = Math.min(from + size, transformIds.size());
            var hits = transformIds.subList(from, to).stream().map(TransformIdSearchClient::hit).toArray(SearchHit[]::new);
            var searchHits = new SearchHits(hits, new TotalHits(transformIds.size(), TotalHits.Relation.EQUAL_TO), 1.0f);
            var response = SearchResponseUtils.successfulResponse(searchHits);
            searchHits.decRef(); // transfer ownership to response

            beforeResponse();
            ActionListener.respondAndRelease((ActionListener<SearchResponse>) listener, response);
        }

        /**
         * A null transform id stands for a config document without an id, which sorts last.
         */
        private static SearchHit hit(String transformId) {
            var hit = new SearchHit(0);
            if (transformId != null) {
                hit.setDocumentField(new DocumentField(TransformField.ID.getPreferredName(), List.of(transformId)));
                hit.sortValues(new Object[] { new BytesRef(transformId) }, new DocValueFormat[] { DocValueFormat.RAW });
            }
            return hit;
        }
    }
}
