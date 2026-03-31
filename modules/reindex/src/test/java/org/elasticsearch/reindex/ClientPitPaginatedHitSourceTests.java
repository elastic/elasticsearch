/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.lang.Math.abs;
import static java.util.stream.Collectors.toList;
import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link ClientPitPaginatedHitSource}.
 */
public class ClientPitPaginatedHitSourceTests extends ESTestCase {

    private static final BytesReference PIT_ID = new BytesArray("pit-id".getBytes(StandardCharsets.UTF_8));
    private static final TimeValue KEEP_ALIVE = timeValueMinutes(5);

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    /** Verifies the happy path: start, first search, consume batch, next search with search_after. */
    public void testStartPitDone() throws InterruptedException {
        doTestBasicsWithRetry(0, 0, 0, e -> fail());
    }

    /** Verifies retries on rejection succeed within the retry limit. */
    public void testRetrySuccess() throws InterruptedException {
        int retries = randomIntBetween(1, 10);
        doTestBasicsWithRetry(retries, 0, retries, e -> fail());
    }

    /** Verifies retries on rejection fail when exceeding the retry limit. */
    public void testRetryFail() throws InterruptedException {
        final int retries = randomInt(10);
        final var exceptionRef = new AtomicReference<Exception>();
        doTestBasicsWithRetry(retries, retries + 1, retries + 1, exceptionRef::set);
        assertThat(exceptionRef.get(), instanceOf(EsRejectedExecutionException.class));
    }

    /** Verifies that restoreState resumes from PitWorkerResumeInfo and fetches next batch. */
    public void testRestoreStateResumesFromPitWorkerResumeInfo() throws InterruptedException {
        BlockingQueue<PaginatedHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());
        Object[] searchAfterValues = new Object[] { 100L, "sort-key" };
        ResumeInfo.PitWorkerResumeInfo resumeInfo = new ResumeInfo.PitWorkerResumeInfo(
            PIT_ID,
            searchAfterValues,
            System.currentTimeMillis(),
            randomStatusWithoutException(),
            null
        );

        ClientPitPaginatedHitSource paginatedHitSource = new ClientPitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            responses::add,
            e -> fail(),
            new ParentTaskAssigningClient(client, parentTask),
            createPitSearchRequest()
        );

        paginatedHitSource.resume(resumeInfo);
        client.validateRequest(TransportSearchAction.TYPE, (SearchRequest r) -> {
            assertNotNull(r.source().searchAfter());
            assertArrayEquals(searchAfterValues, r.source().searchAfter());
            assertNotNull(r.source().pointInTimeBuilder());
        });
        SearchResponse searchResponse = createPitSearchResponse();
        try {
            client.respond(TransportSearchAction.TYPE, searchResponse);
            PaginatedHitSource.AsyncResponse asyncResponse = responses.poll(10, TimeUnit.SECONDS);
            assertNotNull(asyncResponse);
            assertSameHits(asyncResponse.response().getHits(), searchResponse.getHits().getHits());
        } finally {
            searchResponse.decRef();
        }
    }

    /** Verifies that an empty (0-hit) response completes without requesting the next batch. */
    public void testEmptyResponseCompletesWithoutRequestingNextBatch() throws InterruptedException {
        BlockingQueue<PaginatedHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());

        ClientPitPaginatedHitSource paginatedHitSource = new ClientPitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            responses::add,
            e -> fail(),
            new ParentTaskAssigningClient(client, parentTask),
            createPitSearchRequest()
        );

        paginatedHitSource.start();
        client.validateRequest(TransportSearchAction.TYPE, (SearchRequest r) -> assertSame(Boolean.FALSE, r.allowPartialSearchResults()));
        SearchResponse searchResponse = createPitSearchResponse(0);
        try {
            client.respond(TransportSearchAction.TYPE, searchResponse);
            PaginatedHitSource.AsyncResponse asyncResponse = responses.poll(10, TimeUnit.SECONDS);
            assertNotNull(asyncResponse);
            assertTrue(asyncResponse.response().getHits().isEmpty());
            assertFalse(paginatedHitSource.hasMoreBatches());
            // Do not call done() - we're at the end, no search_after values for next batch
        } finally {
            searchResponse.decRef();
        }
    }

    /** Verifies hasMoreBatches reflects search_after state. */
    public void testHasMoreBatches() {
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("id", randomInt());

        ClientPitPaginatedHitSource paginatedHitSource = new ClientPitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            r -> fail(),
            e -> fail(),
            new ParentTaskAssigningClient(client, parentTask),
            createPitSearchRequest()
        );

        // Initially: no search_after -> false
        assertFalse(paginatedHitSource.hasMoreBatches());

        // Non-null search_after -> true
        paginatedHitSource.setSearchAfterValues(new Object[] { 1L, "sort" });
        assertTrue(paginatedHitSource.hasMoreBatches());

        paginatedHitSource.setSearchAfterValues(null);
        assertFalse(paginatedHitSource.hasMoreBatches());
    }

    /** Verifies getPitId returns the initial PIT ID before any search. */
    public void testGetPitIdReturnsInitialValueBeforeSearch() {
        ClientPitPaginatedHitSource paginatedHitSource = new ClientPitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            r -> fail(),
            e -> fail(),
            new ParentTaskAssigningClient(new MockClient(threadPool), new TaskId("n", randomInt())),
            createPitSearchRequest()
        );
        assertThat(paginatedHitSource.getPitId(), equalTo(PIT_ID));
    }

    /** Verifies getPitId returns the updated PIT ID after a search response with a different pointInTimeId. */
    public void testGetPitIdReturnsUpdatedValueAfterSearchResponse() throws InterruptedException {
        BytesReference updatedPitId = new BytesArray("updated-pit-id".getBytes(StandardCharsets.UTF_8));
        BlockingQueue<PaginatedHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());

        ClientPitPaginatedHitSource paginatedHitSource = new ClientPitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            responses::add,
            e -> fail(),
            new ParentTaskAssigningClient(client, parentTask),
            createPitSearchRequest()
        );

        paginatedHitSource.start();
        SearchResponse searchResponse = SearchResponseUtils.response(SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0))
            .pointInTimeId(updatedPitId)
            .shards(5, 4, 0)
            .build();
        try {
            client.respond(TransportSearchAction.TYPE, searchResponse);
            responses.poll(10, TimeUnit.SECONDS);
            assertThat(paginatedHitSource.getPitId(), equalTo(updatedPitId));
        } finally {
            searchResponse.decRef();
        }
    }

    /** Verifies setSearchAfterValues is used when requestNextBatch is called. */
    public void testSetSearchAfterValuesUsedForNextBatch() throws InterruptedException {
        BlockingQueue<PaginatedHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());
        Object[] searchAfterValues = new Object[] { 100L, "sort-key" };

        ClientPitPaginatedHitSource paginatedHitSource = new ClientPitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            responses::add,
            e -> fail(),
            new ParentTaskAssigningClient(client, parentTask),
            createPitSearchRequest()
        );

        paginatedHitSource.setSearchAfterValues(searchAfterValues);
        paginatedHitSource.requestNextBatch(TimeValue.ZERO);
        client.validateRequest(TransportSearchAction.TYPE, (SearchRequest r) -> {
            assertNotNull(r.source().searchAfter());
            assertArrayEquals(searchAfterValues, r.source().searchAfter());
        });
        SearchResponse searchResponse = createPitSearchResponse();
        try {
            client.respond(TransportSearchAction.TYPE, searchResponse);
            PaginatedHitSource.AsyncResponse asyncResponse = responses.poll(10, TimeUnit.SECONDS);
            assertNotNull(asyncResponse);
        } finally {
            searchResponse.decRef();
        }
    }

    /** Verifies that constructor rejects SearchRequest without pointInTimeBuilder. */
    public void testConstructorRejectsMissingPointInTime() {
        MockClient client = new MockClient(threadPool);
        ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(client, new TaskId("n", randomInt()));
        SearchRequest nullSource = new SearchRequest();
        SearchRequest sourceWithoutPit = new SearchRequest().source(new SearchSourceBuilder());
        for (SearchRequest request : randomBoolean()
            ? new SearchRequest[] { nullSource, sourceWithoutPit }
            : new SearchRequest[] { sourceWithoutPit, nullSource }) {
            expectThrows(
                IllegalArgumentException.class,
                () -> new ClientPitPaginatedHitSource(
                    logger,
                    BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
                    threadPool,
                    () -> {},
                    r -> {},
                    e -> {},
                    assigningClient,
                    request
                )
            );
        }
    }

    private void doTestBasicsWithRetry(int retries, int minFailures, int maxFailures, Consumer<Exception> failureHandler)
        throws InterruptedException {
        BlockingQueue<PaginatedHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());
        AtomicInteger actualSearchRetries = new AtomicInteger();
        int expectedSearchRetries = 0;

        ClientPitPaginatedHitSource paginatedHitSource = new ClientPitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, retries),
            threadPool,
            actualSearchRetries::incrementAndGet,
            responses::add,
            failureHandler,
            new ParentTaskAssigningClient(client, parentTask),
            createPitSearchRequest()
        );

        paginatedHitSource.start();
        for (int retry = 0; retry < randomIntBetween(minFailures, maxFailures); ++retry) {
            client.fail(TransportSearchAction.TYPE, new EsRejectedExecutionException());
            if (retry >= retries) {
                return;
            }
            client.awaitOperation();
            ++expectedSearchRetries;
        }
        client.validateRequest(TransportSearchAction.TYPE, (SearchRequest r) -> assertSame(Boolean.FALSE, r.allowPartialSearchResults()));
        SearchResponse searchResponse = createPitSearchResponse();
        try {
            client.respond(TransportSearchAction.TYPE, searchResponse);

            for (int i = 0; i < randomIntBetween(1, 10); ++i) {
                PaginatedHitSource.AsyncResponse asyncResponse = responses.poll(10, TimeUnit.SECONDS);
                assertNotNull(asyncResponse);
                assertEquals(0, responses.size());
                assertSameHits(asyncResponse.response().getHits(), searchResponse.getHits().getHits());
                asyncResponse.done(TimeValue.ZERO);

                for (int retry = 0; retry < randomIntBetween(minFailures, maxFailures); ++retry) {
                    client.fail(TransportSearchAction.TYPE, new EsRejectedExecutionException());
                    client.awaitOperation();
                    ++expectedSearchRetries;
                }

                searchResponse.decRef();
                searchResponse = createPitSearchResponse();
                client.respond(TransportSearchAction.TYPE, searchResponse);
            }

            assertEquals(actualSearchRetries.get(), expectedSearchRetries);
        } finally {
            searchResponse.decRef();
        }
    }

    private static SearchRequest createPitSearchRequest() {
        SearchSourceBuilder source = new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(PIT_ID).setKeepAlive(KEEP_ALIVE));
        return new SearchRequest().source(source);
    }

    private SearchResponse createPitSearchResponse() {
        return createPitSearchResponse(randomIntBetween(1, 20));
    }

    private SearchResponse createPitSearchResponse(int hitCount) {
        Object[] sortValues = new Object[] { randomLong(), randomAlphaOfLengthBetween(1, 10) };
        SearchHit hit = SearchHit.unpooled(0, "id").sourceRef(new BytesArray("{}"));
        hit.sortValues(sortValues, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW });
        SearchHits hits = SearchHits.unpooled(
            IntStream.range(0, hitCount).mapToObj(i -> hit).toArray(SearchHit[]::new),
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            0
        );
        return SearchResponseUtils.response(hits).pointInTimeId(PIT_ID).shards(5, 4, 0).build();
    }

    private void assertSameHits(List<? extends PaginatedHitSource.Hit> actual, SearchHit[] expected) {
        assertEquals(actual.size(), expected.length);
        for (int i = 0; i < actual.size(); ++i) {
            assertThat(expected[i].getSourceRef(), equalBytes(actual.get(i).getSource()));
            assertEquals(actual.get(i).getIndex(), expected[i].getIndex());
            assertEquals(actual.get(i).getVersion(), expected[i].getVersion());
            assertEquals(actual.get(i).getPrimaryTerm(), expected[i].getPrimaryTerm());
            assertEquals(actual.get(i).getSeqNo(), expected[i].getSeqNo());
            assertEquals(actual.get(i).getId(), expected[i].getId());
        }
    }

    private static BulkByScrollTask.Status randomStatusWithoutException() {
        if (randomBoolean()) {
            return randomWorkingStatus(null);
        }
        boolean canHaveNullStatues = randomBoolean();
        List<BulkByScrollTask.StatusOrException> statuses = IntStream.range(0, between(0, 10)).mapToObj(i -> {
            if (canHaveNullStatues && rarely()) {
                return null;
            }
            return new BulkByScrollTask.StatusOrException(randomWorkingStatus(i));
        }).collect(toList());
        return new BulkByScrollTask.Status(statuses, randomBoolean() ? "test" : null);
    }

    private static BulkByScrollTask.Status randomWorkingStatus(Integer sliceId) {
        int total = between(0, 10000000);
        int updated = between(0, total);
        int created = between(0, total - updated);
        int deleted = between(0, total - updated - created);
        int noops = total - updated - created - deleted;
        int batches = between(0, 10000);
        long versionConflicts = between(0, total);
        long bulkRetries = between(0, 10000000);
        long searchRetries = between(0, 100000);
        TimeUnit[] timeUnits = { TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS };
        TimeValue throttled = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        TimeValue throttledUntil = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        return new BulkByScrollTask.Status(
            sliceId,
            total,
            updated,
            created,
            deleted,
            batches,
            versionConflicts,
            noops,
            bulkRetries,
            searchRetries,
            throttled,
            abs(randomFloat()),
            randomBoolean() ? null : randomSimpleString(random()),
            throttledUntil
        );
    }

    private static class ExecuteRequest<Request extends ActionRequest, Response extends ActionResponse> {
        private final ActionType<Response> action;
        private final Request request;
        private final ActionListener<Response> listener;

        ExecuteRequest(ActionType<Response> action, Request request, ActionListener<Response> listener) {
            this.action = action;
            this.request = request;
            this.listener = listener;
        }

        public void respond(ActionType<Response> actionType, Function<Request, Response> response) {
            assertEquals(actionType, this.action);
            listener.onResponse(response.apply(request));
        }

        public void fail(ActionType<Response> actionType, Exception response) {
            assertEquals(actionType, this.action);
            listener.onFailure(response);
        }

        public void validateRequest(ActionType<Response> actionType, Consumer<? super Request> validator) {
            assertEquals(actionType, this.action);
            validator.accept(request);
        }
    }

    private static class MockClient extends AbstractClient {
        private ExecuteRequest<?, ?> executeRequest;

        MockClient(ThreadPool threadPool) {
            super(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow());
        }

        @Override
        protected synchronized <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {

            this.executeRequest = new ExecuteRequest<>(action, request, listener);
            this.notifyAll();
        }

        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void respondx(
            ActionType<Response> action,
            Function<Request, Response> response
        ) {
            ExecuteRequest<?, ?> executeRequestCopy;
            synchronized (this) {
                executeRequestCopy = this.executeRequest;
                this.executeRequest = null;
            }
            ((ExecuteRequest<Request, Response>) executeRequestCopy).respond(action, response);
        }

        public <Response extends ActionResponse> void respond(ActionType<Response> action, Response response) {
            respondx(action, req -> response);
        }

        @SuppressWarnings("unchecked")
        public <Response extends ActionResponse> void fail(ActionType<Response> action, Exception response) {
            ExecuteRequest<?, ?> executeRequestCopy;
            synchronized (this) {
                executeRequestCopy = this.executeRequest;
                this.executeRequest = null;
            }
            ((ExecuteRequest<?, Response>) executeRequestCopy).fail(action, response);
        }

        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void validateRequest(
            ActionType<Response> action,
            Consumer<? super Request> validator
        ) {
            ((ExecuteRequest<Request, Response>) executeRequest).validateRequest(action, validator);
        }

        public synchronized void awaitOperation() throws InterruptedException {
            if (executeRequest == null) {
                wait(10000);
                assertNotNull("Must receive next request within 10s", executeRequest);
            }
        }
    }
}
