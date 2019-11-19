/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;

public class ClientScrollableHitSourceTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    // ensure we test the happy path on every build.
    public void testStartScrollDone() throws InterruptedException {
        dotestBasicsWithRetry(0, 0, 0, e -> fail());
    }

    public void testRetrySuccess() throws InterruptedException {
        int retries = randomIntBetween(1, 10);
        dotestBasicsWithRetry(retries, 0, retries, e -> fail());
    }

    public void testRetryFail() {
        int retries = randomInt(10);
        ExpectedException ex = expectThrows(ExpectedException.class,
            () -> dotestBasicsWithRetry(retries, retries+1, retries+1, e -> { throw new ExpectedException(e); })
        );
        assertThat(ex.getCause(), anyOf(instanceOf(EsRejectedExecutionException.class), instanceOf(SearchFailureException.class)));
    }

    private void dotestBasicsWithRetry(int retries, int minFailures, int maxFailures,
                                       Consumer<Exception> failureHandler) throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool, logger);
        TaskId parentTask = new TaskId("thenode", randomInt());
        AtomicInteger actualSearchRetries = new AtomicInteger();
        int expectedSearchRetries = 0;
        LongSupplier seqNoGenerator = newSeqNoGenerator(0);
        long seqNo = Long.MIN_VALUE;
        ScrollableHitSource.Checkpoint initialCheckpoint = null;
        if (randomBoolean()) {
            seqNo = seqNoGenerator.getAsLong();
            initialCheckpoint = new ScrollableHitSource.Checkpoint(seqNo);
        }
        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(logger, BackoffPolicy.constantBackoff(TimeValue.ZERO, retries),
            threadPool, actualSearchRetries::incrementAndGet, response -> handleResponse(responses, response), failureHandler,
            new ParentTaskAssigningClient(client, parentTask),
            new SearchRequest().scroll("1m"), true, initialCheckpoint);

        hitSource.start();
        for (int retry = 0; retry < randomIntBetween(0, maxFailures); ++retry) {
            client.fail(SearchAction.INSTANCE, new EsRejectedExecutionException());
            client.awaitOperation();
            ++expectedSearchRetries;
            validateSeqNoCondition(client, seqNo);
        }
        client.validateRequest(SearchAction.INSTANCE, (SearchRequest r) -> assertTrue(r.allowPartialSearchResults() == Boolean.FALSE));
        SearchResponse searchResponse = createSearchResponse(seqNoGenerator);
        client.respond(SearchAction.INSTANCE, searchResponse);
        seqNo = extractSeqNoFromSearchResponse(searchResponse, seqNo);
        String scrollId = searchResponse.getScrollId();

        for (int i = 0; i < randomIntBetween(1, 10); ++i) {
            ScrollableHitSource.AsyncResponse asyncResponse = responses.poll(10, TimeUnit.SECONDS);
            assertNotNull(asyncResponse);
            assertEquals(responses.size(), 0);
            assertSameHits(asyncResponse.response().getHits(), searchResponse.getHits().getHits());
            assertEquals(seqNo, asyncResponse.getCheckpoint().restartFromValue);
            asyncResponse.done(TimeValue.ZERO);

            ActionType<SearchResponse> expectedAction = SearchScrollAction.INSTANCE;
            for (int retry = 0; retry < randomIntBetween(minFailures, maxFailures); ++retry) {
                if (failSearch(client, expectedAction, seqNo)) {
                    if (expectedAction == SearchScrollAction.INSTANCE) {
                        handleClearScrollAction(scrollId, client);
                    }
                    expectedAction = SearchAction.INSTANCE;
                }
                client.awaitOperation();
                ++expectedSearchRetries;
            }

            if (expectedAction == SearchAction.INSTANCE) {
                validateSeqNoCondition(client, seqNo);
            }
            searchResponse = createSearchResponse(seqNoGenerator);
            client.respond(expectedAction, searchResponse);
            seqNo = extractSeqNoFromSearchResponse(searchResponse, seqNo);
            scrollId = searchResponse.getScrollId();
        }

        assertEquals(actualSearchRetries.get(), expectedSearchRetries);
    }

    private void handleResponse(BlockingQueue<ScrollableHitSource.AsyncResponse> responses, ScrollableHitSource.AsyncResponse response) {
        if (response.response().getFailures().isEmpty() == false) {
            throw (RuntimeException) response.response().getFailures().get(0).getReason();
        }

        responses.add(response);
    }

    /**
     * @return true if search is expected to restart, false if retry scroll.
     */
    private boolean failSearch(MockClient client, ActionType<SearchResponse> expectedAction, long seqNo) {
        Exception failure = randomFrom(new EsRejectedExecutionException(), new SearchFailureException());
        if (randomBoolean()) {
            client.fail(expectedAction, failure);
            return failure instanceof SearchFailureException;
        } else {
            LongSupplier seqNoGenerator = newSeqNoGenerator(seqNo);
            SearchResponse searchResponse =
                createSearchResponse(seqNoGenerator, new ShardSearchFailure[] { new ShardSearchFailure(failure) });
            client.respond(expectedAction, searchResponse);
            return true;
        }
    }

    private LongSupplier newSeqNoGenerator(long seqNo) {
        return new LongSupplier() {
                    private long sequence = seqNo;
                    @Override
                    public long getAsLong() {
                        return sequence += randomIntBetween(0, 3);
                    }
                };
    }

    public void testScrollExtraKeepAlive() {
        MockClient client = new MockClient(threadPool, logger);
        TaskId parentTask = new TaskId("thenode", randomInt());

        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(logger, BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool, Assert::fail, r -> fail(), e -> fail(), new ParentTaskAssigningClient(client,
            parentTask),
            // Set the base for the scroll to wait - this is added to the figure we calculate below
            new SearchRequest().scroll(timeValueSeconds(10)), false, null);

        hitSource.setScroll(generateScrollId());
        hitSource.startNextScroll(timeValueSeconds(100));
        client.validateRequest(SearchScrollAction.INSTANCE,
            (SearchScrollRequest r) -> assertEquals(r.scroll().keepAlive().seconds(), 110));
    }

    public void testRestartExtraKeepAlive() throws InterruptedException {
        MockClient client = new MockClient(threadPool, logger);
        TaskId parentTask = new TaskId("thenode", randomInt());

        AtomicInteger retries = new AtomicInteger();

        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(logger, BackoffPolicy.constantBackoff(TimeValue.ZERO, 1),
            threadPool, retries::incrementAndGet, r -> fail(), e -> fail(), new ParentTaskAssigningClient(client,
            parentTask),
            // Set the base for the scroll to wait - this is added to the figure we calculate below
            new SearchRequest().scroll(timeValueSeconds(10)), true, null);

        String scrollId = generateScrollId();
        hitSource.setScroll(scrollId);
        hitSource.startNextScroll(timeValueSeconds(100));
        client.fail(SearchScrollAction.INSTANCE, new Exception());
        handleClearScrollAction(scrollId, client);
        client.awaitOperation();
        client.validateRequest(SearchAction.INSTANCE,
            (SearchRequest r) -> assertEquals(r.scroll().keepAlive().seconds(), 110));

        assertEquals(1, retries.get());
    }

    private void handleClearScrollAction(String expectedScrollId, MockClient client) throws InterruptedException {
        client.awaitOperation();
        client.<ClearScrollRequest, ClearScrollResponse>validateRequest(ClearScrollAction.INSTANCE,
            request -> assertEquals(Collections.singletonList(expectedScrollId), request.scrollIds())
        );
        if (randomBoolean()) {
            client.respond(ClearScrollAction.INSTANCE, new ClearScrollResponse(true, 1));
        } else {
            client.fail(ClearScrollAction.INSTANCE, new Exception());
        }
    }

    private String generateScrollId() {
        return randomSimpleString(random(), 1, 10);
    }

    private SearchResponse createSearchResponse(LongSupplier seqNoGenerator) {
        return createSearchResponse(seqNoGenerator, null);
    }

    private SearchResponse createSearchResponse(LongSupplier seqNoGenerator, ShardSearchFailure[] shardFailures) {
        SearchHits hits = new SearchHits(IntStream.range(0, randomIntBetween(0, 20)).mapToObj(i -> createSearchHit(seqNoGenerator))
            .toArray(SearchHit[]::new),
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),0);
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, false, 1);
        return new SearchResponse(internalResponse, generateScrollId(), 5, 4, 0, randomLong(), shardFailures,
            SearchResponse.Clusters.EMPTY);
    }

    private SearchHit createSearchHit(LongSupplier seqNoGenerator) {
        SearchHit hit = new SearchHit(0, "id", emptyMap()).sourceRef(new BytesArray("{}"));
        hit.setSeqNo(seqNoGenerator.getAsLong());
        return hit;
    }


    private void validateSeqNoCondition(MockClient client, long seqNo) {
        client.<SearchRequest,SearchResponse>validateRequest(SearchAction.INSTANCE, request -> validateSeqNoCondition(request, seqNo));
    }

    private void validateSeqNoCondition(SearchRequest request, long seqNo) {
        if (request.source() != null && request.source().query() != null) {
            QueryBuilder query = request.source().query();
            if (query instanceof BoolQueryBuilder) {
                BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
                Optional<QueryBuilder> seqNoFilter = boolQuery.filter().stream()
                    .filter(q -> q instanceof RangeQueryBuilder && ((RangeQueryBuilder) q).fieldName().equals(SeqNoFieldMapper.NAME))
                    .findFirst();
                if (seqNoFilter.isPresent())
                    query = seqNoFilter.get();
            }
            if (query instanceof RangeQueryBuilder && ((RangeQueryBuilder) query).fieldName().equals(SeqNoFieldMapper.NAME)) {
                long requestSeqNo = (long) ((RangeQueryBuilder) query).from();
                assertEquals(requestSeqNo, seqNo);
                assertNotEquals(requestSeqNo, Long.MIN_VALUE);
                return;
            }
        }
        assertEquals(Long.MIN_VALUE, seqNo);
    }

    private long extractSeqNoFromSearchResponse(SearchResponse searchResponse, long seqNo) {
        if (searchResponse.getHits().getHits().length != 0) {
            seqNo = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length-1].getSeqNo();
        }
        return seqNo;
    }

    private void assertSameHits(List<? extends ScrollableHitSource.Hit> actual, SearchHit[] expected) {
        assertEquals(actual.size(), expected.length);
        for (int i = 0; i < actual.size(); ++i) {
            assertEquals(actual.get(i).getSource(), expected[i].getSourceRef());
            assertEquals(actual.get(i).getIndex(), expected[i].getIndex());
            assertEquals(actual.get(i).getVersion(), expected[i].getVersion());
            assertEquals(actual.get(i).getPrimaryTerm(), expected[i].getPrimaryTerm());
            assertEquals(actual.get(i).getSeqNo(), expected[i].getSeqNo());
            assertEquals(actual.get(i).getId(), expected[i].getId());
            assertEquals(actual.get(i).getIndex(), expected[i].getIndex());
        }
    }

    private static class ExpectedException extends RuntimeException {
        ExpectedException(Throwable cause) {
            super(cause);
        }
    }

    private static class SearchFailureException extends RuntimeException {
    }

    private static class ExecuteRequest<Request extends ActionRequest, Response extends ActionResponse> {
        private final ActionType<Response> action;
        private final Request request;
        private final ActionListener<Response> listener;
        private final Exception stackTrace;

        ExecuteRequest(ActionType<Response> action, Request request, ActionListener<Response> listener) {
            this.action = action;
            this.request = request;
            this.listener = listener;
            this.stackTrace = new Exception();
        }

        public void respond(ActionType<Response> action, Function<Request, Response> responseFunction, Logger logger) {
            validateAction(action);
            Response response = responseFunction.apply(request);
            logger.debug("Responding to request {} {}", action, response);
            listener.onResponse(response);
        }

        public void fail(ActionType<Response> action, Exception response) {
            validateAction(action);
            listener.onFailure(response);
        }

        public void validateRequest(ActionType<Response> action, Consumer<? super Request> validator) {
            validateAction(action);
            validator.accept(request);
        }

        private void validateAction(ActionType<Response> action) {
            if (action.equals(this.action) == false) {
                throw new AssertionError("Wrong action, actual: " + this.action +", expected: " + action + ", request: " + request,
                    stackTrace);
            }
            assertEquals(action, this.action);
        }

        @Override
        public String toString() {
            return "ExecuteRequest{" +
                "action=" + action +
                ", request=" + request +
                '}';
        }
    }

    private static class MockClient extends AbstractClient {
        private final Logger logger;
        private final BlockingQueue<ExecuteRequest<?,?>> requests = new ArrayBlockingQueue<>(100);

        MockClient(ThreadPool threadPool, Logger logger) {
            super(Settings.EMPTY, threadPool);
            this.logger = logger;
        }

        @Override
        protected synchronized  <Request extends ActionRequest, Response extends ActionResponse>
        void doExecute(ActionType<Response> action,
                       Request request, ActionListener<Response> listener) {
            logger.debug("Registering request {} {}", action, request);
            requests.add(new ExecuteRequest<>(action, request, listener));
            this.notifyAll();
        }

        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void respondx(ActionType<Response> action,
                                                                                              Function<Request, Response> response) {
            ((ExecuteRequest<Request, Response>) requests.remove()).respond(action, response, logger);
        }

        public <Response extends ActionResponse> void respond(ActionType<Response> action,
                                                              Response response) {
            respondx(action, req -> response);
        }

        @SuppressWarnings("unchecked")
        public <Response extends ActionResponse> void fail(ActionType<Response> action, Exception response) {
            logger.debug("Failing request {} {}", action, response);
            ((ExecuteRequest<?, Response>) requests.remove()).fail(action, response);
        }

        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void validateRequest(ActionType<Response> action,
                                                                                                     Consumer<? super Request> validator) {
            assert requests.peek() != null;
            ((ExecuteRequest<Request, Response>) requests.peek()).validateRequest(action, validator);
        }

        @Override
        public void close() {
        }

        public synchronized void awaitOperation() throws InterruptedException {
            if (requests.isEmpty()) {
                wait(10000);
                assertFalse("Must receive next request within 10s", requests.isEmpty());
            }
        }
    }
}
