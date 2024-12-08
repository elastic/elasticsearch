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
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ClientScrollableHitSource;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
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

    public void testRetryFail() throws InterruptedException {
        final int retries = randomInt(10);
        final var exceptionRef = new AtomicReference<Exception>();
        dotestBasicsWithRetry(retries, retries + 1, retries + 1, exceptionRef::set);
        assertThat(exceptionRef.get(), instanceOf(EsRejectedExecutionException.class));
    }

    private void dotestBasicsWithRetry(int retries, int minFailures, int maxFailures, Consumer<Exception> failureHandler)
        throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());
        AtomicInteger actualSearchRetries = new AtomicInteger();
        int expectedSearchRetries = 0;
        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, retries),
            threadPool,
            actualSearchRetries::incrementAndGet,
            responses::add,
            failureHandler,
            new ParentTaskAssigningClient(client, parentTask),
            new SearchRequest().scroll(TimeValue.timeValueMinutes(1))
        );

        hitSource.start();
        for (int retry = 0; retry < randomIntBetween(minFailures, maxFailures); ++retry) {
            client.fail(TransportSearchAction.TYPE, new EsRejectedExecutionException());
            if (retry >= retries) {
                return;
            }
            client.awaitOperation();
            ++expectedSearchRetries;
        }
        client.validateRequest(TransportSearchAction.TYPE, (SearchRequest r) -> assertTrue(r.allowPartialSearchResults() == Boolean.FALSE));
        SearchResponse searchResponse = createSearchResponse();
        try {
            client.respond(TransportSearchAction.TYPE, searchResponse);

            for (int i = 0; i < randomIntBetween(1, 10); ++i) {
                ScrollableHitSource.AsyncResponse asyncResponse = responses.poll(10, TimeUnit.SECONDS);
                assertNotNull(asyncResponse);
                assertEquals(responses.size(), 0);
                assertSameHits(asyncResponse.response().getHits(), searchResponse.getHits().getHits());
                asyncResponse.done(TimeValue.ZERO);

                for (int retry = 0; retry < randomIntBetween(minFailures, maxFailures); ++retry) {
                    client.fail(TransportSearchScrollAction.TYPE, new EsRejectedExecutionException());
                    client.awaitOperation();
                    ++expectedSearchRetries;
                }

                searchResponse.decRef();
                searchResponse = createSearchResponse();
                client.respond(TransportSearchScrollAction.TYPE, searchResponse);
            }

            assertEquals(actualSearchRetries.get(), expectedSearchRetries);
        } finally {
            searchResponse.decRef();
        }
    }

    public void testScrollKeepAlive() {
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());

        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            () -> fail(),
            r -> fail(),
            e -> fail(),
            new ParentTaskAssigningClient(client, parentTask),
            // Set the base for the scroll to wait - this is added to the figure we calculate below
            new SearchRequest().scroll(timeValueSeconds(10))
        );

        hitSource.startNextScroll(timeValueSeconds(100));
        client.validateRequest(
            TransportSearchScrollAction.TYPE,
            (SearchScrollRequest r) -> assertEquals(r.scroll().keepAlive().seconds(), 110)
        );
    }

    private SearchResponse createSearchResponse() {
        // create a simulated response.
        SearchHit hit = SearchHit.unpooled(0, "id").sourceRef(new BytesArray("{}"));
        SearchHits hits = SearchHits.unpooled(
            IntStream.range(0, randomIntBetween(0, 20)).mapToObj(i -> hit).toArray(SearchHit[]::new),
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            0
        );
        return new SearchResponse(
            hits,
            null,
            null,
            false,
            false,
            null,
            1,
            randomSimpleString(random(), 1, 10),
            5,
            4,
            0,
            randomLong(),
            null,
            SearchResponse.Clusters.EMPTY
        );
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
            super(Settings.EMPTY, threadPool);
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
