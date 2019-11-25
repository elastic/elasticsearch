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

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
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

    private static class ExpectedException extends RuntimeException {
        ExpectedException(Throwable cause) {
            super(cause);
        }
    }

    public void testRetryFail() {
        int retries = randomInt(10);
        ExpectedException ex = expectThrows(ExpectedException.class, () -> {
            dotestBasicsWithRetry(retries, retries+1, retries+1, e -> { throw new ExpectedException(e); });
        });
        assertThat(ex.getCause(), instanceOf(EsRejectedExecutionException.class));
    }

    private void dotestBasicsWithRetry(int retries, int minFailures, int maxFailures,
                                       Consumer<Exception> failureHandler) throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(100);
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());
        AtomicInteger actualSearchRetries = new AtomicInteger();
        int expectedSearchRetries = 0;
        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(logger, BackoffPolicy.constantBackoff(TimeValue.ZERO, retries),
            threadPool, actualSearchRetries::incrementAndGet, responses::add, failureHandler,
            new ParentTaskAssigningClient(client, parentTask),
            new SearchRequest().scroll("1m"));

        hitSource.start();
        for (int retry = 0; retry < randomIntBetween(minFailures, maxFailures); ++retry) {
            client.fail(SearchAction.INSTANCE, new EsRejectedExecutionException());
            client.awaitOperation();
            ++expectedSearchRetries;
        }
        client.validateRequest(SearchAction.INSTANCE, (SearchRequest r) -> assertTrue(r.allowPartialSearchResults() == Boolean.FALSE));
        SearchResponse searchResponse = createSearchResponse();
        client.respond(SearchAction.INSTANCE, searchResponse);

        for (int i = 0; i < randomIntBetween(1, 10); ++i) {
            ScrollableHitSource.AsyncResponse asyncResponse = responses.poll(10, TimeUnit.SECONDS);
            assertNotNull(asyncResponse);
            assertEquals(responses.size(), 0);
            assertSameHits(asyncResponse.response().getHits(), searchResponse.getHits().getHits());
            asyncResponse.done(TimeValue.ZERO);

            for (int retry = 0; retry < randomIntBetween(minFailures, maxFailures); ++retry) {
                client.fail(SearchScrollAction.INSTANCE, new EsRejectedExecutionException());
                client.awaitOperation();
                ++expectedSearchRetries;
            }

            searchResponse = createSearchResponse();
            client.respond(SearchScrollAction.INSTANCE, searchResponse);
        }

        assertEquals(actualSearchRetries.get(), expectedSearchRetries);
    }

    public void testScrollKeepAlive() {
        MockClient client = new MockClient(threadPool);
        TaskId parentTask = new TaskId("thenode", randomInt());

        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(logger, BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool, () -> fail(), r -> fail(), e -> fail(), new ParentTaskAssigningClient(client,
            parentTask),
            // Set the base for the scroll to wait - this is added to the figure we calculate below
            new SearchRequest().scroll(timeValueSeconds(10)));

        hitSource.startNextScroll(timeValueSeconds(100));
        client.validateRequest(SearchScrollAction.INSTANCE,
            (SearchScrollRequest r) -> assertEquals(r.scroll().keepAlive().seconds(), 110));
    }



    private SearchResponse createSearchResponse() {
        // create a simulated response.
        SearchHit hit = new SearchHit(0, "id", emptyMap()).sourceRef(new BytesArray("{}"));
        SearchHits hits = new SearchHits(IntStream.range(0, randomIntBetween(0, 20)).mapToObj(i -> hit).toArray(SearchHit[]::new),
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),0);
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, false, 1);
        return new SearchResponse(internalResponse, randomSimpleString(random(), 1, 10), 5, 4, 0, randomLong(), null,
            SearchResponse.Clusters.EMPTY);
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

        public void respond(ActionType<Response> action, Function<Request, Response> response) {
            assertEquals(action, this.action);
            listener.onResponse(response.apply(request));
        }

        public void fail(ActionType<Response> action, Exception response) {
            assertEquals(action, this.action);
            listener.onFailure(response);
        }

        public void validateRequest(ActionType<Response> action, Consumer<? super Request> validator) {
            assertEquals(action, this.action);
            validator.accept(request);
        }
    }

    private static class MockClient extends AbstractClient {
        private ExecuteRequest<?,?> executeRequest;

        MockClient(ThreadPool threadPool) {
            super(Settings.EMPTY, threadPool);
        }

        @Override
        protected synchronized  <Request extends ActionRequest, Response extends ActionResponse>
        void doExecute(ActionType<Response> action,
                       Request request, ActionListener<Response> listener) {

            this.executeRequest = new ExecuteRequest<>(action, request, listener);
            this.notifyAll();
        }

        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void respondx(ActionType<Response> action,
                                                                                              Function<Request, Response> response) {
            ExecuteRequest<?, ?> executeRequest;
            synchronized (this) {
                executeRequest = this.executeRequest;
                this.executeRequest = null;
            }
            ((ExecuteRequest<Request, Response>) executeRequest).respond(action, response);
        }

        public <Response extends ActionResponse> void respond(ActionType<Response> action,
                                                              Response response) {
            respondx(action, req -> response);
        }

        @SuppressWarnings("unchecked")
        public <Response extends ActionResponse> void fail(ActionType<Response> action, Exception response) {
            ExecuteRequest<?, ?> executeRequest;
            synchronized (this) {
                executeRequest = this.executeRequest;
                this.executeRequest = null;
            }
            ((ExecuteRequest<?, Response>) executeRequest).fail(action, response);
        }

        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void validateRequest(ActionType<Response> action,
                                                                                                     Consumer<? super Request> validator) {
            ((ExecuteRequest<Request, Response>) executeRequest).validateRequest(action, validator);
        }

        @Override
        public void close() {
        }

        public synchronized void awaitOperation() throws InterruptedException {
            if (executeRequest == null) {
                wait(10000);
                assertNotNull("Must receive next request within 10s", executeRequest);
            }
        }
    }
}
