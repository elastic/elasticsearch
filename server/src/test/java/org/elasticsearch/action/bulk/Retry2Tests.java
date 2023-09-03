/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class Retry2Tests extends ESTestCase {
    private static final int CALLS_TO_FAIL = 5;

    private MockBulkClient bulkClient;
    /**
     * Headers that are expected to be sent with all bulk requests.
     */
    private Map<String, String> expectedHeaders = new HashMap<>();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.bulkClient = new MockBulkClient(getTestName(), CALLS_TO_FAIL);
        // Stash some random headers so we can assert that we preserve them
        bulkClient.threadPool().getThreadContext().stashContext();
        expectedHeaders.clear();
        expectedHeaders.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        bulkClient.threadPool().getThreadContext().putHeader(expectedHeaders);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        this.bulkClient.close();
    }

    private BulkRequest createBulkRequest() {
        BulkRequest request = new BulkRequest();
        request.add(new UpdateRequest("shop", "1").doc(Map.of(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))));
        request.add(new UpdateRequest("shop", "2").doc(Map.of(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))));
        request.add(new UpdateRequest("shop", "3").doc(Map.of(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))));
        request.add(new UpdateRequest("shop", "4").doc(Map.of(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))));
        request.add(new UpdateRequest("shop", "5").doc(Map.of(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))));
        return request;
    }

    public void testRetryBacksOff() throws Exception {
        BulkRequest bulkRequest = createBulkRequest();
        Retry2 retry2 = new Retry2(CALLS_TO_FAIL);
        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        retry2.consumeRequestWithRetries(bulkClient::bulk, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertFalse(response.hasFailures());
        assertThat(response.getItems().length, equalTo(bulkRequest.numberOfActions()));
    }

    public void testRetryFailsAfterRetry() throws Exception {
        BulkRequest bulkRequest = createBulkRequest();
        try {
            Retry2 retry2 = new Retry2(CALLS_TO_FAIL - 1);
            PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
            retry2.consumeRequestWithRetries(bulkClient::bulk, bulkRequest, future);
            BulkResponse response = future.actionGet();
            /*
            * If the last failure was an item failure we'll end up here
            */
            assertTrue(response.hasFailures());
            assertThat(response.getItems().length, equalTo(bulkRequest.numberOfActions()));
        } catch (EsRejectedExecutionException e) {
            /*
             * If the last failure was a rejection we'll end up here.
             */
            assertThat(e.getMessage(), equalTo("pretend the coordinating thread pool is stuffed"));
        }

    }

    public void testRetryWithListener() throws Exception {
        AssertingListener listener = new AssertingListener();
        BulkRequest bulkRequest = createBulkRequest();
        Retry2 retry = new Retry2(CALLS_TO_FAIL);
        retry.consumeRequestWithRetries(bulkClient::bulk, bulkRequest, listener);

        listener.awaitCallbacksCalled();
        listener.assertOnResponseCalled();
        listener.assertResponseWithoutFailures();
        listener.assertResponseWithNumberOfItems(bulkRequest.numberOfActions());
        listener.assertOnFailureNeverCalled();
    }

    public void testRetryWithListenerFailsAfterBacksOff() throws Exception {
        AssertingListener listener = new AssertingListener();
        BulkRequest bulkRequest = createBulkRequest();
        Retry2 retry = new Retry2(CALLS_TO_FAIL - 1);
        retry.consumeRequestWithRetries(bulkClient::bulk, bulkRequest, listener);

        listener.awaitCallbacksCalled();

        if (listener.lastFailure == null) {
            /*
             * If the last failure was an item failure we'll end up here.
             */
            listener.assertOnResponseCalled();
            listener.assertResponseWithFailures();
            listener.assertResponseWithNumberOfItems(bulkRequest.numberOfActions());
        } else {
            /*
             * If the last failure was a rejection we'll end up here.
             */
            assertThat(listener.lastFailure, instanceOf(EsRejectedExecutionException.class));
            assertThat(listener.lastFailure.getMessage(), equalTo("pretend the coordinating thread pool is stuffed"));
        }
    }

    public void testAwaitClose() throws Exception {
        /*
         * awaitClose() is called immediately, and we make sure that subsequent requests are rejected.
         */
        {
            Retry2 retry = new Retry2(CALLS_TO_FAIL);
            retry.awaitClose(200, TimeUnit.MILLISECONDS);
            AssertingListener listener = new AssertingListener();
            BulkRequest bulkRequest = createBulkRequest();
            retry.consumeRequestWithRetries(bulkClient::bulk, bulkRequest, listener);
            listener.awaitCallbacksCalled();
            assertNotNull(listener.lastFailure);
            assertThat(listener.lastFailure, instanceOf(EsRejectedExecutionException.class));
        }
        /*
         * awaitClose() returns without exception if all requests complete quickly, whether they were successes or failures
         */
        {
            Retry2 retry = new Retry2(CALLS_TO_FAIL);
            List<AssertingListener> listeners = new ArrayList<>();
            BulkRequest bulkRequest = createBulkRequest();
            for (int i = 0; i < randomIntBetween(1, 100); i++) {
                AssertingListener listener = new AssertingListener();
                listeners.add(listener);
                retry.consumeRequestWithRetries((bulkRequest1, listener1) -> {
                    if (randomBoolean()) {
                        listener1.onResponse(new BulkResponse(new BulkItemResponse[0], 5));
                    } else {
                        listener1.onFailure(new RuntimeException("Some failure"));
                    }
                }, bulkRequest, listener);
            }
            retry.awaitClose(1, TimeUnit.SECONDS);
        }
        /*
         * Most requests are complete but one request is hung so awaitClose ought to wait the full timeout period
         */
        {
            Retry2 retry = new Retry2(CALLS_TO_FAIL);
            List<AssertingListener> listeners = new ArrayList<>();
            BulkRequest bulkRequest = createBulkRequest();
            for (int i = 0; i < randomIntBetween(0, 100); i++) {
                AssertingListener listener = new AssertingListener();
                listeners.add(listener);
                retry.consumeRequestWithRetries((bulkRequest1, listener1) -> {
                    if (randomBoolean()) {
                        listener1.onResponse(new BulkResponse(new BulkItemResponse[0], 5));
                    } else {
                        listener1.onFailure(new RuntimeException("Some failure"));
                    }
                }, bulkRequest, listener);
            }
            for (AssertingListener listener : listeners) {
                listener.awaitCallbacksCalled();
            }
            AssertingListener listener = new AssertingListener();
            retry.consumeRequestWithRetries((bulkRequest1, listener1) -> {
                // never calls onResponse or onFailure
            }, bulkRequest, listener);
            long waitTimeMillis = randomLongBetween(20, 200);
            // Make sure that awaitClose completes without exception, and takes at least waitTimeMillis
            long startTimeNanos = System.nanoTime();
            retry.awaitClose(waitTimeMillis, TimeUnit.MILLISECONDS);
            long stopTimeNanos = System.nanoTime();
            long runtimeMillis = TimeValue.timeValueNanos((stopTimeNanos - startTimeNanos)).millis();
            assertThat(runtimeMillis, greaterThanOrEqualTo(waitTimeMillis));
            // A sanity check that it didn't take an extremely long time to complete:
            assertThat(runtimeMillis, lessThanOrEqualTo(TimeValue.timeValueSeconds(1).millis()));
        }
    }

    private static class AssertingListener implements ActionListener<BulkResponse> {
        private final CountDownLatch latch;
        private final AtomicInteger countOnResponseCalled = new AtomicInteger();
        private volatile Throwable lastFailure;
        private volatile BulkResponse response;

        private AssertingListener() {
            latch = new CountDownLatch(1);
        }

        public void awaitCallbacksCalled() throws InterruptedException {
            latch.await();
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            this.response = bulkItemResponses;
            countOnResponseCalled.incrementAndGet();
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            this.lastFailure = e;
            latch.countDown();
        }

        public void assertOnResponseCalled() {
            assertThat(countOnResponseCalled.get(), equalTo(1));
        }

        public void assertResponseWithNumberOfItems(int numItems) {
            assertThat(response.getItems().length, equalTo(numItems));
        }

        public void assertResponseWithoutFailures() {
            assertThat(response, notNullValue());
            assertFalse("Response should not have failures", response.hasFailures());
        }

        public void assertResponseWithFailures() {
            assertThat(response, notNullValue());
            assertTrue("Response should have failures", response.hasFailures());
        }

        public void assertOnFailureNeverCalled() {
            assertThat(lastFailure, nullValue());
        }
    }

    private class MockBulkClient extends NoOpClient {
        private int numberOfCallsToFail;

        private MockBulkClient(String testName, int numberOfCallsToFail) {
            super(testName);
            this.numberOfCallsToFail = numberOfCallsToFail;
        }

        @Override
        public ActionFuture<BulkResponse> bulk(BulkRequest request) {
            PlainActionFuture<BulkResponse> responseFuture = new PlainActionFuture<>();
            bulk(request, responseFuture);
            return responseFuture;
        }

        @Override
        public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
            if (false == expectedHeaders.equals(threadPool().getThreadContext().getHeaders())) {
                listener.onFailure(
                    new RuntimeException("Expected " + expectedHeaders + " but got " + threadPool().getThreadContext().getHeaders())
                );
                return;
            }

            // do everything synchronously, that's fine for a test
            boolean shouldFail = numberOfCallsToFail > 0;
            numberOfCallsToFail--;

            if (shouldFail && randomBoolean()) {
                listener.onFailure(new EsRejectedExecutionException("pretend the coordinating thread pool is stuffed"));
                return;
            }

            BulkItemResponse[] itemResponses = new BulkItemResponse[request.requests().size()];
            // if we have to fail, we need to fail at least once "reliably", the rest can be random
            int itemToFail = randomInt(request.requests().size() - 1);
            for (int idx = 0; idx < request.requests().size(); idx++) {
                if (shouldFail && (randomBoolean() || idx == itemToFail)) {
                    itemResponses[idx] = failedResponse();
                } else {
                    itemResponses[idx] = successfulResponse();
                }
            }
            listener.onResponse(new BulkResponse(itemResponses, 1000L));
        }

        private BulkItemResponse successfulResponse() {
            return BulkItemResponse.success(1, OpType.DELETE, new DeleteResponse(new ShardId("test", "test", 0), "test", 0, 0, 0, false));
        }

        private BulkItemResponse failedResponse() {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure("test", "1", new EsRejectedExecutionException("pool full"));
            return BulkItemResponse.failure(1, OpType.INDEX, failure);
        }
    }
}
