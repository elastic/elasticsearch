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
package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RetryTests extends ESTestCase {
    // no need to wait fof a long time in tests
    private static final TimeValue DELAY = TimeValue.timeValueMillis(1L);
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
        request.add(new UpdateRequest("shop", "products", "1"));
        request.add(new UpdateRequest("shop", "products", "2"));
        request.add(new UpdateRequest("shop", "products", "3"));
        request.add(new UpdateRequest("shop", "products", "4"));
        request.add(new UpdateRequest("shop", "products", "5"));
        return request;
    }

    public void testRetryBacksOff() throws Exception {
        BackoffPolicy backoff = BackoffPolicy.constantBackoff(DELAY, CALLS_TO_FAIL);

        BulkRequest bulkRequest = createBulkRequest();
        BulkResponse response = new Retry(EsRejectedExecutionException.class, backoff, bulkClient.threadPool())
            .withBackoff(bulkClient::bulk, bulkRequest, bulkClient.settings())
            .actionGet();

        assertFalse(response.hasFailures());
        assertThat(response.getItems().length, equalTo(bulkRequest.numberOfActions()));
    }

    public void testRetryFailsAfterBackoff() throws Exception {
        BackoffPolicy backoff = BackoffPolicy.constantBackoff(DELAY, CALLS_TO_FAIL - 1);

        BulkRequest bulkRequest = createBulkRequest();
        BulkResponse response = new Retry(EsRejectedExecutionException.class, backoff, bulkClient.threadPool())
            .withBackoff(bulkClient::bulk, bulkRequest, bulkClient.settings())
            .actionGet();

        assertTrue(response.hasFailures());
        assertThat(response.getItems().length, equalTo(bulkRequest.numberOfActions()));
    }

    public void testRetryWithListenerBacksOff() throws Exception {
        BackoffPolicy backoff = BackoffPolicy.constantBackoff(DELAY, CALLS_TO_FAIL);
        AssertingListener listener = new AssertingListener();

        BulkRequest bulkRequest = createBulkRequest();
        Retry retry = new Retry(EsRejectedExecutionException.class, backoff, bulkClient.threadPool());
        retry.withBackoff(bulkClient::bulk, bulkRequest, listener, bulkClient.settings());

        listener.awaitCallbacksCalled();
        listener.assertOnResponseCalled();
        listener.assertResponseWithoutFailures();
        listener.assertResponseWithNumberOfItems(bulkRequest.numberOfActions());
        listener.assertOnFailureNeverCalled();
    }

    public void testRetryWithListenerFailsAfterBacksOff() throws Exception {
        BackoffPolicy backoff = BackoffPolicy.constantBackoff(DELAY, CALLS_TO_FAIL - 1);
        AssertingListener listener = new AssertingListener();

        BulkRequest bulkRequest = createBulkRequest();
        Retry retry = new Retry(EsRejectedExecutionException.class, backoff, bulkClient.threadPool());
        retry.withBackoff(bulkClient::bulk, bulkRequest, listener, bulkClient.settings());

        listener.awaitCallbacksCalled();

        listener.assertOnResponseCalled();
        listener.assertResponseWithFailures();
        listener.assertResponseWithNumberOfItems(bulkRequest.numberOfActions());
        listener.assertOnFailureNeverCalled();
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
                        new RuntimeException("Expected " + expectedHeaders + " but got " + threadPool().getThreadContext().getHeaders()));
                return;
            }

            // do everything synchronously, that's fine for a test
            boolean shouldFail = numberOfCallsToFail > 0;
            numberOfCallsToFail--;

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
            return new BulkItemResponse(1, OpType.DELETE, new DeleteResponse());
        }

        private BulkItemResponse failedResponse() {
            return new BulkItemResponse(1, OpType.INDEX, new BulkItemResponse.Failure("test", "test", "1", new EsRejectedExecutionException("pool full")));
        }
    }
}
