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
package org.elasticsearch.client;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BulkProcessorRetryIT extends ESRestHighLevelClientTestCase {

    private static final String INDEX_NAME = "index";

    private static BulkProcessor.Builder initBulkProcessorBuilder(BulkProcessor.Listener listener) {
        return BulkProcessor.builder(
                (request, bulkListener) -> highLevelClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
    }

    public void testBulkRejectionLoadWithoutBackoff() throws Exception {
        boolean rejectedExecutionExpected = true;
        executeBulkRejectionLoad(BackoffPolicy.noBackoff(), rejectedExecutionExpected);
    }

    public void testBulkRejectionLoadWithBackoff() throws Throwable {
        boolean rejectedExecutionExpected = false;
        executeBulkRejectionLoad(BackoffPolicy.exponentialBackoff(), rejectedExecutionExpected);
    }

    private void executeBulkRejectionLoad(BackoffPolicy backoffPolicy, boolean rejectedExecutionExpected) throws Exception {
        final CorrelatingBackoffPolicy internalPolicy = new CorrelatingBackoffPolicy(backoffPolicy);
        final int numberOfAsyncOps = randomIntBetween(600, 700);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final Set<Object> responses = Collections.newSetFromMap(new ConcurrentHashMap<>());

        BulkProcessor bulkProcessor = initBulkProcessorBuilder(new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                internalPolicy.logResponse(response);
                responses.add(response);
                latch.countDown();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                internalPolicy.logResponse(failure);
                responses.add(failure);
                latch.countDown();
            }
        }).setBulkActions(1)
            .setConcurrentRequests(randomIntBetween(0, 100))
            .setBackoffPolicy(internalPolicy)
            .build();

        MultiGetRequest multiGetRequest = indexDocs(bulkProcessor, numberOfAsyncOps);
        latch.await(10, TimeUnit.SECONDS);
        bulkProcessor.close();

        assertEquals(responses.size(), numberOfAsyncOps);

        boolean rejectedAfterAllRetries = false;
        for (Object response : responses) {
            if (response instanceof BulkResponse) {
                BulkResponse bulkResponse = (BulkResponse) response;
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                            if (rejectedExecutionExpected == false) {
                                assertRetriedCorrectly(internalPolicy, bulkResponse, failure.getCause());
                                rejectedAfterAllRetries = true;
                            }
                        } else {
                            throw new AssertionError("Unexpected failure with status: " + failure.getStatus());
                        }
                    }
                }
            } else {
                if (response instanceof RemoteTransportException
                    && ((RemoteTransportException) response).status() == RestStatus.TOO_MANY_REQUESTS) {
                    if (rejectedExecutionExpected == false) {
                        assertRetriedCorrectly(internalPolicy, response, ((Throwable) response).getCause());
                        rejectedAfterAllRetries = true;
                    }
                    // ignored, we exceeded the write queue size when dispatching the initial bulk request
                } else {
                    Throwable t = (Throwable) response;
                    // we're not expecting any other errors
                    throw new AssertionError("Unexpected failure", t);
                }
            }
        }

        highLevelClient().indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
        int multiGetResponsesCount = highLevelClient().mget(multiGetRequest, RequestOptions.DEFAULT).getResponses().length;

        if (rejectedExecutionExpected) {
            assertThat(multiGetResponsesCount, lessThanOrEqualTo(numberOfAsyncOps));
        } else if (rejectedAfterAllRetries) {
            assertThat(multiGetResponsesCount, lessThan(numberOfAsyncOps));
        } else {
            assertThat(multiGetResponsesCount, equalTo(numberOfAsyncOps));
        }

    }

    private void assertRetriedCorrectly(CorrelatingBackoffPolicy internalPolicy, Object bulkResponse, Throwable failure) {
        Iterator<TimeValue> backoffState = internalPolicy.backoffStateFor(bulkResponse);
        assertNotNull("backoffState is null (indicates a bulk request got rejected without retry)", backoffState);
        if (backoffState.hasNext()) {
            // we're not expecting that we overwhelmed it even once when we maxed out the number of retries
            throw new AssertionError("Got rejected although backoff policy would allow more retries", failure);
        } else {
            logger.debug("We maxed out the number of bulk retries and got rejected (this is ok).");
        }
    }

    private static MultiGetRequest indexDocs(BulkProcessor processor, int numDocs) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (int i = 1; i <= numDocs; i++) {
            processor.add(new IndexRequest(INDEX_NAME).id(Integer.toString(i))
                .source(XContentType.JSON, "field", randomRealisticUnicodeOfCodepointLengthBetween(1, 30)));
            multiGetRequest.add(INDEX_NAME, Integer.toString(i));
        }
        return multiGetRequest;
    }

    /**
     * Internal helper class to correlate backoff states with bulk responses. This is needed to check whether we maxed out the number
     * of retries but still got rejected (which is perfectly fine and can also happen from time to time under heavy load).
     *
     * This implementation relies on an implementation detail in Retry, namely that the bulk listener is notified on the same thread
     * as the last call to the backoff policy's iterator. The advantage is that this is non-invasive to the rest of the production code.
     */
    private static class CorrelatingBackoffPolicy extends BackoffPolicy {
        private final Map<Object, Iterator<TimeValue>> correlations = new ConcurrentHashMap<>();
        // this is intentionally *not* static final. We will only ever have one instance of this class per test case and want the
        // thread local to be eligible for garbage collection right after the test to avoid leaks.
        private final ThreadLocal<Iterator<TimeValue>> iterators = new ThreadLocal<>();

        private final BackoffPolicy delegate;

        private CorrelatingBackoffPolicy(BackoffPolicy delegate) {
            this.delegate = delegate;
        }

        public Iterator<TimeValue> backoffStateFor(Object response) {
            return correlations.get(response);
        }

        // Assumption: This method is called from the same thread as the last call to the internal iterator's #hasNext() / #next()
        // see also Retry.AbstractRetryHandler#onResponse().
        public void logResponse(Object response) {
            Iterator<TimeValue> iterator = iterators.get();
            // did we ever retry?
            if (iterator != null) {
                // we should correlate any iterator only once
                iterators.remove();
                correlations.put(response, iterator);
            }
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new CorrelatingIterator(iterators, delegate.iterator());
        }

        private static class CorrelatingIterator implements Iterator<TimeValue> {
            private final Iterator<TimeValue> delegate;
            private final ThreadLocal<Iterator<TimeValue>> iterators;

            private CorrelatingIterator(ThreadLocal<Iterator<TimeValue>> iterators, Iterator<TimeValue> delegate) {
                this.iterators = iterators;
                this.delegate = delegate;
            }

            @Override
            public boolean hasNext() {
                // update on every invocation as we might get rescheduled on a different thread. Unfortunately, there is a chance that
                // we pollute the thread local map with stale values. Due to the implementation of Retry and the life cycle of the
                // enclosing class CorrelatingBackoffPolicy this should not pose a major problem though.
                iterators.set(this);
                return delegate.hasNext();
            }

            @Override
            public TimeValue next() {
                // update on every invocation
                iterators.set(this);
                return delegate.next();
            }
        }
    }
}
