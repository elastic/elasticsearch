/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.bulk;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class BulkProcessorRetryIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // Have very low pool and queue sizes to overwhelm internal pools easily
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // don't mess with this one! It's quite sensitive to a low queue size
            // (see also ThreadedActionListener which is happily spawning threads even when we already got rejected)
            // .put("thread_pool.listener.queue_size", 1)
            .put("thread_pool.get.queue_size", 1)
            // default is 200
            .put("thread_pool.write.queue_size", 30)
            .build();
    }

    public void testBulkRejectionLoadWithoutBackoff() throws Throwable {
        boolean rejectedExecutionExpected = true;
        executeBulkRejectionLoad(BackoffPolicy.noBackoff(), rejectedExecutionExpected);
    }

    public void testBulkRejectionLoadWithBackoff() throws Throwable {
        boolean rejectedExecutionExpected = false;
        executeBulkRejectionLoad(BackoffPolicy.exponentialBackoff(), rejectedExecutionExpected);
    }

    private void executeBulkRejectionLoad(BackoffPolicy backoffPolicy, boolean rejectedExecutionExpected) throws Throwable {
        final CorrelatingBackoffPolicy internalPolicy = new CorrelatingBackoffPolicy(backoffPolicy);
        int numberOfAsyncOps = randomIntBetween(600, 700);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final Set<Object> responses = ConcurrentCollections.newConcurrentSet();

        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen();

        BulkProcessor bulkProcessor = BulkProcessor.builder(client()::bulk, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                // no op
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
        }, "BulkProcssorRetryIT")
            .setBulkActions(1)
            // zero means that we're in the sync case, more means that we're in the async case
            .setConcurrentRequests(randomIntBetween(0, 100))
            .setBackoffPolicy(internalPolicy)
            .build();
        indexDocs(bulkProcessor, numberOfAsyncOps);
        latch.await(10, TimeUnit.SECONDS);
        bulkProcessor.close();

        assertThat(responses.size(), equalTo(numberOfAsyncOps));

        // validate all responses
        boolean rejectedAfterAllRetries = false;
        for (Object response : responses) {
            if (response instanceof BulkResponse bulkResponse) {
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                            if (rejectedExecutionExpected == false) {
                                assertRetriedCorrectly(internalPolicy, bulkResponse, failure.getCause());
                                rejectedAfterAllRetries = true;
                            }
                        } else {
                            throw new AssertionError("Unexpected failure status: " + failure.getStatus());
                        }
                    }
                }
            } else {
                if (ExceptionsHelper.status((Throwable) response) == RestStatus.TOO_MANY_REQUESTS) {
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

        indicesAdmin().refresh(new RefreshRequest()).get();

        final boolean finalRejectedAfterAllRetries = rejectedAfterAllRetries;
        assertResponse(prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(0), results -> {
            if (rejectedExecutionExpected) {
                assertThat((int) results.getHits().getTotalHits().value(), lessThanOrEqualTo(numberOfAsyncOps));
            } else if (finalRejectedAfterAllRetries) {
                assertThat((int) results.getHits().getTotalHits().value(), lessThan(numberOfAsyncOps));
            } else {
                assertThat((int) results.getHits().getTotalHits().value(), equalTo(numberOfAsyncOps));
            }
        });
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

    private static void indexDocs(BulkProcessor processor, int numDocs) {
        for (int i = 1; i <= numDocs; i++) {
            processor.add(
                prepareIndex(INDEX_NAME).setId(Integer.toString(i))
                    .setSource("field", randomRealisticUnicodeOfLengthBetween(1, 30))
                    .request()
            );
        }
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
