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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class BulkProcessorRetryIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test";
    private static final String TYPE_NAME = "type";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        //Have very low pool and queue sizes to overwhelm internal pools easily
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // don't mess with this one! It's quite sensitive to a low queue size
                // (see also ThreadedActionListener which is happily spawning threads even when we already got rejected)
                //.put("thread_pool.listener.queue_size", 1)
                .put("thread_pool.get.queue_size", 1)
                // default is 50
                .put("thread_pool.bulk.queue_size", 30)
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
        final Set<Object> responses = Collections.newSetFromMap(new ConcurrentHashMap<>());

        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen();

        BulkProcessor bulkProcessor = BulkProcessor.builder(client(), new BulkProcessor.Listener() {
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
                responses.add(failure);
                latch.countDown();
            }
        }).setBulkActions(1)
                 // zero means that we're in the sync case, more means that we're in the async case
                .setConcurrentRequests(randomIntBetween(0, 100))
                .setBackoffPolicy(internalPolicy)
                .build();
        indexDocs(bulkProcessor, numberOfAsyncOps);
        latch.await(10, TimeUnit.SECONDS);
        bulkProcessor.close();

        assertThat(responses.size(), equalTo(numberOfAsyncOps));

        // validate all responses
        for (Object response : responses) {
            if (response instanceof BulkResponse) {
                BulkResponse bulkResponse = (BulkResponse) response;
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        Throwable rootCause = ExceptionsHelper.unwrapCause(failure.getCause());
                        if (rootCause instanceof EsRejectedExecutionException) {
                            if (rejectedExecutionExpected == false) {
                                Iterator<TimeValue> backoffState = internalPolicy.backoffStateFor(bulkResponse);
                                assertNotNull("backoffState is null (indicates a bulk request got rejected without retry)", backoffState);
                                if (backoffState.hasNext()) {
                                    // we're not expecting that we overwhelmed it even once when we maxed out the number of retries
                                    throw new AssertionError("Got rejected although backoff policy would allow more retries", rootCause);
                                } else {
                                    logger.debug("We maxed out the number of bulk retries and got rejected (this is ok).");
                                }
                            }
                        } else {
                            throw new AssertionError("Unexpected failure", rootCause);
                        }
                    }
                }
            } else {
                Throwable t = (Throwable) response;
                // we're not expecting any other errors
                throw new AssertionError("Unexpected failure", t);
            }
        }

        client().admin().indices().refresh(new RefreshRequest()).get();

        // validate we did not create any duplicates due to retries
        Matcher<Long> searchResultCount;
        // it is ok if we lost some index operations to rejected executions (which is possible even when backing off (although less likely)
        searchResultCount = lessThanOrEqualTo((long) numberOfAsyncOps);

        SearchResponse results = client()
                .prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0)
                .get();
        assertThat(results.getHits().totalHits(), searchResultCount);
    }

    private static void indexDocs(BulkProcessor processor, int numDocs) {
        for (int i = 1; i <= numDocs; i++) {
            processor.add(client()
                    .prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setType(TYPE_NAME)
                    .setId(Integer.toString(i))
                    .setSource("field", randomRealisticUnicodeOfLengthBetween(1, 30))
                    .request());
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
        private final Map<BulkResponse, Iterator<TimeValue>> correlations = new ConcurrentHashMap<>();
        // this is intentionally *not* static final. We will only ever have one instance of this class per test case and want the
        // thread local to be eligible for garbage collection right after the test to avoid leaks.
        private final ThreadLocal<Iterator<TimeValue>> iterators = new ThreadLocal<>();

        private final BackoffPolicy delegate;

        private CorrelatingBackoffPolicy(BackoffPolicy delegate) {
            this.delegate = delegate;
        }

        public Iterator<TimeValue> backoffStateFor(BulkResponse response) {
            return correlations.get(response);
        }

        // Assumption: This method is called from the same thread as the last call to the internal iterator's #hasNext() / #next()
        // see also Retry.AbstractRetryHandler#onResponse().
        public void logResponse(BulkResponse response) {
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
