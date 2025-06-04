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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

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
public class BulkProcessor2RetryIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test";
    Map<String, Integer> requestToExecutionCountMap = new ConcurrentHashMap<>();

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
        executeBulkRejectionLoad(0, rejectedExecutionExpected);
    }

    // @TestLogging(
    // value = "org.elasticsearch.action.bulk.Retry2:trace",
    // reason = "Logging information about locks useful for tracking down deadlock"
    // )
    public void testBulkRejectionLoadWithBackoff() throws Throwable {
        boolean rejectedExecutionExpected = false;
        executeBulkRejectionLoad(8, rejectedExecutionExpected);
    }

    @SuppressWarnings("unchecked")
    private void executeBulkRejectionLoad(int maxRetries, boolean rejectedExecutionExpected) throws Throwable {
        int numberOfAsyncOps = randomIntBetween(600, 700);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final Set<BulkResponse> successfulResponses = ConcurrentCollections.newConcurrentSet();
        final Set<Tuple<BulkRequest, Throwable>> failedResponses = ConcurrentCollections.newConcurrentSet();

        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen();

        BulkProcessor2 bulkProcessor = BulkProcessor2.builder(this::countAndBulk, new BulkProcessor2.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                // no op
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                successfulResponses.add(response);
                latch.countDown();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                failedResponses.add(Tuple.tuple(request, failure));
                latch.countDown();
            }
        }, client().threadPool()).setBulkActions(1).setMaxNumberOfRetries(maxRetries).build();
        indexDocs(bulkProcessor, numberOfAsyncOps);
        latch.await(10, TimeUnit.SECONDS);
        bulkProcessor.awaitClose(1, TimeUnit.SECONDS);
        assertThat(successfulResponses.size() + failedResponses.size(), equalTo(numberOfAsyncOps));
        // validate all responses
        boolean rejectedAfterAllRetries = false;
        for (BulkResponse bulkResponse : successfulResponses) {
            for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                        if (rejectedExecutionExpected == false) {
                            int count = requestToExecutionCountMap.get(bulkItemResponse.getId());
                            if (count < maxRetries + 1) {
                                throw new AssertionError("Got rejected although backoff policy would allow more retries");
                            }
                            rejectedAfterAllRetries = true;
                        }
                    } else {
                        throw new AssertionError("Unexpected failure status: " + failure.getStatus());
                    }
                }
            }
        }
        for (Tuple<BulkRequest, Throwable> failureTuple : failedResponses) {
            if (ExceptionsHelper.status(failureTuple.v2()) == RestStatus.TOO_MANY_REQUESTS) {
                if (rejectedExecutionExpected == false) {
                    for (DocWriteRequest<?> request : failureTuple.v1().requests) {
                        int count = requestToExecutionCountMap.get(request.id());
                        if (count < maxRetries + 1) {
                            throw new AssertionError("Got rejected although backoff policy would allow more retries");
                        }
                    }
                    rejectedAfterAllRetries = true;
                }
                // ignored, we exceeded the write queue size when dispatching the initial bulk request
            } else {
                Throwable t = failureTuple.v2();
                // we're not expecting any other errors
                throw new AssertionError("Unexpected failure", t);
            }
        }

        indicesAdmin().refresh(new RefreshRequest()).get();

        final boolean finalRejectedAfterAllRetries = rejectedAfterAllRetries;
        assertResponse(prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(0), results -> {
            assertThat(bulkProcessor.getTotalBytesInFlight(), equalTo(0L));
            if (rejectedExecutionExpected) {
                assertThat((int) results.getHits().getTotalHits().value(), lessThanOrEqualTo(numberOfAsyncOps));
            } else if (finalRejectedAfterAllRetries) {
                assertThat((int) results.getHits().getTotalHits().value(), lessThan(numberOfAsyncOps));
            } else {
                assertThat((int) results.getHits().getTotalHits().value(), equalTo(numberOfAsyncOps));
            }
        });
    }

    private static void indexDocs(BulkProcessor2 processor, int numDocs) {
        for (int i = 1; i <= numDocs; i++) {
            processor.add(
                prepareIndex(INDEX_NAME).setId(Integer.toString(i))
                    .setSource("field", randomRealisticUnicodeOfLengthBetween(1, 30))
                    .request()
            );
        }
    }

    void countAndBulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        for (DocWriteRequest<?> docWriteRequest : request.requests) {
            requestToExecutionCountMap.compute(docWriteRequest.id(), (key, value) -> value == null ? 1 : value + 1);
        }
        client().bulk(request, listener);
    }

}
