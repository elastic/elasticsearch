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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collections;
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
                .put("threadpool.generic.size", 1)
                .put("threadpool.generic.queue_size", 1)
                // don't mess with this one! It's quite sensitive to a low queue size
                // (see also ThreadedActionListener which is happily spawning threads even when we already got rejected)
                //.put("threadpool.listener.queue_size", 1)
                .put("threadpool.get.queue_size", 1)
                // default is 50
                .put("threadpool.bulk.queue_size", 30)
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
                .setBackoffPolicy(backoffPolicy)
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
                                // we're not expecting that we overwhelmed it even once
                                throw new AssertionError("Unexpected failure reason", rootCause);
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
        if (rejectedExecutionExpected) {
            // it is ok if we lost some index operations to rejected executions
            searchResultCount = lessThanOrEqualTo((long) numberOfAsyncOps);
        } else {
            searchResultCount = equalTo((long) numberOfAsyncOps);
        }

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
}
