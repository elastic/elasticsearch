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

package org.elasticsearch.action;

import com.google.common.collect.Lists;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;

/**
 */
@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 2)
public class RejectionActionTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("threadpool.search.size", 1)
                .put("threadpool.search.queue_size", 1)
                .put("threadpool.index.size", 1)
                .put("threadpool.index.queue_size", 1)
                .put("threadpool.get.size", 1)
                .put("threadpool.get.queue_size", 1)
                .build();
    }


    @Test
    public void simulateSearchRejectionLoad() throws Throwable {
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "1").get();
        }

        int numberOfAsyncOps = randomIntBetween(200, 700);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final CopyOnWriteArrayList<Object> responses = Lists.newCopyOnWriteArrayList();
        for (int i = 0; i < numberOfAsyncOps; i++) {
            client().prepareSearch("test")
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field", "1"))
                    .execute(new ActionListener<SearchResponse>() {
                        @Override
                        public void onResponse(SearchResponse searchResponse) {
                            responses.add(searchResponse);
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            responses.add(e);
                            latch.countDown();
                        }
                    });
        }
        latch.await();
        assertThat(responses.size(), equalTo(numberOfAsyncOps));

        // validate all responses
        for (Object response : responses) {
            if (response instanceof SearchResponse) {
                SearchResponse searchResponse = (SearchResponse) response;
                for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                    assertTrue("got unexpected reason..." + failure.reason(), failure.reason().toLowerCase(Locale.ENGLISH).contains("rejected"));
                }
            } else {
                Throwable t = (Throwable) response;
                Throwable unwrap = ExceptionsHelper.unwrapCause(t);
                if (unwrap instanceof SearchPhaseExecutionException) {
                    SearchPhaseExecutionException e = (SearchPhaseExecutionException) unwrap;
                    for (ShardSearchFailure failure : e.shardFailures()) {
                        assertTrue("got unexpected reason..." + failure.reason(), failure.reason().toLowerCase(Locale.ENGLISH).contains("rejected"));
                    }
                } else if ((unwrap instanceof EsRejectedExecutionException) == false) {
                    throw new AssertionError("unexpected failure", (Throwable) response);
                }
            }
        }
    }
}
