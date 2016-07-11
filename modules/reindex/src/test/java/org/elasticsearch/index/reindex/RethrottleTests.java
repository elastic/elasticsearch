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

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;

import static org.hamcrest.Matchers.hasSize;

/**
 * Tests that you can set requests_per_second over the Java API and that you can rethrottle running requests. There are REST tests for this
 * too but this is the only place that tests running against multiple nodes so it is the only integration tests that checks for
 * serialization.
 */
public class RethrottleTests extends ReindexTestCase {

    public void testReindex() throws Exception {
        testCase(reindex().source("test").destination("dest"), ReindexAction.NAME);
    }

    public void testUpdateByQuery() throws Exception {
        testCase(updateByQuery().source("test"), UpdateByQueryAction.NAME);
    }

    public void testDeleteByQuery() throws Exception {
        testCase(deleteByQuery().source("test"), DeleteByQueryAction.NAME);
    }

    private void testCase(AbstractBulkByScrollRequestBuilder<?, ?> request, String actionName)
            throws Exception {
        // Use a single shard so the reindex has to happen in multiple batches
        client().admin().indices().prepareCreate("test").setSettings("index.number_of_shards", 1).get();
        indexRandom(true,
                client().prepareIndex("test", "test", "1").setSource("foo", "bar"),
                client().prepareIndex("test", "test", "2").setSource("foo", "bar"),
                client().prepareIndex("test", "test", "3").setSource("foo", "bar"));

        // Start a request that will never finish unless we rethrottle it
        request.setRequestsPerSecond(.000001f);  // Throttle "forever"
        request.source().setSize(1);             // Make sure we use multiple batches
        ListenableActionFuture<? extends BulkIndexByScrollResponse> responseListener = request.execute();

        // Now rethrottle it so it'll finish
        ListTasksResponse rethrottleResponse = rethrottle().setActions(actionName).setRequestsPerSecond(Float.POSITIVE_INFINITY).get();
        assertThat(rethrottleResponse.getTasks(), hasSize(1));
        BulkByScrollTask.Status status = (BulkByScrollTask.Status) rethrottleResponse.getTasks().get(0).getStatus();
        assertEquals(Float.POSITIVE_INFINITY, status.getRequestsPerSecond(), Float.MIN_NORMAL);

        // Now the response should come back quickly because we've rethrottled the request
        BulkIndexByScrollResponse response = responseListener.get();
        assertEquals("Batches didn't match, this may invalidate the test as throttling is done between batches", 3, response.getBatches());
    }
}
