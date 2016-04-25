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

package org.elasticsearch.index;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

/**
 * Tests that requests with block_until_refresh set to true will be visible when they return. 
 */
public class BlockUntilRefreshIT extends ESIntegTestCase {
    public void testIndex() {
        IndexResponse index = client().prepareIndex("test", "index", "1").setSource("foo", "bar").setBlockUntilRefresh(true).get();
        assertEquals(RestStatus.CREATED, index.status());
        assertFalse("request shouldn't have forced a refresh", index.forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");
        // TODO update!!!
    }

    public void testDelete() throws InterruptedException, ExecutionException {
        // Index normally
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "bar"));
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Now delete with blockUntilRefresh
        DeleteResponse delete = client().prepareDelete("test", "test", "1").setBlockUntilRefresh(true).get();
        assertTrue("document was deleted", delete.isFound());
        assertFalse("request shouldn't have forced a refresh", delete.forcedRefresh());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get());
    }

    public void testBulk() {
        // Index by bulk with block_until_refresh
        BulkRequestBuilder bulk = client().prepareBulk().setBlockUntilRefresh(true);
        bulk.add(client().prepareIndex("test", "index", "1").setSource("foo", "bar"));
        assertBulkSuccess(bulk.get());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Update by bulk with block_until_refresh
        bulk = client().prepareBulk().setBlockUntilRefresh(true);
        bulk.add(client().prepareUpdate("test", "index", "1").setDoc("foo", "baz"));
        assertBulkSuccess(bulk.get());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "baz")).get(), "1");

        // Update by bulk with block_until_refresh
        bulk = client().prepareBulk().setBlockUntilRefresh(true);
        bulk.add(client().prepareDelete("test", "index", "1"));
        assertBulkSuccess(bulk.get());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get());
    }

    private void assertBulkSuccess(BulkResponse response) {
        assertNoFailures(response);
        for (BulkItemResponse item : response) {
            assertFalse("request shouldn't have forced a refresh", item.getResponse().forcedRefresh());
        }
    }
}
