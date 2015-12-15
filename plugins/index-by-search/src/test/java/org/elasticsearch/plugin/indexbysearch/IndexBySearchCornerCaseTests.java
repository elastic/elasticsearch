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

package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

import org.elasticsearch.index.get.GetField;

/**
 * Index-by-search test for ttl, timestamp, and routing.
 */
public class IndexBySearchCornerCaseTests extends IndexBySearchTestCase {
    /**
     * Creates two indexes with the provided mapping, indexes a single doc into
     * them and copies the data from one to the other.
     */
    private void copyDoc(String mapping) throws Exception {
        assertAcked(client().admin().indices().prepareCreate("source").addMapping("test",
                mapping));
        assertAcked(client().admin().indices().prepareCreate("dest").addMapping("test",
                mapping));
        ensureGreen();

        indexRandom(true, client().prepareIndex("source", "test", "test").setSource("foo", "bar"));

        // Copy the doc with the timestamp
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("source");
        copy.destination().setIndex("dest");
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

    }

    public void testTimestamp() throws Exception {
        copyDoc("{\"_timestamp\": {\"enabled\": true}}");
        assertSearchHits(client().prepareSearch("dest").setQuery(existsQuery("_timestamp")).get(), "test");
    }

    public void testTTL() throws Exception {
        copyDoc("{\"_ttl\": {\"enabled\": true, \"default\": \"20d\"}}");
        assertNotNull(client().prepareGet("dest", "test", "test").get().getField("_ttl").getValue());
    }

    public void testRoutingCopiedByDefault() throws Exception {
        routingTestCase(null, "bar");
    }

    public void testRoutingCopiedIfRequested() throws Exception {
        routingTestCase("keep", "bar");
    }

    public void testRoutingDiscardedIfRequested() throws Exception {
        routingTestCase("discard", null);
    }

    public void testRoutingSetIfRequested() throws Exception {
        routingTestCase("=cat", "cat");
    }

    public void testRoutingSetIfWithDegenerateValue() throws Exception {
        routingTestCase("==]", "=]");
    }

    /**
     * Check that index-by-search does the right thing with routing.
     *
     * @param specification
     *            if non-null then routing is specified as this on the
     *            index-by-search request
     * @param expectedRoutingAfterCopy
     *            should the index-by-search request result in the routing being
     *            copied (true) or stripped (false)
     */
    public void routingTestCase(String specification, String expectedRoutingAfterCopy) throws Exception {
        indexRandom(true,
                client().prepareIndex("source", "test", "has_routing").setRouting("bar").setSource("foo", "bar"));

        assertNotNull(client().prepareGet("source", "test", "has_routing").setRouting("bar").get().getField("_routing").getValue());

        // Copy the child to a new type
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("source");
        copy.destination().setIndex("dest");
        if (specification != null) {
            copy.destination().setRouting(specification);
        }
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

        // Make sure routing is intact on the copy
        GetField routing = client().prepareGet("dest", "test", "has_routing").setRouting(expectedRoutingAfterCopy).get().getField("_routing");
        if (expectedRoutingAfterCopy == null) {
            assertNull(expectedRoutingAfterCopy, routing);
        } else {
            assertEquals(expectedRoutingAfterCopy, routing.getValue());
        }

        // Find by rounting
        assertSearchHits(client().prepareSearch("dest").setRouting(expectedRoutingAfterCopy).get(), "has_routing");
    }
}
