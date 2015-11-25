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

import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;

/**
 * Index-by-search tests for parent/child.
 */
public class IndexBySearchParentChildTests extends IndexBySearchTestCase {
    public void testParentChild() throws Exception {
        QueryBuilder<?> parentIsUS = setupParentChildIndex(true);

        // Copy the child to a new type
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setQuery(parentIsUS);
        copy.index().setIndex("test").setType("dest");
        assertResponse(copy.get(), 1, 0);
        refresh();

        // Make sure parent/child is intact on that type
        assertSearchHits(client().prepareSearch("test").setTypes("dest").setQuery(parentIsUS).get(), "north carolina");
    }

    public void testErrorMessageWhenBadParentChild() throws Exception {
        QueryBuilder<?> parentIsUS = setupParentChildIndex(false);

        // Copy the child to a new type
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setQuery(parentIsUS);
        copy.index().setIndex("test").setType("dest");
        try {
            copy.get();
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify parent if no parent field has been configured"));
        }
    }

    // NOCOMMIT needs grandparent tests

    /**
     * Setup a parent/child index and return a query that should find the child
     * using the parent.
     */
    private QueryBuilder<?> setupParentChildIndex(boolean addParentMappingForDest) throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("city", "{\"_parent\": {\"type\": \"country\"}}")
                .addMapping(addParentMappingForDest ? "dest" : "not_dest", "{\"_parent\": {\"type\": \"country\"}}"));
        ensureGreen();

        indexRandom(true,
                client().prepareIndex("test", "country", "united states").setSource("foo", "bar"),
                client().prepareIndex("test", "city", "north carolina").setParent("united states").setSource("foo", "bar"));

        // Make sure we build the parent/child relationship
        QueryBuilder<?> parentIsUS = hasParentQuery("country", idsQuery("country").addIds("united states"));
        assertSearchHits(client().prepareSearch("test").setQuery(parentIsUS).get(), "north carolina");

        return parentIsUS;
    }
}
