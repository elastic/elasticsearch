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

import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Index-by-search tests for parent/child.
 */
public class IndexBySearchParentChildTests extends IndexBySearchTestCase {
    QueryBuilder<?> findsCity;
    QueryBuilder<?> findsNeighborhood;

    public void testParentChild() throws Exception {
        setupParentChildIndex(true);

        // Copy the child to a new type
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setQuery(findsCity);
        copy.index().setIndex("test").setType("dest_city");
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

        // Make sure parent/child is intact on that type
        assertSearchHits(client().prepareSearch("test").setTypes("dest_city").setQuery(findsCity).get(), "pittsburgh");

        // Copy the grandchild to a new type
        copy = newIndexBySearch();
        copy.search().setQuery(findsNeighborhood);
        copy.index().setIndex("test").setType("dest_neighborhood");
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

        // Make sure parent/child is intact on that type
        assertSearchHits(client().prepareSearch("test").setTypes("dest_neighborhood").setQuery(findsNeighborhood).get(), "make-believe");
    }

    public void testErrorMessageWhenBadParentChild() throws Exception {
        setupParentChildIndex(false);

        // Copy the child to a new type
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setQuery(findsCity);
        copy.index().setIndex("test").setType("dest");
        try {
            copy.get();
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify parent if no parent field has been configured"));
        }
    }

    /**
     * Setup a parent/child index and return a query that should find the child
     * using the parent.
     */
    private void setupParentChildIndex(boolean addParentMappingForDest) throws Exception {
        CreateIndexRequestBuilder create = client().admin().indices().prepareCreate("test");
        create.addMapping("city", "{\"_parent\": {\"type\": \"country\"}}");
        create.addMapping("neighborhood", "{\"_parent\": {\"type\": \"city\"}}");
        if (addParentMappingForDest) {
            create.addMapping("dest_city", "{\"_parent\": {\"type\": \"country\"}}");
            create.addMapping("dest_neighborhood", "{\"_parent\": {\"type\": \"city\"}}");
        }
        assertAcked(create);
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "country", "united states").setSource("foo", "bar"),
                client().prepareIndex("test", "city", "pittsburgh").setParent("united states").setSource("foo", "bar"),
                client().prepareIndex("test", "neighborhood", "make-believe").setParent("pittsburgh")
                        .setSource("foo", "bar").setRouting("united states"));

        // Make sure we build the parent/child relationship
        findsCity = hasParentQuery("country", idsQuery("country").addIds("united states"));
        assertSearchHits(client().prepareSearch("test").setQuery(findsCity).get(), "pittsburgh");

        findsNeighborhood = hasParentQuery("city", findsCity);
        assertSearchHits(client().prepareSearch("test").setQuery(findsNeighborhood).get(), "make-believe");
    }
}
