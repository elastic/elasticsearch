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
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Index-by-search tests for parent/child.
 */
public class IndexBySearchParentChildTests extends IndexBySearchTestCase {
    QueryBuilder<?> findsCountry;
    QueryBuilder<?> findsCity;
    QueryBuilder<?> findsNeighborhood;

    public void testParentChild() throws Exception {
        createParentChildIndex("source");
        createParentChildIndex("dest");
        createParentChildDocs("source");

        // Copy parent to the new index
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("source").setQuery(findsCountry);
        copy.destination().setIndex("dest");
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

        // Copy the child to a new index
        copy = newIndexBySearch();
        copy.search().setIndices("source").setQuery(findsCity);
        copy.destination().setIndex("dest");
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

        // Make sure parent/child is intact on that index
        assertSearchHits(client().prepareSearch("dest").setQuery(findsCity).get(), "pittsburgh");

        // Copy the grandchild to a new index
        copy = newIndexBySearch();
        copy.search().setIndices("source").setQuery(findsNeighborhood);
        copy.destination().setIndex("dest");
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

        // Make sure parent/child is intact on that index
        assertSearchHits(client().prepareSearch("dest").setQuery(findsNeighborhood).get(),
                "make-believe");

        // Copy the parent/child/grandchild structure all at once to a third index
        createParentChildIndex("dest_all_at_once");
        copy = newIndexBySearch();
        copy.search().setIndices("source");
        copy.destination().setIndex("dest_all_at_once");
        assertThat(copy.get(), responseMatcher().created(3));
        refresh();

        // Make sure parent/child/grandchild is intact there too
        assertSearchHits(client().prepareSearch("dest_all_at_once").setQuery(findsNeighborhood).get(),
                "make-believe");

    }

    public void testErrorMessageWhenBadParentChild() throws Exception {
        createParentChildIndex("source");
        createParentChildDocs("source");

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("source").setQuery(findsCity);
        copy.destination().setIndex("dest");
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
    private void createParentChildIndex(String indexName) throws Exception {
        CreateIndexRequestBuilder create = client().admin().indices().prepareCreate(indexName);
        create.addMapping("city", "{\"_parent\": {\"type\": \"country\"}}");
        create.addMapping("neighborhood", "{\"_parent\": {\"type\": \"city\"}}");
        assertAcked(create);
        ensureGreen();

    }

    private void createParentChildDocs(String indexName) throws Exception {
        indexRandom(true, client().prepareIndex(indexName, "country", "united states").setSource("foo", "bar"),
                client().prepareIndex(indexName, "city", "pittsburgh").setParent("united states").setSource("foo", "bar"),
                client().prepareIndex(indexName, "neighborhood", "make-believe").setParent("pittsburgh")
                        .setSource("foo", "bar").setRouting("united states"));

        findsCountry = idsQuery("country").addIds("united states");
        findsCity = hasParentQuery("country", findsCountry);
        findsNeighborhood = hasParentQuery("city", findsCity);

        // Make sure we built the parent/child relationship
        assertSearchHits(client().prepareSearch(indexName).setQuery(findsCity).get(), "pittsburgh");
        assertSearchHits(client().prepareSearch(indexName).setQuery(findsNeighborhood).get(), "make-believe");
    }
}
