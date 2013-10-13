/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.matchedqueries;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;

/**
 *
 */
public class MatchedQueriesTests extends AbstractIntegrationTest {

    @Test
    public void simpleMatchedQueryFromFilteredQuery() throws Exception {

        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "test1")
                .field("number", 1)
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "test2")
                .field("number", 2)
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "test3")
                .field("number", 3)
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), orFilter(rangeFilter("number").lte(2).filterName("test1"), rangeFilter("number").gt(2).filterName("test2"))))
                .execute().actionGet();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test1"));
            } else if (hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test2"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }

        searchResponse = client().prepareSearch()
                .setQuery(boolQuery().should(rangeQuery("number").lte(2).queryName("test1")).should(rangeQuery("number").gt(2).queryName("test2")))
                .execute().actionGet();


        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test1"));
            } else if (hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test2"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void simpleMatchedQueryFromTopLevelFilter() throws Exception {

        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "test")
                .field("title", "title1")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "test")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "test")
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(orFilter(
                        termFilter("name", "test").filterName("name"),
                        termFilter("title", "title1").filterName("title")))
                .execute().actionGet();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else if (hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(queryFilter(boolQuery()
                        .should(termQuery("name", "test").queryName("name"))
                        .should(termQuery("title", "title1").queryName("title"))))
                .execute().actionGet();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else if (hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void simpleMatchedQueryFromTopLevelFilterAndFilteredQuery() throws Exception {

        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "test")
                .field("title", "title1")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "test")
                .field("title", "title2")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "test")
                .field("title", "title3")
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("title", "title1", "title2", "title3").filterName("title")))
                        .setFilter(termFilter("name", "test").filterName("name"))
                        .execute().actionGet();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }

        searchResponse = client().prepareSearch()
                .setQuery(termsQuery("title", "title1", "title2", "title3").queryName("title"))
                .setFilter(queryFilter(matchQuery("name", "test").queryName("name")))
                .execute().actionGet();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }
}
