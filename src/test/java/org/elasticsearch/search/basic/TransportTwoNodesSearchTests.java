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

package org.elasticsearch.search.basic;



import org.elasticsearch.common.unit.TimeValue;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.action.search.SearchType.*;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

public class TransportTwoNodesSearchTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    private Set<String> prepareData() throws Exception {
        return prepareData(-1);
    }

    private Set<String> prepareData(int numShards) throws Exception {
        Set<String> fullExpectedIds = Sets.newTreeSet();

        ImmutableSettings.Builder settingsBuilder = settingsBuilder()
                .put(indexSettings())
                .put("routing.hash.type", "simple");

        if (numShards > 0) {
            settingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numShards);
        }

        client().admin().indices().create(createIndexRequest("test")
                .settings(settingsBuilder))
                .actionGet();

        ensureGreen();
        for (int i = 0; i < 100; i++) {
            index(Integer.toString(i), "test", i);
            fullExpectedIds.add(Integer.toString(i));
        }
        refresh();
        return fullExpectedIds;
    }

    private void index(String id, String nameValue, int age) throws IOException {
        client().index(Requests.indexRequest("test").type("type1").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private XContentBuilder source(String id, String nameValue, int age) throws IOException {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return jsonBuilder().startObject()
                .field("id", id)
                .field("nid", Integer.parseInt(id))
                .field("name", nameValue + id)
                .field("age", age)
                .field("multi", multi.toString())
                .endObject();
    }

    @Test
    public void testDfsQueryThenFetch() throws Exception {
        ImmutableSettings.Builder settingsBuilder = settingsBuilder()
            .put(indexSettings())
            .put("routing.hash.type", "simple");
        client().admin().indices().create(createIndexRequest("test")
            .settings(settingsBuilder))
            .actionGet();
        ensureGreen();
        
        // we need to have age (ie number of repeats of "test" term) high enough
        // to produce the same 8-bit norm for all docs here, so that
        // the tf is basically the entire score (assuming idf is fixed, which
        // it should be if dfs is working correctly)
        for (int i = 1024; i < 1124; i++) {
            index(Integer.toString(i - 1024), "test", i);
        }
        refresh();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(DFS_QUERY_THEN_FETCH).setQuery(termQuery("multi", "test")).setSize(60).setExplain(true).setScroll(TimeValue.timeValueSeconds(30)).get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
            SearchHit[] hits = searchResponse.getHits().hits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.explanation(), notNullValue());
                assertThat("id[" + hit.id() + "] -> " + hit.explanation().toString(), hit.id(), equalTo(Integer.toString(100 - total - i - 1)));
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    @Test
    public void testDfsQueryThenFetchWithSort() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(DFS_QUERY_THEN_FETCH).setQuery(termQuery("multi", "test")).setSize(60).setExplain(true).addSort("age", SortOrder.ASC).setScroll(TimeValue.timeValueSeconds(30)).get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
            SearchHit[] hits = searchResponse.getHits().hits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.explanation(), notNullValue());
                assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(total + i)));
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    @Test
    public void testQueryThenFetch() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(QUERY_THEN_FETCH).setQuery(termQuery("multi", "test")).setSize(60).setExplain(true).addSort("nid", SortOrder.DESC).setScroll(TimeValue.timeValueSeconds(30)).get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
            SearchHit[] hits = searchResponse.getHits().hits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.explanation(), notNullValue());
                assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - total - i - 1)));
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    @Test
    public void testQueryThenFetchWithFrom() throws Exception {
        Set<String> fullExpectedIds = prepareData();

        SearchSourceBuilder source = searchSource()
                .query(matchAllQuery())
                .explain(true);

        Set<String> collectedIds = Sets.newTreeSet();

        SearchResponse searchResponse = client().search(searchRequest("test").source(source.from(0).size(60)).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
        assertThat(searchResponse.getHits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.getHits().hits()[i];
            collectedIds.add(hit.id());
        }
        searchResponse = client().search(searchRequest("test").source(source.from(60).size(60)).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
        assertThat(searchResponse.getHits().hits().length, equalTo(40));
        for (int i = 0; i < 40; i++) {
            SearchHit hit = searchResponse.getHits().hits()[i];
            collectedIds.add(hit.id());
        }
        assertThat(collectedIds, equalTo(fullExpectedIds));
    }

    @Test
    public void testQueryThenFetchWithSort() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(termQuery("multi", "test")).setSize(60).setExplain(true).addSort("age", SortOrder.ASC).setScroll(TimeValue.timeValueSeconds(30)).get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
            SearchHit[] hits = searchResponse.getHits().hits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.explanation(), notNullValue());
                assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(total + i)));
            }
            total += hits.length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        assertEquals(100, total);
    }

    @Test
    public void testQueryAndFetch() throws Exception {
        prepareData(3);

        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(20).explain(true);

        Set<String> expectedIds = Sets.newHashSet();
        for (int i = 0; i < 100; i++) {
            expectedIds.add(Integer.toString(i));
        }

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_AND_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
        assertThat(searchResponse.getHits().hits().length, equalTo(60)); // 20 per shard
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.getHits().hits()[i];
//            System.out.println(hit.shard() + ": " +  hit.explanation());
            assertThat(hit.explanation(), notNullValue());
            // we can't really check here, since its query and fetch, and not controlling distribution
//            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
            assertThat("make sure we don't have duplicates", expectedIds.remove(hit.id()), notNullValue());
        }

        do {
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll("10m").get();
            assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, lessThanOrEqualTo(40));
            for (int i = 0; i < searchResponse.getHits().hits().length; i++) {
                SearchHit hit = searchResponse.getHits().hits()[i];
                // we don't do perfect sorting when it comes to scroll with Query+Fetch
                assertThat("make sure we don't have duplicates", expectedIds.remove(hit.id()), notNullValue());
            }
        } while (searchResponse.getHits().getHits().length > 0);
        clearScroll(searchResponse.getScrollId());
        assertThat("make sure we got all [" + expectedIds + "]", expectedIds.size(), equalTo(0));
    }

    @Test
    public void testDfsQueryAndFetch() throws Exception {
        prepareData(3);

        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(20).explain(true);

        Set<String> expectedIds = Sets.newHashSet();
        for (int i = 0; i < 100; i++) {
            expectedIds.add(Integer.toString(i));
        }


        //SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(DFS_QUERY_AND_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();
        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(DFS_QUERY_AND_FETCH).setScroll("10m").setSource(source.buildAsBytes()).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
        assertThat(searchResponse.getHits().hits().length, equalTo(60)); // 20 per shard
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.getHits().hits()[i];
//            System.out.println(hit.shard() + ": " +  hit.explanation());
            assertThat(hit.explanation(), notNullValue());
//            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
            assertThat("make sure we don't have duplicates", expectedIds.remove(hit.id()), notNullValue());
        }

        do {
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll("10m").get();
    
            assertThat(searchResponse.getHits().totalHits(), equalTo(100l));
            assertThat(searchResponse.getHits().hits().length, lessThanOrEqualTo(40));
            for (int i = 0; i < searchResponse.getHits().hits().length; i++) {
                SearchHit hit = searchResponse.getHits().hits()[i];
                // we don't do perfect sorting when it comes to scroll with Query+Fetch
                assertThat("make sure we don't have duplicates", expectedIds.remove(hit.id()), notNullValue());
            }
        } while (searchResponse.getHits().hits().length > 0);
        clearScroll(searchResponse.getScrollId());
        assertThat("make sure we got all [" + expectedIds + "]", expectedIds.size(), equalTo(0));
    }

    @Test
    public void testSimpleFacets() throws Exception {
        prepareData();

        SearchSourceBuilder sourceBuilder = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(20).explain(true)
                .aggregation(AggregationBuilders.global("global").subAggregation(
                        AggregationBuilders.filter("all").filter(termFilter("multi", "test"))))
                .aggregation(AggregationBuilders.filter("test1").filter(termFilter("name", "test1")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(sourceBuilder)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(100l));

        Global global = searchResponse.getAggregations().get("global");
        Filter all = global.getAggregations().get("all");
        Filter test1 = searchResponse.getAggregations().get("test1");
        assertThat(test1.getDocCount(), equalTo(1l));
        assertThat(all.getDocCount(), equalTo(100l));
    }

    @Test
    public void testFailedSearchWithWrongQuery() throws Exception {
        prepareData();

        NumShards test = getNumShards("test");

        logger.info("Start Testing failed search with wrong query");
        try {
            SearchResponse searchResponse = client().search(searchRequest("test").source("{ xxx }".getBytes(Charsets.UTF_8))).actionGet();
            assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
            assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
            assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
            fail("search should fail");
        } catch (ElasticsearchException e) {
            assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
            // all is well
        }
        logger.info("Done Testing failed search");
    }

    @Test
    public void testFailedSearchWithWrongFrom() throws Exception {
        prepareData();

        NumShards test = getNumShards("test");

        logger.info("Start Testing failed search with wrong from");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(1000).size(20).explain(true);
        SearchResponse response = client().search(searchRequest("test").searchType(DFS_QUERY_AND_FETCH).source(source)).actionGet();
        assertThat(response.getHits().hits().length, equalTo(0));
        assertThat(response.getTotalShards(), equalTo(test.numPrimaries));
        assertThat(response.getSuccessfulShards(), equalTo(test.numPrimaries));
        assertThat(response.getFailedShards(), equalTo(0));

        response = client().search(searchRequest("test").searchType(QUERY_THEN_FETCH).source(source)).actionGet();
        assertNoFailures(response);
        assertThat(response.getHits().hits().length, equalTo(0));

        response = client().search(searchRequest("test").searchType(DFS_QUERY_AND_FETCH).source(source)).actionGet();
        assertNoFailures(response);
        assertThat(response.getHits().hits().length, equalTo(0));

        response = client().search(searchRequest("test").searchType(DFS_QUERY_THEN_FETCH).source(source)).actionGet();
        assertNoFailures(response);
        assertThat(response.getHits().hits().length, equalTo(0));

        logger.info("Done Testing failed search");
    }

    @Test
    public void testFailedMultiSearchWithWrongQuery() throws Exception {
        prepareData();

        logger.info("Start Testing failed multi search with a wrong query");

        MultiSearchResponse response = client().prepareMultiSearch()
                // Add function score with a bogus score mode
                .add(client().prepareSearch("test").setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("nid", 1)).scoreMode("foobar")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("nid", 2)))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        assertThat(response.getResponses().length, equalTo(3));
        assertThat(response.getResponses()[0].getFailureMessage(), notNullValue());

        assertThat(response.getResponses()[1].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[1].getResponse().getHits().hits().length, equalTo(1));

        assertThat(response.getResponses()[2].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[2].getResponse().getHits().hits().length, equalTo(10));

        logger.info("Done Testing failed search");
    }


    @Test
    public void testFailedMultiSearchWithWrongQuery_withFunctionScore() throws Exception {
        prepareData();

        logger.info("Start Testing failed multi search with a wrong query");

        MultiSearchResponse response = client().prepareMultiSearch()
                // Add custom score query with missing script
                .add(client().prepareSearch("test").setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("nid", 1)).add(new ScriptScoreFunctionBuilder())))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("nid", 2)))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        assertThat(response.getResponses().length, equalTo(3));
        assertThat(response.getResponses()[0].getFailureMessage(), notNullValue());

        assertThat(response.getResponses()[1].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[1].getResponse().getHits().hits().length, equalTo(1));

        assertThat(response.getResponses()[2].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[2].getResponse().getHits().hits().length, equalTo(10));

        logger.info("Done Testing failed search");
    }
}
