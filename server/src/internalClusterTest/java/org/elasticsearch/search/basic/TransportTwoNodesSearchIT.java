/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.basic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class TransportTwoNodesSearchIT extends ESIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    private Set<String> prepareData() throws Exception {
        return prepareData(-1);
    }

    private Set<String> prepareData(int numShards) throws Exception {
        Set<String> fullExpectedIds = new TreeSet<>();

        Settings.Builder settingsBuilder = Settings.builder().put(indexSettings());

        if (numShards > 0) {
            settingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numShards);
        }

        indicesAdmin().create(new CreateIndexRequest("test").settings(settingsBuilder).simpleMapping("foo", "type=geo_point")).actionGet();

        ensureGreen();
        for (int i = 0; i < 100; i++) {
            index(Integer.toString(i), "test", i);
            fullExpectedIds.add(Integer.toString(i));
        }
        refresh();
        return fullExpectedIds;
    }

    private void index(String id, String nameValue, int age) throws IOException {
        client().index(new IndexRequest("test").id(id).source(source(id, nameValue, age))).actionGet();
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

    public void testDfsQueryThenFetch() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder().put(indexSettings());
        indicesAdmin().create(new CreateIndexRequest("test").settings(settingsBuilder)).actionGet();
        ensureGreen();

        // we need to have age (ie number of repeats of "test" term) high enough
        // to produce the same 8-bit norm for all docs here, so that
        // the tf is basically the entire score (assuming idf is fixed, which
        // it should be if dfs is working correctly)
        // With the current way of encoding norms, every length between 1048 and 1176
        // are encoded into the same byte
        for (int i = 1048; i < 1148; i++) {
            index(Integer.toString(i - 1048), "test", i);
        }
        refresh();

        int total = 0;
        SearchResponse searchResponse = prepareSearch("test").setSearchType(DFS_QUERY_THEN_FETCH)
            .setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat(hit.getExplanation().getDetails().length, equalTo(1));
                assertThat(hit.getExplanation().getDetails()[0].getDetails().length, equalTo(3));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails().length, equalTo(2));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[0].getDescription(), startsWith("n,"));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[0].getValue(), equalTo(100L));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[1].getDescription(), startsWith("N,"));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[1].getValue(), equalTo(100L));
                assertThat(
                    "id[" + hit.getId() + "] -> " + hit.getExplanation().toString(),
                    hit.getId(),
                    equalTo(Integer.toString(100 - total - i - 1))
                );
            }
            total += hits.length;
            searchResponse.decRef();
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        searchResponse.decRef();
        assertEquals(100, total);
    }

    public void testDfsQueryThenFetchWithSort() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = prepareSearch("test").setSearchType(DFS_QUERY_THEN_FETCH)
            .setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .addSort("age", SortOrder.ASC)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat(hit.getExplanation().getDetails().length, equalTo(1));
                assertThat(hit.getExplanation().getDetails()[0].getDetails().length, equalTo(3));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails().length, equalTo(2));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[0].getDescription(), startsWith("n,"));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[0].getValue(), equalTo(100L));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[1].getDescription(), startsWith("N,"));
                assertThat(hit.getExplanation().getDetails()[0].getDetails()[1].getDetails()[1].getValue(), equalTo(100L));
                assertThat("id[" + hit.getId() + "]", hit.getId(), equalTo(Integer.toString(total + i)));
            }
            total += hits.length;
            searchResponse.decRef();
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        searchResponse.decRef();
        assertEquals(100, total);
    }

    public void testQueryThenFetch() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = prepareSearch("test").setSearchType(QUERY_THEN_FETCH)
            .setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .addSort("nid", SortOrder.DESC)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat("id[" + hit.getId() + "]", hit.getId(), equalTo(Integer.toString(100 - total - i - 1)));
            }
            total += hits.length;
            searchResponse.decRef();
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        searchResponse.decRef();
        assertEquals(100, total);
    }

    public void testQueryThenFetchWithFrom() throws Exception {
        Set<String> fullExpectedIds = prepareData();

        SearchSourceBuilder source = searchSource().query(matchAllQuery()).explain(true);

        Set<String> collectedIds = new TreeSet<>();

        assertNoFailuresAndResponse(
            client().search(new SearchRequest("test").source(source.from(0).size(60)).searchType(QUERY_THEN_FETCH)),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
                assertThat(searchResponse.getHits().getHits().length, equalTo(60));
                for (int i = 0; i < 60; i++) {
                    SearchHit hit = searchResponse.getHits().getHits()[i];
                    collectedIds.add(hit.getId());
                }
            }
        );
        assertNoFailuresAndResponse(
            client().search(new SearchRequest("test").source(source.from(60).size(60)).searchType(QUERY_THEN_FETCH)),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
                assertThat(searchResponse.getHits().getHits().length, equalTo(40));
                for (int i = 0; i < 40; i++) {
                    SearchHit hit = searchResponse.getHits().getHits()[i];
                    collectedIds.add(hit.getId());
                }
                assertThat(collectedIds, equalTo(fullExpectedIds));
            }
        );
    }

    public void testQueryThenFetchWithSort() throws Exception {
        prepareData();

        int total = 0;
        SearchResponse searchResponse = prepareSearch("test").setQuery(termQuery("multi", "test"))
            .setSize(60)
            .setExplain(true)
            .addSort("age", SortOrder.ASC)
            .setScroll(TimeValue.timeValueSeconds(30))
            .get();
        while (true) {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(100L));
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits.length == 0) {
                break; // finished
            }
            for (int i = 0; i < hits.length; ++i) {
                SearchHit hit = hits[i];
                assertThat(hit.getExplanation(), notNullValue());
                assertThat("id[" + hit.getId() + "]", hit.getId(), equalTo(Integer.toString(total + i)));
            }
            total += hits.length;
            searchResponse.decRef();
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(30)).get();
        }
        clearScroll(searchResponse.getScrollId());
        searchResponse.decRef();
        assertEquals(100, total);
    }

    public void testSimpleFacets() throws Exception {
        prepareData();

        SearchSourceBuilder sourceBuilder = searchSource().query(termQuery("multi", "test"))
            .from(0)
            .size(20)
            .explain(true)
            .aggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.filter("all", termQuery("multi", "test"))))
            .aggregation(AggregationBuilders.filter("test1", termQuery("name", "test1")));

        assertNoFailuresAndResponse(client().search(new SearchRequest("test").source(sourceBuilder)), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(100L));

            SingleBucketAggregation global = response.getAggregations().get("global");
            SingleBucketAggregation all = global.getAggregations().get("all");
            SingleBucketAggregation test1 = response.getAggregations().get("test1");
            assertThat(test1.getDocCount(), equalTo(1L));
            assertThat(all.getDocCount(), equalTo(100L));
        });
    }

    public void testFailedSearchWithWrongQuery() throws Exception {
        prepareData();

        NumShards test = getNumShards("test");

        logger.info("Start Testing failed search with wrong query");
        try {
            SearchResponse searchResponse = client().search(
                new SearchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz")))
            ).actionGet();
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

    public void testFailedSearchWithWrongFrom() throws Exception {
        prepareData();

        NumShards test = getNumShards("test");

        logger.info("Start Testing failed search with wrong from");
        SearchSourceBuilder source = searchSource().query(termQuery("multi", "test")).from(1000).size(20).explain(true);
        assertResponse(client().search(new SearchRequest("test").searchType(DFS_QUERY_THEN_FETCH).source(source)), response -> {
            assertThat(response.getHits().getHits().length, equalTo(0));
            assertThat(response.getTotalShards(), equalTo(test.numPrimaries));
            assertThat(response.getSuccessfulShards(), equalTo(test.numPrimaries));
            assertThat(response.getFailedShards(), equalTo(0));
        });

        assertNoFailuresAndResponse(
            client().search(new SearchRequest("test").searchType(QUERY_THEN_FETCH).source(source)),
            response -> assertThat(response.getHits().getHits().length, equalTo(0))
        );

        assertNoFailuresAndResponse(
            client().search(new SearchRequest("test").searchType(DFS_QUERY_THEN_FETCH).source(source)),
            response -> assertThat(response.getHits().getHits().length, equalTo(0))
        );

        assertNoFailuresAndResponse(
            client().search(new SearchRequest("test").searchType(DFS_QUERY_THEN_FETCH).source(source)),
            response -> assertThat(response.getHits().getHits().length, equalTo(0))
        );

        logger.info("Done Testing failed search");
    }

    public void testFailedMultiSearchWithWrongQuery() throws Exception {
        prepareData();

        logger.info("Start Testing failed multi search with a wrong query");

        assertResponse(
            client().prepareMultiSearch()
                .add(prepareSearch("test").setQuery(new MatchQueryBuilder("foo", "biz")))
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("nid", 2)))
                .add(prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())),
            response -> {
                assertThat(response.getResponses().length, equalTo(3));
                assertThat(response.getResponses()[0].getFailureMessage(), notNullValue());

                assertThat(response.getResponses()[1].getFailureMessage(), nullValue());
                assertThat(response.getResponses()[1].getResponse().getHits().getHits().length, equalTo(1));

                assertThat(response.getResponses()[2].getFailureMessage(), nullValue());
                assertThat(response.getResponses()[2].getResponse().getHits().getHits().length, equalTo(10));
            }
        );
        logger.info("Done Testing failed search");
    }

    public void testFailedMultiSearchWithWrongQueryWithFunctionScore() throws Exception {
        prepareData();

        logger.info("Start Testing failed multi search with a wrong query");

        assertResponse(
            client().prepareMultiSearch()
                // Add custom score query with bogus script
                .add(
                    prepareSearch("test").setQuery(
                        QueryBuilders.functionScoreQuery(
                            QueryBuilders.termQuery("nid", 1),
                            new ScriptScoreFunctionBuilder(new Script(ScriptType.INLINE, "bar", "foo", Collections.emptyMap()))
                        )
                    )
                )
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("nid", 2)))
                .add(prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())),
            response -> {
                assertThat(response.getResponses().length, equalTo(3));
                assertThat(response.getResponses()[0].getFailureMessage(), notNullValue());

                assertThat(response.getResponses()[1].getFailureMessage(), nullValue());
                assertThat(response.getResponses()[1].getResponse().getHits().getHits().length, equalTo(1));

                assertThat(response.getResponses()[2].getFailureMessage(), nullValue());
                assertThat(response.getResponses()[2].getResponse().getHits().getHits().length, equalTo(10));
            }
        );
        logger.info("Done Testing failed search");
    }
}
