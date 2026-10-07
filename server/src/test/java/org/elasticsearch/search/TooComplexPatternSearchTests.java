/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A pattern from the request that Lucene cannot determinize must be rejected with a 400,
 * wherever in the search it is compiled.
 */
public class TooComplexPatternSearchTests extends ESSingleNodeTestCase {

    private static final String PATTERN = "*" + "0".repeat(4146) + "*";

    @Before
    public void setupIndex() {
        // Two shards so that the fetch phase runs as its own shard request rather than together with the query phase
        client().admin().indices().prepareCreate("idx").setSettings(Settings.builder().put("index.number_of_shards", 2)).setMapping("""
            {
              "properties": {
                "kw": {"type": "keyword"},
                "n": {"type": "nested", "properties": {"kw": {"type": "keyword"}}}
              }
            }
            """).get();
        for (int i = 0; i < 4; i++) {
            client().prepareIndex("idx").setSource("kw", "x" + i, "n", Map.of("kw", "y" + i)).get();
        }
        client().admin().indices().prepareRefresh("idx").get();
    }

    private static QueryBuilder tooComplex(String field) {
        return QueryBuilders.wildcardQuery(field, PATTERN);
    }

    private SearchRequestBuilder search() {
        return client().prepareSearch("idx").setQuery(QueryBuilders.matchAllQuery());
    }

    public void testMainQuery() {
        assertBadRequest(client().prepareSearch("idx").setQuery(tooComplex("kw")));
    }

    public void testPostFilter() {
        assertBadRequest(search().setPostFilter(tooComplex("kw")));
    }

    public void testFilterAggregation() {
        assertBadRequest(search().addAggregation(AggregationBuilders.filter("f", tooComplex("kw"))));
    }

    public void testFiltersAggregation() {
        assertBadRequest(search().addAggregation(AggregationBuilders.filters("f", tooComplex("kw"))));
    }

    public void testTermsAggregationIncludeRegex() {
        IncludeExclude include = new IncludeExclude("[ac]*a[ac]{200,500}", null, null, null);
        assertBadRequest(search().addAggregation(AggregationBuilders.terms("t").field("kw").includeExclude(include)));
    }

    public void testHighlightQuery() {
        assertBadRequest(search().highlighter(new HighlightBuilder().field("kw").highlightQuery(tooComplex("kw"))));
    }

    public void testNestedSortFilter() {
        assertBadRequest(
            search().addSort(SortBuilders.fieldSort("n.kw").setNestedSort(new NestedSortBuilder("n").setFilter(tooComplex("n.kw"))))
        );
    }

    public void testFetchUnmappedFieldPattern() {
        FieldAndFormat field = new FieldAndFormat("*" + "0".repeat(50_000) + "*", null, true);
        assertBadRequest(search().addFetchField(field).setAllowPartialSearchResults(false));
    }

    private static void assertBadRequest(SearchRequestBuilder request) {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, request::get);
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.shardFailures(), not(emptyArray()));
        for (ShardSearchFailure failure : e.shardFailures()) {
            assertThat(failure.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(ExceptionsHelper.unwrap(failure.getCause(), TooComplexToDeterminizeException.class), notNullValue());
        }
    }
}
