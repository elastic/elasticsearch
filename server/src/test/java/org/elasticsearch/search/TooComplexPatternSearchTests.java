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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A pattern that Lucene cannot determinize must be rejected with a 400 wherever the query is built,
 * not only as the main query.
 */
public class TooComplexPatternSearchTests extends ESSingleNodeTestCase {

    private static final String PATTERN = "*" + "0".repeat(4146) + "*";

    @Before
    public void setupIndex() {
        client().admin().indices().prepareCreate("idx").setMapping("""
            {
              "properties": {
                "kw": {"type": "keyword"},
                "n": {"type": "nested", "properties": {"kw": {"type": "keyword"}}}
              }
            }
            """).get();
        client().prepareIndex("idx")
            .setSource("kw", "x", "n", Map.of("kw", "y"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
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

    public void testHighlightQuery() {
        assertBadRequest(search().highlighter(new HighlightBuilder().field("kw").highlightQuery(tooComplex("kw"))));
    }

    public void testNestedSortFilter() {
        assertBadRequest(
            search().addSort(SortBuilders.fieldSort("n.kw").setNestedSort(new NestedSortBuilder("n").setFilter(tooComplex("n.kw"))))
        );
    }

    private static void assertBadRequest(SearchRequestBuilder request) {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, request::get);
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        Throwable cause = e.shardFailures()[0].getCause();
        while (cause != null && cause instanceof IllegalArgumentException == false) {
            cause = cause.getCause();
        }
        assertThat("expected an IllegalArgumentException in the cause chain", cause, notNullValue());
        assertThat(cause.getMessage(), equalTo("Pattern was too complex to determinize"));
        assertThat(cause.getCause(), instanceOf(TooComplexToDeterminizeException.class));
        assertThat(cause.getCause().getMessage(), containsString("would require more than 10000 effort"));
    }
}
