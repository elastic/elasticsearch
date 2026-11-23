/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * A query, filter aggregation or alias filter with more nested clauses than the max clause count fails the search with a
 * 400 on every shard. The internal cluster runs in this JVM, so lowering
 * {@link IndexSearcher#setMaxClauseCount(int)} lowers the limit its nodes apply.
 */
public class TooManyClausesIT extends ESIntegTestCase {

    private static final int MAX_CLAUSE_COUNT = 5;

    @Before
    public void createIndex() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate("index").setSettings(indexSettings(between(2, 4), 0)).setMapping("field", "type=keyword"));
        indexRandom(true, prepareIndex("index").setId("1").setSource("field", "v0"));
    }

    public void testQuery() {
        assertBadRequest(prepareSearch("index").setQuery(tooManyNestedClauses()));
    }

    public void testFilterAggregation() {
        assertBadRequest(prepareSearch("index").setSize(0).addAggregation(AggregationBuilders.filter("filter", tooManyNestedClauses())));
    }

    public void testAliasFilter() {
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("index", "alias", tooManyNestedClauses())
        );
        assertBadRequest(prepareSearch("alias"));
    }

    /**
     * Three bool queries of three term queries each: every bool stays under the limit but the whole query has nine leaves.
     */
    private static QueryBuilder tooManyNestedClauses() {
        BoolQueryBuilder outer = boolQuery();
        for (int i = 0; i < 3; i++) {
            BoolQueryBuilder inner = boolQuery();
            for (int j = 0; j < 3; j++) {
                inner.should(termQuery("field", "v" + (i * 3 + j)));
            }
            outer.should(inner);
        }
        return outer;
    }

    private static void assertBadRequest(SearchRequestBuilder request) {
        int originalMaxClauses = IndexSearcher.getMaxClauseCount();
        try {
            IndexSearcher.setMaxClauseCount(MAX_CLAUSE_COUNT);
            SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, request);
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.shardFailures(), not(emptyArray()));
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.status(), equalTo(RestStatus.BAD_REQUEST));
            }
        } finally {
            IndexSearcher.setMaxClauseCount(originalMaxClauses);
        }
    }
}
