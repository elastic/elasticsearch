/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByColumnAgg;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class SourceGeneratorTests extends ESTestCase {

    public void testNoQueryNoFilter() {
        QueryContainer container = new QueryContainer();
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertNull(sourceBuilder.query());
    }

    public void testQueryNoFilter() {
        QueryContainer container = new QueryContainer().with(new MatchQuery(Location.EMPTY, "foo", "bar"));
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(new MatchQueryBuilder("foo", "bar").operator(Operator.AND), sourceBuilder.query());
    }

    public void testNoQueryFilter() {
        QueryContainer container = new QueryContainer();
        QueryBuilder filter = new MatchQueryBuilder("bar", "baz");
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, randomIntBetween(1, 10));
        assertEquals(new ConstantScoreQueryBuilder(new MatchQueryBuilder("bar", "baz")), sourceBuilder.query());
    }

    public void testQueryFilter() {
        QueryContainer container = new QueryContainer().with(new MatchQuery(Location.EMPTY, "foo", "bar"));
        QueryBuilder filter = new MatchQueryBuilder("bar", "baz");
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, randomIntBetween(1, 10));
        assertEquals(new BoolQueryBuilder().must(new MatchQueryBuilder("foo", "bar").operator(Operator.AND))
                .filter(new MatchQueryBuilder("bar", "baz")), sourceBuilder.query());
    }

    public void testLimit() {
        Aggs aggs = new Aggs(emptyList(), emptyList(), singletonList(new GroupByColumnAgg("1", "", "field")));
        QueryContainer container = new QueryContainer().withLimit(10).with(aggs);
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        Builder aggBuilder = sourceBuilder.aggregations();
        assertEquals(1, aggBuilder.count());
        TermsAggregationBuilder termsBuilder = (TermsAggregationBuilder) aggBuilder.getAggregatorFactories().get(0);
        assertEquals(10, termsBuilder.size());
    }
}
