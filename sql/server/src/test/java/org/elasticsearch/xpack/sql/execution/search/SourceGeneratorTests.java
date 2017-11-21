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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.sql.tree.Location;

public class SourceGeneratorTests extends ESTestCase {

    public void testNoQueryNoFilter() {
        QueryContainer container = new QueryContainer();
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(sourceBuilder.query(), null);
    }

    public void testQueryNoFilter() {
        QueryContainer container = new QueryContainer().with(new MatchQuery(Location.EMPTY, "foo", "bar"));
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(sourceBuilder.query(), new MatchQueryBuilder("foo", "bar").operator(Operator.AND));
    }

    public void testNoQueryFilter() {
        QueryContainer container = new QueryContainer();
        QueryBuilder filter = new MatchQueryBuilder("bar", "baz");
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, randomIntBetween(1, 10));
        assertEquals(sourceBuilder.query(), new ConstantScoreQueryBuilder(new MatchQueryBuilder("bar", "baz")));
    }

    public void testQueryFilter() {
        QueryContainer container = new QueryContainer().with(new MatchQuery(Location.EMPTY, "foo", "bar"));
        QueryBuilder filter = new MatchQueryBuilder("bar", "baz");
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, randomIntBetween(1, 10));
        assertEquals(sourceBuilder.query(), new BoolQueryBuilder().must( new MatchQueryBuilder("foo", "bar").operator(Operator.AND))
                .filter(new MatchQueryBuilder("bar", "baz")));
    }
}
