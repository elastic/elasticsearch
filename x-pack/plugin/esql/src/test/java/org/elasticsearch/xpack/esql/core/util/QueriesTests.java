/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class QueriesTests extends ESTestCase {

    private static QueryBuilder randomQuery() {
        return randomFrom(
            random(),
            QueryBuilders::matchAllQuery,
            QueryBuilders::idsQuery,
            () -> QueryBuilders.rangeQuery(randomRealisticUnicodeOfLength(5)),
            () -> QueryBuilders.termQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            () -> QueryBuilders.existsQuery(randomAlphaOfLength(5)),
            () -> QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(randomAlphaOfLength(5))),
            () -> QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery(randomAlphaOfLength(5), 1))
                .should(QueryBuilders.termQuery(randomAlphaOfLength(5), 2))
        );
    }

    public void testCombineNothing() {
        var clause = randomFrom(Queries.Clause.values());
        assertThat(Queries.combine(clause, asList(null, null)), nullValue());
    }

    public void testCombineOneQuery() {
        var clause = randomFrom(Queries.Clause.values());
        var query = randomQuery();
        assertThat(Queries.combine(clause, asList(null, query, null, query)), sameInstance(query));
    }

    /** Each query is a clause of its own, so a bool query keeps its meaning, as one with should clauses that must match one. */
    public void testCombineQueries() {
        Set<QueryBuilder> distinct = new LinkedHashSet<>();
        int size = between(2, 10);
        while (distinct.size() < size) {
            distinct.add(randomQuery());
        }
        List<QueryBuilder> queries = new ArrayList<>(distinct);
        List<QueryBuilder> withNullsAndDuplicates = new ArrayList<>(queries);
        withNullsAndDuplicates.add(null);
        withNullsAndDuplicates.add(randomFrom(queries));

        var clause = randomFrom(Queries.Clause.values());
        var combination = Queries.combine(clause, withNullsAndDuplicates);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;
        assertThat(clause.innerQueries.apply(bool), equalTo(queries));
        assertThat(bool.should(), empty());
    }

    /** Combining a combination with queries it already holds changes nothing, as a rule pushing the same filters again needs. */
    public void testCombineAgainChangesNothing() {
        var clause = randomFrom(Queries.Clause.values());
        var queries = List.of(randomQuery(), QueryBuilders.existsQuery("a"), QueryBuilders.existsQuery("b"));
        var combination = Queries.combine(clause, queries);
        var nested = Queries.combine(clause, List.of(combination, QueryBuilders.existsQuery("c")));

        assertThat(Queries.combine(clause, List.of(combination, randomFrom(queries))), sameInstance(combination));
        assertThat(Queries.combine(clause, List.of(nested, randomFrom(queries))), sameInstance(nested));
    }
}
