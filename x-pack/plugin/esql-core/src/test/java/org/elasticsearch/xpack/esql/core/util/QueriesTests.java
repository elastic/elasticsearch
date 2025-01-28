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

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class QueriesTests extends ESTestCase {

    private static QueryBuilder randomNonBoolQuery() {
        return randomFrom(
            random(),
            QueryBuilders::matchAllQuery,
            QueryBuilders::idsQuery,
            () -> QueryBuilders.rangeQuery(randomRealisticUnicodeOfLength(5)),
            () -> QueryBuilders.termQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            () -> QueryBuilders.existsQuery(randomAlphaOfLength(5)),
            () -> QueryBuilders.geoBoundingBoxQuery(randomAlphaOfLength(5))
        );
    }

    private static BoolQueryBuilder randomBoolQuery() {
        var bool = QueryBuilders.boolQuery();
        if (randomBoolean()) {
            bool.filter(randomNonBoolQuery());
        }
        if (randomBoolean()) {
            bool.must(randomNonBoolQuery());
        }
        if (randomBoolean()) {
            bool.mustNot(randomNonBoolQuery());
        }
        if (randomBoolean()) {
            bool.should(randomNonBoolQuery());
        }
        return bool;
    }

    public void testCombineNotCreatingBool() {
        var clause = randomFrom(Queries.Clause.values());
        var nonBool = randomNonBoolQuery();
        assertThat(nonBool, sameInstance(Queries.combine(clause, asList(null, null, nonBool, null))));
    }

    public void testCombineNonBoolQueries() {
        var queries = randomArray(2, 10, QueryBuilder[]::new, QueriesTests::randomNonBoolQuery);

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;
        var clauseList = clause.innerQueries.apply(bool);
        assertThat(list, everyItem(in(clauseList)));
    }

    public void testCombineBoolQueries() {
        var queries = randomArray(2, 10, QueryBuilder[]::new, () -> {
            var bool = QueryBuilders.boolQuery();
            if (randomBoolean()) {
                bool.filter(randomNonBoolQuery());
            }
            if (randomBoolean()) {
                bool.must(randomNonBoolQuery());
            }
            if (randomBoolean()) {
                bool.mustNot(randomNonBoolQuery());
            }
            if (randomBoolean()) {
                bool.should(randomNonBoolQuery());
            }
            return bool;
        });

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;
        assertBoolQueryMerge(queries, bool, clause);
    }

    private void assertBoolQueryMerge(QueryBuilder[] queries, BoolQueryBuilder bool, Queries.Clause clause) {
        BoolQueryBuilder first = (BoolQueryBuilder) queries[0];
        for (QueryBuilder b : first.must()) {
            assertThat(bool.must(), hasItem(b));
        }
        for (QueryBuilder b : first.mustNot()) {
            assertThat(bool.mustNot(), hasItem(b));
        }
        for (QueryBuilder b : first.should()) {
            assertThat(bool.should(), hasItem(b));
        }
        for (QueryBuilder b : first.filter()) {
            assertThat(bool.filter(), hasItem(b));
        }

        var clauseList = clause.innerQueries.apply(bool);
        for (int i = 1; i < queries.length; i++) {
            assertThat(queries[i], in(clauseList));
        }
    }

    public void testCombineMixedBoolAndNonBoolQueries() {
        var queries = randomArray(2, 10, QueryBuilder[]::new, () -> {
            if (randomBoolean()) {
                return QueriesTests.randomBoolQuery();
            } else {
                return QueriesTests.randomNonBoolQuery();
            }
        });

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;

        if (queries[0] instanceof BoolQueryBuilder) {
            assertBoolQueryMerge(queries, bool, clause);
        } else {
            var clauseList = clause.innerQueries.apply(bool);
            for (QueryBuilder query : queries) {
                assertThat(query, in(clauseList));
            }
        }
    }
}
