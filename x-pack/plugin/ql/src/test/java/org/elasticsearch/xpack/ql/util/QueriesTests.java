/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class QueriesTests extends ESTestCase {

    private static QueryBuilder randomNonBool() {
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

    private static BoolQueryBuilder randomBool() {
        var bool = QueryBuilders.boolQuery();
        if (randomBoolean()) {
            bool.filter(randomNonBool());
        }
        if (randomBoolean()) {
            bool.must(randomNonBool());
        }
        if (randomBoolean()) {
            bool.mustNot(randomNonBool());
        }
        if (randomBoolean()) {
            bool.should(randomNonBool());
        }
        return bool;
    }

    public void testCombineNotCreatingBool() {
        var clause = randomFrom(Queries.Clause.values());
        var nonBool = randomNonBool();
        assertThat(nonBool, sameInstance(Queries.combine(clause, asList(null, null, nonBool, null))));
    }

    public void testCombineNonBoolQueries() {
        var queries = randomArray(2, 10, QueryBuilder[]::new, QueriesTests::randomNonBool);

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;
        assertEquals(clause.operation.apply(bool), list);
    }

    public void testCombineBoolQueries() {
        var queries = randomArray(2, 10, QueryBuilder[]::new, () -> {
            var bool = QueryBuilders.boolQuery();
            if (randomBoolean()) {
                bool.filter(randomNonBool());
            }
            if (randomBoolean()) {
                bool.must(randomNonBool());
            }
            if (randomBoolean()) {
                bool.mustNot(randomNonBool());
            }
            if (randomBoolean()) {
                bool.should(randomNonBool());
            }
            return bool;
        });

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;

        for (QueryBuilder query : queries) {
            assertThat(query, instanceOf(BoolQueryBuilder.class));
            BoolQueryBuilder bqb = (BoolQueryBuilder) query;
            assertThat(bqb.filter(), everyItem(in(bool.filter())));
            assertThat(bqb.must(), everyItem(in(bool.must())));
            assertThat(bqb.mustNot(), everyItem(in(bool.mustNot())));
            assertThat(bqb.should(), everyItem(in(bool.should())));
        }
    }

    public void testCombineMixedBoolAndNonBoolQueries() {
        var nonBool = new int[] { 0 };
        var queries = randomArray(2, 10, QueryBuilder[]::new, () -> {
            if (randomBoolean()) {
                return QueriesTests.randomBool();
            } else {
                nonBool[0]++;
                return QueriesTests.randomNonBool();
            }
        });

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;

        var innerQueries = clause.operation.apply(bool);
        assertThat(innerQueries, hasSize(nonBool[0]));

        for (QueryBuilder query : queries) {
            if (query instanceof BoolQueryBuilder bqb) {
                assertThat(bqb.filter(), everyItem(in(bool.filter())));
                assertThat(bqb.must(), everyItem(in(bool.must())));
                assertThat(bqb.mustNot(), everyItem(in(bool.mustNot())));
                assertThat(bqb.should(), everyItem(in(bool.should())));
            } else {
                assertThat(query, in(innerQueries));
            }
        }
    }

    public void testCombineBoolQueryWithMinClauseSet() {
        var nonBool = new int[] { 0 };
        var nonMixedBool = new int[] { 0 };
        var queries = randomArray(2, 10, QueryBuilder[]::new, () -> {
            if (randomBoolean()) {
                var b = QueriesTests.randomBool();
                if (randomBoolean()) {
                    nonMixedBool[0]++;
                    b.minimumShouldMatch(1);
                }
                return b;
            } else {
                nonBool[0]++;
                return QueriesTests.randomNonBool();
            }
        });

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;

        var innerQueries = clause.operation.apply(bool);
        assertThat(innerQueries, hasSize(nonBool[0] + nonMixedBool[0]));


        for (QueryBuilder query : queries) {
            if (query instanceof BoolQueryBuilder bqb) {
                if (bqb.minimumShouldMatch() != "0") {
                    assertThat(query, in(innerQueries));
                } else {
                    assertThat(bqb.filter(), everyItem(in(bool.filter())));
                    assertThat(bqb.must(), everyItem(in(bool.must())));
                    assertThat(bqb.mustNot(), everyItem(in(bool.mustNot())));
                    assertThat(bqb.should(), everyItem(in(bool.should())));
                }
            } else {
                assertThat(query, in(innerQueries));
            }
        }
    }
}
