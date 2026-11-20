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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
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

    private static QueryBuilder[] randomDistinctQueries(Supplier<QueryBuilder> supplier) {
        Set<QueryBuilder> queries = new LinkedHashSet<>();
        int size = between(2, 10);
        while (queries.size() < size) {
            queries.add(supplier.get());
        }
        return queries.toArray(QueryBuilder[]::new);
    }

    public void testCombineNotCreatingBool() {
        var clause = randomFrom(Queries.Clause.values());
        var nonBool = randomNonBoolQuery();
        assertThat(nonBool, sameInstance(Queries.combine(clause, asList(null, null, nonBool, null))));
    }

    public void testCombineNonBoolQueries() {
        var queries = randomDistinctQueries(QueriesTests::randomNonBoolQuery);

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;
        var clauseList = clause.innerQueries.apply(bool);
        assertThat(list, everyItem(in(clauseList)));
    }

    public void testCombineBoolQueries() {
        var queries = randomDistinctQueries(QueriesTests::randomBoolQuery);

        var clause = randomFrom(Queries.Clause.values());
        var list = asList(queries);
        var combination = Queries.combine(clause, list);

        assertThat(combination, instanceOf(BoolQueryBuilder.class));
        var bool = (BoolQueryBuilder) combination;
        assertBoolQueryMerge(queries, bool, clause);
    }

    public void testCombineEqualNonBoolQueries() {
        var clause = randomFrom(Queries.Clause.values());
        var query = randomNonBoolQuery();
        assertThat(Queries.combine(clause, asList(query, query)), sameInstance(query));
    }

    public void testCombineEqualBoolQueries() {
        var clause = randomFrom(Queries.Clause.values());
        var query = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("a", 1)).should(QueryBuilders.termQuery("b", 2));
        assertThat(Queries.combine(clause, asList(query, query)), equalTo(query));
    }

    /** Whether {@link Queries#combine} keeps the first query as a clause of its own, as it does a query that is not a bool. */
    private static boolean wrapsFirstBool(QueryBuilder first, Queries.Clause clause) {
        return first instanceof BoolQueryBuilder bool
            && (clause == Queries.Clause.FILTER || clause == Queries.Clause.MUST)
            && bool.should().isEmpty() == false
            && bool.must().isEmpty()
            && bool.filter().isEmpty();
    }

    private void assertBoolQueryMerge(QueryBuilder[] queries, BoolQueryBuilder bool, Queries.Clause clause) {
        if (wrapsFirstBool(queries[0], clause)) {
            var wrappedList = clause.innerQueries.apply(bool);
            for (QueryBuilder query : queries) {
                assertThat(query, in(wrappedList));
            }
            return;
        }
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

    /**
     * A bool of {@code should} clauses is an {@code OR}. Adding a {@code filter} or a {@code must} clause to it would make those clauses
     * optional, so it stays a clause of its own and the outer bool has no {@code should} clause.
     */
    public void testCombineKeepsTheShouldClausesOfTheFirstBoolRequired() {
        var or = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("a", 1)).should(QueryBuilders.termQuery("b", 2));
        var other = QueryBuilders.termQuery("c", 3);
        for (var clause : asList(Queries.Clause.FILTER, Queries.Clause.MUST)) {
            var combination = (BoolQueryBuilder) Queries.combine(clause, asList(or, other));
            assertThat(clause.innerQueries.apply(combination), equalTo(asList(or, other)));
            assertThat(combination.should(), empty());
        }
    }

    /** A should-only bool stays a clause of its own whatever its minimum_should_match, a zero one makes the should clauses optional. */
    public void testCombineKeepsAShouldOnlyBoolWithAMinimumShouldMatchNested() {
        var other = QueryBuilders.termQuery("c", 3);
        for (String minimum : asList("0", "1", "50%", "-100%")) {
            var or = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("a", 1))
                .should(QueryBuilders.termQuery("b", 2))
                .minimumShouldMatch(minimum);
            var combination = (BoolQueryBuilder) Queries.combine(Queries.Clause.FILTER, asList(or, other));
            assertThat(combination.filter(), equalTo(asList(or, other)));
            assertThat(combination.should(), empty());
        }
    }

    /** Adding a clause to the first bool is still fine when it does not change what that bool matches. */
    public void testCombineStillReusesTheFirstBoolWhenItsMeaningIsKept() {
        var other = QueryBuilders.termQuery("c", 3);
        var withFilter = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("a", 1)).should(QueryBuilders.termQuery("b", 2));
        var onlyFilter = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("a", 1));
        for (var first : asList(withFilter, onlyFilter)) {
            var combination = (BoolQueryBuilder) Queries.combine(Queries.Clause.FILTER, asList(first, other));
            assertThat(combination.should(), equalTo(first.should()));
            assertThat(combination.filter(), hasItem(other));
            assertThat(combination.filter(), not(hasItem(first)));
        }
        // A should or must_not clause is added to the bool as before.
        var or = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("a", 1));
        assertThat(((BoolQueryBuilder) Queries.combine(Queries.Clause.SHOULD, asList(or, other))).should(), hasItem(other));
        assertThat(((BoolQueryBuilder) Queries.combine(Queries.Clause.MUST_NOT, asList(or, other))).mustNot(), hasItem(other));
    }

    public void testCombineMixedBoolAndNonBoolQueries() {
        var queries = randomDistinctQueries(() -> randomBoolean() ? randomBoolQuery() : randomNonBoolQuery());

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
