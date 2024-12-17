/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.NotQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class BoolQueryTests extends ESTestCase {
    static BoolQuery randomBoolQuery(int depth) {
        return new BoolQuery(SourceTests.randomSource(), randomBoolean(), randomQuery(depth), randomQuery(depth));
    }

    static Query randomQuery(int depth) {
        List<Supplier<Query>> options = new ArrayList<>();
        options.add(MatchQueryTests::randomMatchQuery);
        if (depth > 0) {
            options.add(() -> BoolQueryTests.randomBoolQuery(depth - 1));
        }
        return randomFrom(options).get();
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomBoolQuery(5), BoolQueryTests::copy, BoolQueryTests::mutate);
    }

    private static BoolQuery copy(BoolQuery query) {
        return new BoolQuery(query.source(), query.isAnd(), query.queries());
    }

    private static BoolQuery mutate(BoolQuery query) {
        List<Function<BoolQuery, BoolQuery>> options = Arrays.asList(
            q -> new BoolQuery(SourceTests.mutate(q.source()), q.isAnd(), left(q), right(q)),
            q -> new BoolQuery(q.source(), false == q.isAnd(), left(q), right(q)),
            q -> new BoolQuery(q.source(), q.isAnd(), randomValueOtherThan(left(q), () -> randomQuery(5)), right(q)),
            q -> new BoolQuery(q.source(), q.isAnd(), left(q), randomValueOtherThan(right(q), () -> randomQuery(5)))
        );
        return randomFrom(options).apply(query);
    }

    public void testToString() {
        assertEquals(
            "BoolQuery@1:2[ExistsQuery@1:2[f1] AND ExistsQuery@1:8[f2]]",
            new BoolQuery(
                new Source(1, 1, StringUtils.EMPTY),
                true,
                new ExistsQuery(new Source(1, 1, StringUtils.EMPTY), "f1"),
                new ExistsQuery(new Source(1, 7, StringUtils.EMPTY), "f2")
            ).toString()
        );
    }

    public void testNotAllNegated() {
        var q = new BoolQuery(Source.EMPTY, true, new ExistsQuery(Source.EMPTY, "f1"), new ExistsQuery(Source.EMPTY, "f2"));
        assertThat(q.negate(Source.EMPTY), equalTo(new NotQuery(Source.EMPTY, q)));
    }

    public void testNotSomeNegated() {
        var q = new BoolQuery(
            Source.EMPTY,
            true,
            new ExistsQuery(Source.EMPTY, "f1"),
            new NotQuery(Source.EMPTY, new ExistsQuery(Source.EMPTY, "f2"))
        );
        assertThat(
            q.negate(Source.EMPTY),
            equalTo(
                new BoolQuery(
                    Source.EMPTY,
                    false,
                    new NotQuery(Source.EMPTY, new ExistsQuery(Source.EMPTY, "f1")),
                    new ExistsQuery(Source.EMPTY, "f2")
                )
            )
        );
    }

    public static Query left(BoolQuery bool) {
        return indexOf(bool, 0);
    }

    public static Query right(BoolQuery bool) {
        return indexOf(bool, 1);
    }

    private static Query indexOf(BoolQuery bool, int index) {
        List<Query> queries = bool.queries();
        assertThat(queries, hasSize(greaterThanOrEqualTo(2)));
        return queries.get(index);
    }

}
