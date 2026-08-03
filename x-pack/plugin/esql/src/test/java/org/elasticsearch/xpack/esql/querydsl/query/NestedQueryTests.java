/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class NestedQueryTests extends ESTestCase {

    private static NestedQuery randomNestedQuery() {
        return new NestedQuery(
            SourceTests.randomSource(),
            randomAlphaOfLength(5),
            new ExistsQuery(SourceTests.randomSource(), randomAlphaOfLength(5)),
            randomFrom(ScoreMode.values())
        );
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomNestedQuery(), NestedQueryTests::copy, NestedQueryTests::mutate);
    }

    private static NestedQuery copy(NestedQuery q) {
        return new NestedQuery(q.source(), q.path(), q.child(), q.scoreMode());
    }

    private static NestedQuery mutate(NestedQuery q) {
        List<Function<NestedQuery, NestedQuery>> options = Arrays.asList(
            n -> new NestedQuery(SourceTests.mutate(n.source()), n.path(), n.child(), n.scoreMode()),
            n -> new NestedQuery(n.source(), n.path() + "!", n.child(), n.scoreMode()),
            n -> new NestedQuery(
                n.source(),
                n.path(),
                new ExistsQuery(n.child().source(), "other_" + randomAlphaOfLength(3)),
                n.scoreMode()
            ),
            n -> new NestedQuery(n.source(), n.path(), n.child(), randomValueOtherThan(n.scoreMode(), () -> randomFrom(ScoreMode.values())))
        );
        return randomFrom(options).apply(q);
    }

    public void testDefaultScoreModeIsMax() {
        assertThat(NestedQuery.DEFAULT_SCORE_MODE, equalTo(ScoreMode.Max));
    }

    public void testToQueryBuilder() {
        // A scorable child with a scoring mode keeps the wrapper scorable, so toQueryBuilder returns the
        // nested query directly (no constant-score unwrapping).
        Query child = MatchQueryTests.randomMatchQuery();
        NestedQuery q = new NestedQuery(Source.EMPTY, "users", child, ScoreMode.Max);
        assertThat(q.toQueryBuilder(), instanceOf(NestedQueryBuilder.class));
        NestedQueryBuilder builder = (NestedQueryBuilder) q.toQueryBuilder();
        assertThat(builder.path(), equalTo("users"));
        assertThat(builder.scoreMode(), equalTo(ScoreMode.Max));
        assertThat(builder.query(), equalTo(child.toQueryBuilder()));
    }

    public void testScorableDelegatesToChildAndMode() {
        Query scorableChild = MatchQueryTests.randomMatchQuery(); // full-text queries are scorable
        assertTrue(new NestedQuery(Source.EMPTY, "users", scorableChild, ScoreMode.Max).scorable());
        assertFalse(new NestedQuery(Source.EMPTY, "users", scorableChild, ScoreMode.None).scorable());
        // ExistsQuery is not scorable, so the wrapper is not scorable regardless of the mode.
        assertFalse(new NestedQuery(Source.EMPTY, "users", new ExistsQuery(Source.EMPTY, "users.role"), ScoreMode.Max).scorable());
    }
}
