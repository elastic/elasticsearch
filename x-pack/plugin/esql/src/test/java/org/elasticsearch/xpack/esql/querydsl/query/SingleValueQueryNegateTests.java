/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.NotQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;

import static org.hamcrest.Matchers.equalTo;

/**
 * Assertions that negating {@link SingleValueQuery} spits out the classes
 * we expect. See {@link SingleValueQueryTests} for tests that it matches
 * the docs we expect.
 */
public class SingleValueQueryNegateTests extends ESTestCase {
    public void testNot() {
        boolean useSyntheticSourceDelegate = randomBoolean();
        var sv = new SingleValueQuery(new MatchAll(Source.EMPTY), "foo", useSyntheticSourceDelegate);
        assertThat(
            sv.negate(Source.EMPTY),
            equalTo(
                new SingleValueQuery(
                    new NotQuery(Source.EMPTY, new MatchAll(Source.EMPTY)),
                    "foo",
                    useSyntheticSourceDelegate
                        ? SingleValueQuery.UseSyntheticSourceDelegate.YES_NEGATED
                        : SingleValueQuery.UseSyntheticSourceDelegate.NO
                )
            )
        );
    }

    public void testNotNot() {
        var sv = new SingleValueQuery(new MatchAll(Source.EMPTY), "foo", randomBoolean());
        assertThat(sv.negate(Source.EMPTY).negate(Source.EMPTY), equalTo(sv));
    }
}
