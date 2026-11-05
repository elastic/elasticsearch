/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class InStaticTests extends ESTestCase {
    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);

    public void testInWithContainedValue() {
        In in = new In(EMPTY, TWO, Arrays.asList(ONE, TWO, THREE));
        assertTrue((Boolean) in.fold(FoldContext.small()));
    }

    public void testInWithNotContainedValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, TWO));
        assertFalse((Boolean) in.fold(FoldContext.small()));
    }

    public void testHandleNullOnLeftValue() {
        In in = new In(EMPTY, NULL, Arrays.asList(ONE, TWO, THREE));
        assertNull(in.fold(FoldContext.small()));
        in = new In(EMPTY, NULL, Arrays.asList(ONE, NULL, THREE));
        assertNull(in.fold(FoldContext.small()));

    }

    public void testHandleNullsOnRightValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, NULL, THREE));
        assertTrue((Boolean) in.fold(FoldContext.small()));
        in = new In(EMPTY, ONE, Arrays.asList(TWO, NULL, THREE));
        assertNull(in.fold(FoldContext.small()));
    }

    /**
     * {@code x IN (a, b)} means {@code x == a OR x == b}, so for doubles {@code NaN} matches nothing, not even itself, and
     * {@code -0.0} matches {@code 0.0}. {@link #testZeroPushesDownAsBothZeros} covers the same for the pushed down query.
     */
    public void testDoublesMatchLikeEquals() {
        assertFalse((Boolean) new In(EMPTY, L(Double.NaN), Arrays.asList(L(Double.NaN), L(1.0))).fold(FoldContext.small()));
        assertTrue((Boolean) new In(EMPTY, L(-0.0), Arrays.asList(L(0.0), L(1.0))).fold(FoldContext.small()));
        assertTrue((Boolean) new In(EMPTY, L(0.0), Arrays.asList(L(-0.0), L(1.0))).fold(FoldContext.small()));
    }

    /**
     * Numeric fields only index finite values, so a non-finite value in the list can never match. It must not reach the
     * terms query either, which rejects non-finite values and would fail the whole query.
     */
    public void testNonFiniteDoublesPushDownAsNoMatch() {
        In in = new In(
            EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "field",
                new EsField("field", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            Arrays.asList(L(1.0), L(Double.NaN), L(Double.POSITIVE_INFINITY), L(Double.NEGATIVE_INFINITY))
        );
        var query = in.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
        Query matchNone = new MatchAll(EMPTY).negate(EMPTY);
        assertEquals(
            new BoolQuery(EMPTY, false, List.of(matchNone, matchNone, matchNone, new TermsQuery(EMPTY, "field", Set.of(1.0)))),
            query
        );
    }

    /**
     * Lucene keeps {@code -0.0} apart from {@code 0.0}, so a zero in the list must push down as both zeros to match what
     * evaluation does, whatever the literal's sign or type.
     */
    public void testZeroPushesDownAsBothZeros() {
        In in = new In(
            EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "field",
                new EsField("field", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            Arrays.asList(L(1.0), randomFrom(L(0.0), L(-0.0), L(0)))
        );
        assertEquals(
            new TermsQuery(EMPTY, "field", Set.of(1.0, -0.0, 0.0)),
            in.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER)
        );
    }

    public void testConvertedNull() {
        In in = new In(
            EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "field",
                new EsField("suffix", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            Arrays.asList(ONE, new Literal(Source.EMPTY, null, randomFrom(DataType.types())), THREE)
        );
        var query = in.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
        assertEquals(new TermsQuery(EMPTY, "field", Set.of(1, 3)), query);
    }

}
