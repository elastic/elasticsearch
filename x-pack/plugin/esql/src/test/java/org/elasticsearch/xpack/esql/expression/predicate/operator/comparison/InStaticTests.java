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
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.Arrays;
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
