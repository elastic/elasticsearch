/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.hamcrest.Matchers;

import java.util.List;

public class EvaluatorMapperTests extends ESTestCase {
    public void testFoldCompletesWithPlentyOfMemory() {
        Add add = new Add(
            Source.synthetic("shouldn't break"),
            new Literal(Source.EMPTY, 1, DataType.INTEGER),
            new Literal(Source.EMPTY, 3, DataType.INTEGER),
            EsqlTestUtils.TEST_CFG
        );
        assertEquals(add.fold(new FoldContext(200)), 4);
    }

    public void testFoldBreaksWithLittleMemory() {
        Add add = new Add(
            Source.synthetic("should break"),
            new Literal(Source.EMPTY, 1, DataType.INTEGER),
            new Literal(Source.EMPTY, 3, DataType.INTEGER),
            EsqlTestUtils.TEST_CFG
        );
        Exception e = expectThrows(FoldContext.FoldTooMuchMemoryException.class, () -> add.fold(new FoldContext(10)));
        assertThat(
            e.getMessage(),
            Matchers.equalTo(
                "line -1:-1: Folding query used more than 10b. "
                    + "The expression that pushed past the limit is [should break] which needed 40b."
            )
        );
    }

    /**
     * {@code CASE(false, 1)} folds to {@code null}, so the {@link Add} folds to {@code null}. The {@code 1/0} argument
     * must still be folded and warn, whatever its position, like it would when evaluated.
     */
    public void testAnyNullIsNullFoldWarnsForEveryArgument() {
        Expression nullArgument = new Case(Source.EMPTY, Literal.FALSE, List.of(new Literal(Source.EMPTY, 1, DataType.INTEGER)));
        Expression divByZero = new Div(
            Source.synthetic("1/0"),
            new Literal(Source.EMPTY, 1, DataType.INTEGER),
            new Literal(Source.EMPTY, 0, DataType.INTEGER)
        );

        assertNull(new Add(Source.EMPTY, divByZero, nullArgument, EsqlTestUtils.TEST_CFG).fold(FoldContext.small()));
        assertDivByZeroWarnings();

        assertNull(new Add(Source.EMPTY, nullArgument, divByZero, EsqlTestUtils.TEST_CFG).fold(FoldContext.small()));
        assertDivByZeroWarnings();
    }

    private void assertDivByZeroWarnings() {
        assertWarnings(
            "Line -1:-1: evaluation of [1/0] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.ArithmeticException: / by zero"
        );
    }
}
