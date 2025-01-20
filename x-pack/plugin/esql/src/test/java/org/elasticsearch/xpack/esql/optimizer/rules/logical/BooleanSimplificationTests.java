/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.optimizer.OptimizerRulesTests;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class BooleanSimplificationTests extends ESTestCase {
    private static final Expression DUMMY_EXPRESSION = new OptimizerRulesTests.DummyBooleanExpression(EMPTY, 0);

    private Expression booleanSimplification(ScalarFunction e) {
        return new BooleanSimplification().rule(e, unboundLogicalOptimizerContext());
    }

    public void testBoolSimplifyOr() {
        assertEquals(TRUE, booleanSimplification(new Or(EMPTY, TRUE, TRUE)));
        assertEquals(TRUE, booleanSimplification(new Or(EMPTY, TRUE, DUMMY_EXPRESSION)));
        assertEquals(TRUE, booleanSimplification(new Or(EMPTY, DUMMY_EXPRESSION, TRUE)));

        assertEquals(FALSE, booleanSimplification(new Or(EMPTY, FALSE, FALSE)));
        assertEquals(DUMMY_EXPRESSION, booleanSimplification(new Or(EMPTY, FALSE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, booleanSimplification(new Or(EMPTY, DUMMY_EXPRESSION, FALSE)));
    }

    public void testBoolSimplifyAnd() {
        assertEquals(TRUE, booleanSimplification(new And(EMPTY, TRUE, TRUE)));
        assertEquals(DUMMY_EXPRESSION, booleanSimplification(new And(EMPTY, TRUE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, booleanSimplification(new And(EMPTY, DUMMY_EXPRESSION, TRUE)));

        assertEquals(FALSE, booleanSimplification(new And(EMPTY, FALSE, FALSE)));
        assertEquals(FALSE, booleanSimplification(new And(EMPTY, FALSE, DUMMY_EXPRESSION)));
        assertEquals(FALSE, booleanSimplification(new And(EMPTY, DUMMY_EXPRESSION, FALSE)));
    }

    public void testBoolCommonFactorExtraction() {
        BooleanSimplification simplification = new BooleanSimplification();

        Expression a1 = new OptimizerRulesTests.DummyBooleanExpression(EMPTY, 1);
        Expression a2 = new OptimizerRulesTests.DummyBooleanExpression(EMPTY, 1);
        Expression b = new OptimizerRulesTests.DummyBooleanExpression(EMPTY, 2);
        Expression c = new OptimizerRulesTests.DummyBooleanExpression(EMPTY, 3);

        Or actual = new Or(EMPTY, new And(EMPTY, a1, b), new And(EMPTY, a2, c));
        And expected = new And(EMPTY, a1, new Or(EMPTY, b, c));

        assertEquals(expected, booleanSimplification(actual));
    }

}
