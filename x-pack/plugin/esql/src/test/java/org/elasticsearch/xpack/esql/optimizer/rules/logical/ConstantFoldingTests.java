/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.FIVE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.notEqualsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.rangeOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class ConstantFoldingTests extends ESTestCase {
    private Expression constantFolding(Expression e) {
        return new ConstantFolding().rule(e, unboundLogicalOptimizerContext());
    }

    public void testConstantFolding() {
        Expression exp = new Add(EMPTY, TWO, THREE);

        assertTrue(exp.foldable());
        Expression result = constantFolding(exp);
        assertEquals(5, as(result, Literal.class).value());

        // check now with an alias
        result = constantFolding(new Alias(EMPTY, "a", exp));
        assertEquals("a", Expressions.name(result));
        assertEquals(Alias.class, result.getClass());
    }

    public void testConstantFoldingBinaryComparison() {
        assertEquals(FALSE, constantFolding(greaterThanOf(TWO, THREE)).canonical());
        assertEquals(FALSE, constantFolding(greaterThanOrEqualOf(TWO, THREE)).canonical());
        assertEquals(FALSE, constantFolding(equalsOf(TWO, THREE)).canonical());
        assertEquals(TRUE, constantFolding(notEqualsOf(TWO, THREE)).canonical());
        assertEquals(TRUE, constantFolding(lessThanOrEqualOf(TWO, THREE)).canonical());
        assertEquals(TRUE, constantFolding(lessThanOf(TWO, THREE)).canonical());
    }

    public void testConstantFoldingBinaryLogic() {
        assertEquals(FALSE, constantFolding(new And(EMPTY, greaterThanOf(TWO, THREE), TRUE)).canonical());
        assertEquals(TRUE, constantFolding(new Or(EMPTY, greaterThanOrEqualOf(TWO, THREE), TRUE)).canonical());
    }

    public void testConstantFoldingBinaryLogic_WithNullHandling() {
        assertEquals(Nullability.TRUE, constantFolding(new And(EMPTY, NULL, TRUE)).canonical().nullable());
        assertEquals(Nullability.TRUE, constantFolding(new And(EMPTY, TRUE, NULL)).canonical().nullable());
        assertEquals(FALSE, constantFolding(new And(EMPTY, NULL, FALSE)).canonical());
        assertEquals(FALSE, constantFolding(new And(EMPTY, FALSE, NULL)).canonical());
        assertEquals(Nullability.TRUE, constantFolding(new And(EMPTY, NULL, NULL)).canonical().nullable());

        assertEquals(TRUE, constantFolding(new Or(EMPTY, NULL, TRUE)).canonical());
        assertEquals(TRUE, constantFolding(new Or(EMPTY, TRUE, NULL)).canonical());
        assertEquals(Nullability.TRUE, constantFolding(new Or(EMPTY, NULL, FALSE)).canonical().nullable());
        assertEquals(Nullability.TRUE, constantFolding(new Or(EMPTY, FALSE, NULL)).canonical().nullable());
        assertEquals(Nullability.TRUE, constantFolding(new Or(EMPTY, NULL, NULL)).canonical().nullable());
    }

    public void testConstantFoldingRange() {
        assertEquals(
            true,
            constantFolding(rangeOf(FIVE, FIVE, true, new Literal(EMPTY, 10, DataType.INTEGER), false)).fold(FoldContext.unbounded())
        );
        assertEquals(
            false,
            constantFolding(rangeOf(FIVE, FIVE, false, new Literal(EMPTY, 10, DataType.INTEGER), false)).fold(FoldContext.unbounded())
        );
    }

    public void testConstantNot() {
        assertEquals(FALSE, constantFolding(new Not(EMPTY, TRUE)));
        assertEquals(TRUE, constantFolding(new Not(EMPTY, FALSE)));
    }

    public void testConstantFoldingLikes() {
        assertEquals(TRUE, constantFolding(new WildcardLike(EMPTY, of("test_emp"), new WildcardPattern("test*"))).canonical());
        assertEquals(TRUE, constantFolding(new RLike(EMPTY, of("test_emp"), new RLikePattern("test.emp"))).canonical());
    }

    public void testArithmeticFolding() {
        assertEquals(10, foldOperator(new Add(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(4, foldOperator(new Sub(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(21, foldOperator(new Mul(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(2, foldOperator(new Div(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(1, foldOperator(new Mod(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
    }

    private Object foldOperator(BinaryOperator<?, ?, ?, ?> b) {
        return ((Literal) constantFolding(b)).value();
    }
}
