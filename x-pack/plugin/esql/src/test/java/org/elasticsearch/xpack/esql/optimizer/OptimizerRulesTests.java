/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.TestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Like;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.FoldNull;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.PropagateNullable;
import org.elasticsearch.xpack.esql.core.plan.logical.Filter;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.ReplaceRegexMatch;
import org.elasticsearch.xpack.esql.optimizer.rules.ConstantFolding;
import org.elasticsearch.xpack.esql.optimizer.rules.LiteralsOnTheRight;

import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.TestUtils.nullEqualsOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.of;
import static org.elasticsearch.xpack.esql.core.TestUtils.rangeOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.relation;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.hamcrest.Matchers.contains;

public class OptimizerRulesTests extends ESTestCase {
    private static final Literal ONE = new Literal(Source.EMPTY, 1, DataType.INTEGER);
    private static final Literal TWO = new Literal(Source.EMPTY, 2, DataType.INTEGER);
    private static final Literal THREE = new Literal(Source.EMPTY, 3, DataType.INTEGER);
    private static final Literal FOUR = new Literal(Source.EMPTY, 4, DataType.INTEGER);
    private static final Literal FIVE = new Literal(Source.EMPTY, 5, DataType.INTEGER);
    private static final Literal SIX = new Literal(Source.EMPTY, 6, DataType.INTEGER);
    private static final Expression DUMMY_EXPRESSION =
        new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRulesTests.DummyBooleanExpression(EMPTY, 0);

    private static Equals equalsOf(Expression left, Expression right) {
        return new Equals(EMPTY, left, right, null);
    }

    private static LessThan lessThanOf(Expression left, Expression right) {
        return new LessThan(EMPTY, left, right, null);
    }

    public static GreaterThan greaterThanOf(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right, randomZone());
    }

    public static NotEquals notEqualsOf(Expression left, Expression right) {
        return new NotEquals(EMPTY, left, right, randomZone());
    }

    public static LessThanOrEqual lessThanOrEqualOf(Expression left, Expression right) {
        return new LessThanOrEqual(EMPTY, left, right, randomZone());
    }

    public static GreaterThanOrEqual greaterThanOrEqualOf(Expression left, Expression right) {
        return new GreaterThanOrEqual(EMPTY, left, right, randomZone());
    }

    private static FieldAttribute getFieldAttribute() {
        return TestUtils.getFieldAttribute("a");
    }

    //
    // Constant folding
    //

    public void testConstantFolding() {
        Expression exp = new Add(EMPTY, TWO, THREE);

        assertTrue(exp.foldable());
        Expression result = new ConstantFolding().rule(exp);
        assertTrue(result instanceof Literal);
        assertEquals(5, ((Literal) result).value());

        // check now with an alias
        result = new ConstantFolding().rule(new Alias(EMPTY, "a", exp));
        assertEquals("a", Expressions.name(result));
        assertEquals(Alias.class, result.getClass());
    }

    public void testConstantFoldingBinaryComparison() {
        assertEquals(FALSE, new ConstantFolding().rule(greaterThanOf(TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(greaterThanOrEqualOf(TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(equalsOf(TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(nullEqualsOf(TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(nullEqualsOf(TWO, NULL)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(notEqualsOf(TWO, THREE)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(lessThanOrEqualOf(TWO, THREE)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(lessThanOf(TWO, THREE)).canonical());
    }

    public void testConstantFoldingBinaryLogic() {
        assertEquals(FALSE, new ConstantFolding().rule(new And(EMPTY, greaterThanOf(TWO, THREE), TRUE)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new Or(EMPTY, greaterThanOrEqualOf(TWO, THREE), TRUE)).canonical());
    }

    public void testConstantFoldingBinaryLogic_WithNullHandling() {
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new And(EMPTY, NULL, TRUE)).canonical().nullable());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new And(EMPTY, TRUE, NULL)).canonical().nullable());
        assertEquals(FALSE, new ConstantFolding().rule(new And(EMPTY, NULL, FALSE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(new And(EMPTY, FALSE, NULL)).canonical());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new And(EMPTY, NULL, NULL)).canonical().nullable());

        assertEquals(TRUE, new ConstantFolding().rule(new Or(EMPTY, NULL, TRUE)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new Or(EMPTY, TRUE, NULL)).canonical());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new Or(EMPTY, NULL, FALSE)).canonical().nullable());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new Or(EMPTY, FALSE, NULL)).canonical().nullable());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new Or(EMPTY, NULL, NULL)).canonical().nullable());
    }

    public void testConstantFoldingRange() {
        assertEquals(true, new ConstantFolding().rule(rangeOf(FIVE, FIVE, true, new Literal(EMPTY, 10, DataType.INTEGER), false)).fold());
        assertEquals(false, new ConstantFolding().rule(rangeOf(FIVE, FIVE, false, new Literal(EMPTY, 10, DataType.INTEGER), false)).fold());
    }

    public void testConstantNot() {
        assertEquals(FALSE, new ConstantFolding().rule(new Not(EMPTY, TRUE)));
        assertEquals(TRUE, new ConstantFolding().rule(new Not(EMPTY, FALSE)));
    }

    public void testConstantFoldingLikes() {
        assertEquals(TRUE, new ConstantFolding().rule(new Like(EMPTY, of("test_emp"), new LikePattern("test%", (char) 0))).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new WildcardLike(EMPTY, of("test_emp"), new WildcardPattern("test*"))).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new RLike(EMPTY, of("test_emp"), new RLikePattern("test.emp"))).canonical());
    }

    public void testArithmeticFolding() {
        assertEquals(10, foldOperator(new Add(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(4, foldOperator(new Sub(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(21, foldOperator(new Mul(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(2, foldOperator(new Div(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
        assertEquals(1, foldOperator(new Mod(EMPTY, new Literal(EMPTY, 7, DataType.INTEGER), THREE)));
    }

    private static Object foldOperator(BinaryOperator<?, ?, ?, ?> b) {
        return ((Literal) new ConstantFolding().rule(b)).value();
    }

    //
    // CombineDisjunction in Equals
    //
    public void testTwoEqualsWithOr() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), equalsOf(fa, TWO));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testTwoEqualsWithSameValue() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), equalsOf(fa, ONE));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(or);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals(ONE, eq.right());
    }

    public void testOneEqualsOneIn() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), new In(EMPTY, fa, List.of(TWO)));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testOneEqualsOneInWithSameValue() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), new In(EMPTY, fa, asList(ONE, TWO)));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testSingleValueInToEquals() {
        FieldAttribute fa = getFieldAttribute();

        Equals equals = equalsOf(fa, ONE);
        Or or = new Or(EMPTY, equals, new In(EMPTY, fa, List.of(ONE)));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(or);
        assertEquals(equals, e);
    }

    public void testEqualsBehindAnd() {
        FieldAttribute fa = getFieldAttribute();

        And and = new And(EMPTY, equalsOf(fa, ONE), equalsOf(fa, TWO));
        Filter dummy = new Filter(EMPTY, relation(), and);
        LogicalPlan transformed = new OptimizerRules.CombineDisjunctionsToIn().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(and, ((Filter) transformed).condition());
    }

    public void testTwoEqualsDifferentFields() {
        FieldAttribute fieldOne = TestUtils.getFieldAttribute("ONE");
        FieldAttribute fieldTwo = TestUtils.getFieldAttribute("TWO");

        Or or = new Or(EMPTY, equalsOf(fieldOne, ONE), equalsOf(fieldTwo, TWO));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(or);
        assertEquals(or, e);
    }

    public void testMultipleIn() {
        FieldAttribute fa = getFieldAttribute();

        Or firstOr = new Or(EMPTY, new In(EMPTY, fa, List.of(ONE)), new In(EMPTY, fa, List.of(TWO)));
        Or secondOr = new Or(EMPTY, firstOr, new In(EMPTY, fa, List.of(THREE)));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(secondOr);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO, THREE));
    }

    public void testOrWithNonCombinableExpressions() {
        FieldAttribute fa = getFieldAttribute();

        Or firstOr = new Or(EMPTY, new In(EMPTY, fa, List.of(ONE)), lessThanOf(fa, TWO));
        Or secondOr = new Or(EMPTY, firstOr, new In(EMPTY, fa, List.of(THREE)));
        Expression e = new OptimizerRules.CombineDisjunctionsToIn().rule(secondOr);
        assertEquals(Or.class, e.getClass());
        Or or = (Or) e;
        assertEquals(or.left(), firstOr.right());
        assertEquals(In.class, or.right().getClass());
        In in = (In) or.right();
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, THREE));
    }

    // Test BooleanFunctionEqualsElimination
    public void testBoolEqualsSimplificationOnExpressions() {
        OptimizerRules.BooleanFunctionEqualsElimination s = new OptimizerRules.BooleanFunctionEqualsElimination();
        Expression exp = new GreaterThan(EMPTY, getFieldAttribute(), new Literal(EMPTY, 0, DataType.INTEGER), null);

        assertEquals(exp, s.rule(new Equals(EMPTY, exp, TRUE)));
        // TODO: Replace use of QL Not with ESQL Not
        assertEquals(new Not(EMPTY, exp), s.rule(new Equals(EMPTY, exp, FALSE)));
    }

    public void testBoolEqualsSimplificationOnFields() {
        OptimizerRules.BooleanFunctionEqualsElimination s = new OptimizerRules.BooleanFunctionEqualsElimination();

        FieldAttribute field = getFieldAttribute();

        List<? extends BinaryComparison> comparisons = asList(
            new Equals(EMPTY, field, TRUE),
            new Equals(EMPTY, field, FALSE),
            notEqualsOf(field, TRUE),
            notEqualsOf(field, FALSE),
            new Equals(EMPTY, NULL, TRUE),
            new Equals(EMPTY, NULL, FALSE),
            notEqualsOf(NULL, TRUE),
            notEqualsOf(NULL, FALSE)
        );

        for (BinaryComparison comparison : comparisons) {
            assertEquals(comparison, s.rule(comparison));
        }
    }

    // Test Propagate Equals

    // a == 1 AND a == 2 -> FALSE
    public void testDualEqualsConjunction() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = equalsOf(fa, ONE);
        Equals eq2 = equalsOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, eq2));
        assertEquals(FALSE, exp);
    }

    // 1 < a < 10 AND a == 10 -> FALSE
    public void testEliminateRangeByEqualsOutsideInterval() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = equalsOf(fa, new Literal(EMPTY, 10, DataType.INTEGER));
        Range r = rangeOf(fa, ONE, false, new Literal(EMPTY, 10, DataType.INTEGER), false);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(FALSE, exp);
    }

    // a != 3 AND a = 3 -> FALSE
    public void testPropagateEquals_VarNeq3AndVarEq3() {
        FieldAttribute fa = getFieldAttribute();
        NotEquals neq = notEqualsOf(fa, THREE);
        Equals eq = equalsOf(fa, THREE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, neq, eq));
        assertEquals(FALSE, exp);
    }

    // a != 4 AND a = 3 -> a = 3
    public void testPropagateEquals_VarNeq4AndVarEq3() {
        FieldAttribute fa = getFieldAttribute();
        NotEquals neq = notEqualsOf(fa, FOUR);
        Equals eq = equalsOf(fa, THREE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, neq, eq));
        assertEquals(Equals.class, exp.getClass());
        assertEquals(eq, exp);
    }

    // a = 2 AND a < 2 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLt2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThan lt = lessThanOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, lt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a <= 2 -> a = 2
    public void testPropagateEquals_VarEq2AndVarLte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThanOrEqual lt = lessThanOrEqualOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, lt));
        assertEquals(eq, exp);
    }

    // a = 2 AND a <= 1 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLte1() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThanOrEqual lt = lessThanOrEqualOf(fa, ONE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, lt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a > 2 -> FALSE
    public void testPropagateEquals_VarEq2AndVarGt2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, gt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a >= 2 -> a = 2
    public void testPropagateEquals_VarEq2AndVarGte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, gte));
        assertEquals(eq, exp);
    }

    // a = 2 AND a > 3 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, THREE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, gt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a < 3 AND a > 1 AND a != 4 -> a = 2
    public void testPropagateEquals_VarEq2AndVarLt3AndVarGt1AndVarNeq4() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThan lt = lessThanOf(fa, THREE);
        GreaterThan gt = greaterThanOf(fa, ONE);
        NotEquals neq = notEqualsOf(fa, FOUR);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression and = Predicates.combineAnd(asList(eq, lt, gt, neq));
        Expression exp = rule.rule((And) and);
        assertEquals(eq, exp);
    }

    // a = 2 AND 1 < a < 3 AND a > 0 AND a != 4 -> a = 2
    public void testPropagateEquals_VarEq2AndVarRangeGt1Lt3AndVarGt0AndVarNeq4() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        Range range = rangeOf(fa, ONE, false, THREE, false);
        GreaterThan gt = greaterThanOf(fa, new Literal(EMPTY, 0, DataType.INTEGER));
        NotEquals neq = notEqualsOf(fa, FOUR);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression and = Predicates.combineAnd(asList(eq, range, gt, neq));
        Expression exp = rule.rule((And) and);
        assertEquals(eq, exp);
    }

    // a = 2 OR a > 1 -> a > 1
    public void testPropagateEquals_VarEq2OrVarGt1() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, ONE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, gt));
        assertEquals(gt, exp);
    }

    // a = 2 OR a > 2 -> a >= 2
    public void testPropagateEquals_VarEq2OrVarGte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, gt));
        assertEquals(GreaterThanOrEqual.class, exp.getClass());
        GreaterThanOrEqual gte = (GreaterThanOrEqual) exp;
        assertEquals(TWO, gte.right());
    }

    // a = 2 OR a < 3 -> a < 3
    public void testPropagateEquals_VarEq2OrVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThan lt = lessThanOf(fa, THREE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, lt));
        assertEquals(lt, exp);
    }

    // a = 3 OR a < 3 -> a <= 3
    public void testPropagateEquals_VarEq3OrVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, THREE);
        LessThan lt = lessThanOf(fa, THREE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, lt));
        assertEquals(LessThanOrEqual.class, exp.getClass());
        LessThanOrEqual lte = (LessThanOrEqual) exp;
        assertEquals(THREE, lte.right());
    }

    // a = 2 OR 1 < a < 3 -> 1 < a < 3
    public void testPropagateEquals_VarEq2OrVarRangeGt1Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        Range range = rangeOf(fa, ONE, false, THREE, false);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, range));
        assertEquals(range, exp);
    }

    // a = 2 OR 2 < a < 3 -> 2 <= a < 3
    public void testPropagateEquals_VarEq2OrVarRangeGt2Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        Range range = rangeOf(fa, TWO, false, THREE, false);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertTrue(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a = 3 OR 2 < a < 3 -> 2 < a <= 3
    public void testPropagateEquals_VarEq3OrVarRangeGt2Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, THREE);
        Range range = rangeOf(fa, TWO, false, THREE, false);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertTrue(r.includeUpper());
    }

    // a = 2 OR a != 2 -> TRUE
    public void testPropagateEquals_VarEq2OrVarNeq2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        NotEquals neq = notEqualsOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, neq));
        assertEquals(TRUE, exp);
    }

    // a = 2 OR a != 5 -> a != 5
    public void testPropagateEquals_VarEq2OrVarNeq5() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        NotEquals neq = notEqualsOf(fa, FIVE);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, neq));
        assertEquals(NotEquals.class, exp.getClass());
        NotEquals ne = (NotEquals) exp;
        assertEquals(FIVE, ne.right());
    }

    // a = 2 OR 3 < a < 4 OR a > 2 OR a!= 2 -> TRUE
    public void testPropagateEquals_VarEq2OrVarRangeGt3Lt4OrVarGt2OrVarNe2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        Range range = rangeOf(fa, THREE, false, FOUR, false);
        GreaterThan gt = greaterThanOf(fa, TWO);
        NotEquals neq = notEqualsOf(fa, TWO);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule((Or) Predicates.combineOr(asList(eq, range, neq, gt)));
        assertEquals(TRUE, exp);
    }

    // a == 1 AND a == 2 -> nop for date/time fields
    public void testPropagateEquals_ignoreDateTimeFields() {
        FieldAttribute fa = TestUtils.getFieldAttribute("a", DataType.DATETIME);
        Equals eq1 = equalsOf(fa, ONE);
        Equals eq2 = equalsOf(fa, TWO);
        And and = new And(EMPTY, eq1, eq2);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(and);
        assertEquals(and, exp);
    }

    // 1 <= a < 10 AND a == 1 -> a == 1
    public void testEliminateRangeByEqualsInInterval() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = equalsOf(fa, ONE);
        Range r = rangeOf(fa, ONE, true, new Literal(EMPTY, 10, DataType.INTEGER), false);

        OptimizerRules.PropagateEquals rule = new OptimizerRules.PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(eq1, exp);
    }
    //
    // Null folding

    public void testNullFoldingIsNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNull(EMPTY, NULL)).fold());
        assertEquals(false, foldNull.rule(new IsNull(EMPTY, TRUE)).fold());
    }

    public void testGenericNullableExpression() {
        FoldNull rule = new FoldNull();
        // arithmetic
        assertNullLiteral(rule.rule(new Add(EMPTY, getFieldAttribute(), NULL)));
        // comparison
        assertNullLiteral(rule.rule(greaterThanOf(getFieldAttribute(), NULL)));
        // regex
        assertNullLiteral(rule.rule(new RLike(EMPTY, NULL, new RLikePattern("123"))));
    }

    public void testNullFoldingDoesNotApplyOnLogicalExpressions() {
        org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.FoldNull rule =
            new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.FoldNull();

        Or or = new Or(EMPTY, NULL, TRUE);
        assertEquals(or, rule.rule(or));
        or = new Or(EMPTY, NULL, NULL);
        assertEquals(or, rule.rule(or));

        And and = new And(EMPTY, NULL, TRUE);
        assertEquals(and, rule.rule(and));
        and = new And(EMPTY, NULL, NULL);
        assertEquals(and, rule.rule(and));
    }

    //
    // Propagate nullability (IS NULL / IS NOT NULL)
    //

    // a IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNull() {
        FieldAttribute fa = getFieldAttribute();

        And and = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        assertEquals(FALSE, new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.PropagateNullable().rule(and));
    }

    // a IS NULL AND b IS NOT NULL AND c IS NULL AND d IS NOT NULL AND e IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNullMultiField() {
        FieldAttribute fa = getFieldAttribute();

        And andOne = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, getFieldAttribute()));
        And andTwo = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute()), new IsNotNull(EMPTY, getFieldAttribute()));
        And andThree = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute()), new IsNotNull(EMPTY, fa));

        And and = new And(EMPTY, andOne, new And(EMPTY, andThree, andTwo));

        assertEquals(FALSE, new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.PropagateNullable().rule(and));
    }

    // a IS NULL AND a > 1 => a IS NULL AND false
    public void testIsNullAndComparison() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        And and = new And(EMPTY, isNull, greaterThanOf(fa, ONE));
        assertEquals(new And(EMPTY, isNull, nullOf(BOOLEAN)), new PropagateNullable().rule(and));
    }

    // a IS NULL AND b < 1 AND c < 1 AND a < 1 => a IS NULL AND b < 1 AND c < 1 => a IS NULL AND b < 1 AND c < 1
    public void testIsNullAndMultipleComparison() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        And nestedAnd = new And(
            EMPTY,
            lessThanOf(TestUtils.getFieldAttribute("b"), ONE),
            lessThanOf(TestUtils.getFieldAttribute("c"), ONE)
        );
        And and = new And(EMPTY, isNull, nestedAnd);
        And top = new And(EMPTY, and, lessThanOf(fa, ONE));

        Expression optimized = new PropagateNullable().rule(top);
        Expression expected = new And(EMPTY, and, nullOf(BOOLEAN));
        assertEquals(Predicates.splitAnd(expected), Predicates.splitAnd(optimized));
    }

    // ((a+1)/2) > 1 AND a + 2 AND a IS NULL AND b < 3 => NULL AND NULL AND a IS NULL AND b < 3
    public void testIsNullAndDeeplyNestedExpression() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        Expression nullified = new And(
            EMPTY,
            greaterThanOf(new Div(EMPTY, new Add(EMPTY, fa, ONE), TWO), ONE),
            greaterThanOf(new Add(EMPTY, fa, TWO), ONE)
        );
        Expression kept = new And(EMPTY, isNull, lessThanOf(TestUtils.getFieldAttribute("b"), THREE));
        And and = new And(EMPTY, nullified, kept);

        Expression optimized = new PropagateNullable().rule(and);
        Expression expected = new And(EMPTY, new And(EMPTY, nullOf(BOOLEAN), nullOf(BOOLEAN)), kept);

        assertEquals(Predicates.splitAnd(expected), Predicates.splitAnd(optimized));
    }

    // a IS NULL OR a IS NOT NULL => no change
    // a IS NULL OR a > 1 => no change
    public void testIsNullInDisjunction() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        Filter dummy = new Filter(EMPTY, relation(), or);
        LogicalPlan transformed = new PropagateNullable().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());

        or = new Or(EMPTY, new IsNull(EMPTY, fa), greaterThanOf(fa, ONE));
        dummy = new Filter(EMPTY, relation(), or);
        transformed = new PropagateNullable().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());
    }

    // a + 1 AND (a IS NULL OR a > 3) => no change
    public void testIsNullDisjunction() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        Or or = new Or(EMPTY, isNull, greaterThanOf(fa, THREE));
        And and = new And(EMPTY, new Add(EMPTY, fa, ONE), or);

        assertEquals(and, new PropagateNullable().rule(and));
    }

    //
    // Like / Regex
    //
    public void testMatchAllLikeToExist() {
        for (String s : asList("%", "%%", "%%%")) {
            LikePattern pattern = new LikePattern(s, (char) 0);
            FieldAttribute fa = getFieldAttribute();
            Like l = new Like(EMPTY, fa, pattern);
            Expression e = new ReplaceRegexMatch().rule(l);
            assertEquals(IsNotNull.class, e.getClass());
            IsNotNull inn = (IsNotNull) e;
            assertEquals(fa, inn.field());
        }
    }

    public void testMatchAllWildcardLikeToExist() {
        for (String s : asList("*", "**", "***")) {
            WildcardPattern pattern = new WildcardPattern(s);
            FieldAttribute fa = getFieldAttribute();
            WildcardLike l = new WildcardLike(EMPTY, fa, pattern);
            Expression e = new ReplaceRegexMatch().rule(l);
            assertEquals(IsNotNull.class, e.getClass());
            IsNotNull inn = (IsNotNull) e;
            assertEquals(fa, inn.field());
        }
    }

    public void testMatchAllRLikeToExist() {
        RLikePattern pattern = new RLikePattern(".*");
        FieldAttribute fa = getFieldAttribute();
        RLike l = new RLike(EMPTY, fa, pattern);
        Expression e = new ReplaceRegexMatch().rule(l);
        assertEquals(IsNotNull.class, e.getClass());
        IsNotNull inn = (IsNotNull) e;
        assertEquals(fa, inn.field());
    }

    public void testExactMatchLike() {
        for (String s : asList("ab", "ab0%", "ab0_c")) {
            LikePattern pattern = new LikePattern(s, '0');
            FieldAttribute fa = getFieldAttribute();
            Like l = new Like(EMPTY, fa, pattern);
            Expression e = new ReplaceRegexMatch().rule(l);
            assertEquals(Equals.class, e.getClass());
            Equals eq = (Equals) e;
            assertEquals(fa, eq.left());
            assertEquals(s.replace("0", StringUtils.EMPTY), eq.right().fold());
        }
    }

    public void testExactMatchWildcardLike() {
        String s = "ab";
        WildcardPattern pattern = new WildcardPattern(s);
        FieldAttribute fa = getFieldAttribute();
        WildcardLike l = new WildcardLike(EMPTY, fa, pattern);
        Expression e = new ReplaceRegexMatch().rule(l);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals(s, eq.right().fold());
    }

    public void testExactMatchRLike() {
        RLikePattern pattern = new RLikePattern("abc");
        FieldAttribute fa = getFieldAttribute();
        RLike l = new RLike(EMPTY, fa, pattern);
        Expression e = new ReplaceRegexMatch().rule(l);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals("abc", eq.right().fold());
    }

    private void assertNullLiteral(Expression expression) {
        assertEquals(Literal.class, expression.getClass());
        assertNull(expression.fold());
    }

    private IsNotNull isNotNull(Expression field) {
        return new IsNotNull(EMPTY, field);
    }

    private IsNull isNull(Expression field) {
        return new IsNull(EMPTY, field);
    }

    private Literal nullOf(DataType dataType) {
        return new Literal(Source.EMPTY, null, dataType);
    }
    //
    // Logical simplifications
    //

    public void testLiteralsOnTheRight() {
        Alias a = new Alias(EMPTY, "a", new Literal(EMPTY, 10, INTEGER));
        Expression result = new LiteralsOnTheRight().rule(equalsOf(FIVE, a));
        assertTrue(result instanceof Equals);
        Equals eq = (Equals) result;
        assertEquals(a, eq.left());
        assertEquals(FIVE, eq.right());

        // Note: Null Equals test removed here
    }

    public void testBoolSimplifyOr() {
        org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification simplification =
            new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification();

        assertEquals(TRUE, simplification.rule(new Or(EMPTY, TRUE, TRUE)));
        assertEquals(TRUE, simplification.rule(new Or(EMPTY, TRUE, DUMMY_EXPRESSION)));
        assertEquals(TRUE, simplification.rule(new Or(EMPTY, DUMMY_EXPRESSION, TRUE)));

        assertEquals(FALSE, simplification.rule(new Or(EMPTY, FALSE, FALSE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(EMPTY, FALSE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(EMPTY, DUMMY_EXPRESSION, FALSE)));
    }

    public void testBoolSimplifyAnd() {
        org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification simplification =
            new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification();

        assertEquals(TRUE, simplification.rule(new And(EMPTY, TRUE, TRUE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(EMPTY, TRUE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(EMPTY, DUMMY_EXPRESSION, TRUE)));

        assertEquals(FALSE, simplification.rule(new And(EMPTY, FALSE, FALSE)));
        assertEquals(FALSE, simplification.rule(new And(EMPTY, FALSE, DUMMY_EXPRESSION)));
        assertEquals(FALSE, simplification.rule(new And(EMPTY, DUMMY_EXPRESSION, FALSE)));
    }

    public void testBoolCommonFactorExtraction() {
        org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification simplification =
            new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification();

        Expression a1 = new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRulesTests.DummyBooleanExpression(EMPTY, 1);
        Expression a2 = new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRulesTests.DummyBooleanExpression(EMPTY, 1);
        Expression b = new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRulesTests.DummyBooleanExpression(EMPTY, 2);
        Expression c = new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRulesTests.DummyBooleanExpression(EMPTY, 3);

        Or actual = new Or(EMPTY, new And(EMPTY, a1, b), new And(EMPTY, a2, c));
        And expected = new And(EMPTY, a1, new Or(EMPTY, b, c));

        assertEquals(expected, simplification.rule(actual));
    }
}
