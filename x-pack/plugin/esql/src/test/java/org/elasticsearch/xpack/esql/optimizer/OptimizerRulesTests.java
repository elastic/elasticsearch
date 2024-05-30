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
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.FoldNull;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.PropagateNullable;
import org.elasticsearch.xpack.esql.core.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.core.plan.logical.Filter;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
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
import org.elasticsearch.xpack.esql.optimizer.OptimizerRules.CombineBinaryComparisons;

import java.time.ZoneId;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.TestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.TestUtils.nullEqualsOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.of;
import static org.elasticsearch.xpack.esql.core.TestUtils.rangeOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.relation;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.contains;

public class OptimizerRulesTests extends ESTestCase {
    private static final Literal ZERO = new Literal(Source.EMPTY, 0, DataTypes.INTEGER);
    private static final Literal ONE = new Literal(Source.EMPTY, 1, DataTypes.INTEGER);
    private static final Literal TWO = new Literal(Source.EMPTY, 2, DataTypes.INTEGER);
    private static final Literal THREE = new Literal(Source.EMPTY, 3, DataTypes.INTEGER);
    private static final Literal FOUR = new Literal(Source.EMPTY, 4, DataTypes.INTEGER);
    private static final Literal FIVE = new Literal(Source.EMPTY, 5, DataTypes.INTEGER);
    private static final Literal SIX = new Literal(Source.EMPTY, 6, DataTypes.INTEGER);
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
        assertEquals(true, new ConstantFolding().rule(rangeOf(FIVE, FIVE, true, new Literal(EMPTY, 10, DataTypes.INTEGER), false)).fold());
        assertEquals(
            false,
            new ConstantFolding().rule(rangeOf(FIVE, FIVE, false, new Literal(EMPTY, 10, DataTypes.INTEGER), false)).fold()
        );
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
        assertEquals(10, foldOperator(new Add(EMPTY, new Literal(EMPTY, 7, DataTypes.INTEGER), THREE)));
        assertEquals(4, foldOperator(new Sub(EMPTY, new Literal(EMPTY, 7, DataTypes.INTEGER), THREE)));
        assertEquals(21, foldOperator(new Mul(EMPTY, new Literal(EMPTY, 7, DataTypes.INTEGER), THREE)));
        assertEquals(2, foldOperator(new Div(EMPTY, new Literal(EMPTY, 7, DataTypes.INTEGER), THREE)));
        assertEquals(1, foldOperator(new Mod(EMPTY, new Literal(EMPTY, 7, DataTypes.INTEGER), THREE)));
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
        Expression exp = new GreaterThan(EMPTY, getFieldAttribute(), new Literal(EMPTY, 0, DataTypes.INTEGER), null);

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
        Equals eq1 = equalsOf(fa, new Literal(EMPTY, 10, DataTypes.INTEGER));
        Range r = rangeOf(fa, ONE, false, new Literal(EMPTY, 10, DataTypes.INTEGER), false);

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
        GreaterThan gt = greaterThanOf(fa, new Literal(EMPTY, 0, DataTypes.INTEGER));
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
        FieldAttribute fa = TestUtils.getFieldAttribute("a", DataTypes.DATETIME);
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
        Range r = rangeOf(fa, ONE, true, new Literal(EMPTY, 10, DataTypes.INTEGER), false);

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

    public void testIsNotNullOnIsNullField() {
        EsRelation relation = relation();
        var fieldA = TestUtils.getFieldAttribute("a");
        Expression inn = isNotNull(fieldA);
        Filter f = new Filter(EMPTY, relation, inn);

        assertEquals(f, new LocalLogicalPlanOptimizer.InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnOperatorWithOneField() {
        EsRelation relation = relation();
        var fieldA = TestUtils.getFieldAttribute("a");
        Expression inn = isNotNull(new Add(EMPTY, fieldA, ONE));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, isNotNull(fieldA), inn));

        assertEquals(expected, new LocalLogicalPlanOptimizer.InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnOperatorWithTwoFields() {
        EsRelation relation = relation();
        var fieldA = TestUtils.getFieldAttribute("a");
        var fieldB = TestUtils.getFieldAttribute("b");
        Expression inn = isNotNull(new Add(EMPTY, fieldA, fieldB));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, new And(EMPTY, isNotNull(fieldA), isNotNull(fieldB)), inn));

        assertEquals(expected, new LocalLogicalPlanOptimizer.InferIsNotNull().apply(f));
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
        Expression result = new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.LiteralsOnTheRight().rule(equalsOf(FIVE, a));
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

    public void testBinaryComparisonSimplification() {
        assertEquals(TRUE, new OptimizerRules.BinaryComparisonSimplification().rule(equalsOf(FIVE, FIVE)));
        // Removed Null equals tests here
        assertEquals(FALSE, new OptimizerRules.BinaryComparisonSimplification().rule(notEqualsOf(FIVE, FIVE)));
        assertEquals(TRUE, new OptimizerRules.BinaryComparisonSimplification().rule(greaterThanOrEqualOf(FIVE, FIVE)));
        assertEquals(TRUE, new OptimizerRules.BinaryComparisonSimplification().rule(lessThanOrEqualOf(FIVE, FIVE)));

        assertEquals(FALSE, new OptimizerRules.BinaryComparisonSimplification().rule(greaterThanOf(FIVE, FIVE)));
        assertEquals(FALSE, new OptimizerRules.BinaryComparisonSimplification().rule(lessThanOf(FIVE, FIVE)));
    }

    //
    // Combine Binary Comparison
    //

    public void testCombineBinaryComparisonsNotComparable() {
        FieldAttribute fa = getFieldAttribute();
        LessThanOrEqual lte = lessThanOrEqualOf(fa, SIX);
        LessThan lt = lessThanOf(fa, FALSE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        And and = new And(EMPTY, lte, lt);
        Expression exp = rule.rule(and);
        assertEquals(exp, and);
    }

    // a <= 6 AND a < 5 -> a < 5
    public void testCombineBinaryComparisonsUpper() {
        FieldAttribute fa = getFieldAttribute();
        LessThanOrEqual lte = lessThanOrEqualOf(fa, SIX);
        LessThan lt = lessThanOf(fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, lte, lt));
        assertEquals(LessThan.class, exp.getClass());
        LessThan r = (LessThan) exp;
        assertEquals(FIVE, r.right());
    }

    // 6 <= a AND 5 < a -> 6 <= a
    public void testCombineBinaryComparisonsLower() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, SIX);
        GreaterThan gt = greaterThanOf(fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, gte, gt));
        assertEquals(GreaterThanOrEqual.class, exp.getClass());
        GreaterThanOrEqual r = (GreaterThanOrEqual) exp;
        assertEquals(SIX, r.right());
    }

    // 5 <= a AND 5 < a -> 5 < a
    public void testCombineBinaryComparisonsInclude() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, FIVE);
        GreaterThan gt = greaterThanOf(fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, gte, gt));
        assertEquals(GreaterThan.class, exp.getClass());
        GreaterThan r = (GreaterThan) exp;
        assertEquals(FIVE, r.right());
    }

    // 2 < a AND (2 <= a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsAndRangeLower() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt = greaterThanOf(fa, TWO);
        Range range = rangeOf(fa, TWO, true, THREE, false);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, gt, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a < 4 AND (1 < a < 3) -> 1 < a < 3
    public void testCombineBinaryComparisonsAndRangeUpper() {
        FieldAttribute fa = getFieldAttribute();

        LessThan lt = lessThanOf(fa, FOUR);
        Range range = rangeOf(fa, ONE, false, THREE, false);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, range, lt));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(ONE, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a <= 2 AND (1 < a < 3) -> 1 < a <= 2
    public void testCombineBinaryComparisonsAndRangeUpperEqual() {
        FieldAttribute fa = getFieldAttribute();

        LessThanOrEqual lte = lessThanOrEqualOf(fa, TWO);
        Range range = rangeOf(fa, ONE, false, THREE, false);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, lte, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(ONE, r.lower());
        assertFalse(r.includeLower());
        assertEquals(TWO, r.upper());
        assertTrue(r.includeUpper());
    }

    // 3 <= a AND 4 < a AND a <= 7 AND a < 6 -> 4 < a < 6
    public void testCombineMultipleBinaryComparisons() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, THREE);
        GreaterThan gt = greaterThanOf(fa, FOUR);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, new Literal(EMPTY, 7, INTEGER));
        LessThan lt = lessThanOf(fa, SIX);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, gte, new And(EMPTY, gt, new And(EMPTY, lt, lte))));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(FOUR, r.lower());
        assertFalse(r.includeLower());
        assertEquals(SIX, r.upper());
        assertFalse(r.includeUpper());
    }

    // 3 <= a AND TRUE AND 4 < a AND a != 5 AND a <= 7 -> 4 < a <= 7 AND a != 5 AND TRUE
    public void testCombineMixedMultipleBinaryComparisons() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, THREE);
        GreaterThan gt = greaterThanOf(fa, FOUR);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, new Literal(EMPTY, 7, INTEGER));
        Expression ne = new Not(EMPTY, equalsOf(fa, FIVE));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        // TRUE AND a != 5 AND 4 < a <= 7
        Expression exp = rule.rule(new And(EMPTY, gte, new And(EMPTY, TRUE, new And(EMPTY, gt, new And(EMPTY, ne, lte)))));
        assertEquals(And.class, exp.getClass());
        And and = ((And) exp);
        assertEquals(Range.class, and.right().getClass());
        Range r = (Range) and.right();
        assertEquals(FOUR, r.lower());
        assertFalse(r.includeLower());
        assertEquals(new Literal(EMPTY, 7, INTEGER), r.upper());
        assertTrue(r.includeUpper());
    }

    // 1 <= a AND a < 5 -> 1 <= a < 5
    public void testCombineComparisonsIntoRange() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, ONE);
        LessThan lt = lessThanOf(fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, gte, lt));
        assertEquals(Range.class, exp.getClass());

        Range r = (Range) exp;
        assertEquals(ONE, r.lower());
        assertTrue(r.includeLower());
        assertEquals(FIVE, r.upper());
        assertFalse(r.includeUpper());
    }

    // 1 < a AND a < 3 AND 2 < b AND b < 4 AND c < 4 -> (1 < a < 3) AND (2 < b < 4) AND c < 4
    public void testCombineMultipleComparisonsIntoRange() {
        FieldAttribute fa = TestUtils.getFieldAttribute("a");
        FieldAttribute fb = TestUtils.getFieldAttribute("b");
        FieldAttribute fc = TestUtils.getFieldAttribute("c");

        ZoneId zoneId = randomZone();
        GreaterThan agt1 = new GreaterThan(EMPTY, fa, ONE, zoneId);
        LessThan alt3 = new LessThan(EMPTY, fa, THREE, zoneId);
        GreaterThan bgt2 = new GreaterThan(EMPTY, fb, TWO, zoneId);
        LessThan blt4 = new LessThan(EMPTY, fb, FOUR, zoneId);
        LessThan clt4 = new LessThan(EMPTY, fc, FOUR, zoneId);

        Expression inputAnd = Predicates.combineAnd(asList(agt1, alt3, bgt2, blt4, clt4));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression outputAnd = rule.rule((And) inputAnd);

        Range agt1lt3 = new Range(EMPTY, fa, ONE, false, THREE, false, zoneId);
        Range bgt2lt4 = new Range(EMPTY, fb, TWO, false, FOUR, false, zoneId);

        // The actual outcome is (c < 4) AND (1 < a < 3) AND (2 < b < 4), due to the way the Expression types are combined in the Optimizer
        Expression expectedAnd = Predicates.combineAnd(asList(clt4, agt1lt3, bgt2lt4));

        assertTrue(outputAnd.semanticEquals(expectedAnd));
    }

    // (2 < a < 3) AND (1 < a < 4) -> (2 < a < 3)
    public void testCombineBinaryComparisonsConjunctionOfIncludedRange() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, TWO, false, THREE, false);
        Range r2 = rangeOf(fa, ONE, false, FOUR, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // (2 < a < 3) AND a < 2 -> 2 < a < 2
    public void testCombineBinaryComparisonsConjunctionOfNonOverlappingBoundaries() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, TWO, false, THREE, false);
        Range r2 = rangeOf(fa, ONE, false, TWO, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(TWO, r.upper());
        assertFalse(r.includeUpper());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // (2 < a < 3) AND (2 < a <= 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionOfUpperEqualsOverlappingBoundaries() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, TWO, false, THREE, false);
        Range r2 = rangeOf(fa, TWO, false, THREE, true);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // (2 < a < 3) AND (1 < a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionOverlappingUpperBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r2 = rangeOf(fa, TWO, false, THREE, false);
        Range r1 = rangeOf(fa, ONE, false, THREE, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r2, exp);
    }

    // (2 < a <= 3) AND (1 < a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionWithDifferentUpperLimitInclusion() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, ONE, false, THREE, false);
        Range r2 = rangeOf(fa, TWO, false, THREE, true);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // (0 < a <= 1) AND (0 <= a < 2) -> 0 < a <= 1
    public void testRangesOverlappingConjunctionNoLowerBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, ZERO, false, ONE, true);
        Range r2 = rangeOf(fa, ZERO, true, TWO, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // a != 2 AND 3 < a < 5 -> 3 < a < 5
    public void testCombineBinaryComparisonsConjunction_Neq2AndRangeGt3Lt5() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        Range range = rangeOf(fa, THREE, false, FIVE, false);
        And and = new And(EMPTY, range, neq);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(THREE, r.lower());
        assertFalse(r.includeLower());
        assertEquals(FIVE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a != 2 AND 0 < a < 1 -> 0 < a < 1
    public void testCombineBinaryComparisonsConjunction_Neq2AndRangeGt0Lt1() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        Range range = rangeOf(fa, ZERO, false, ONE, false);
        And and = new And(EMPTY, neq, range);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(ZERO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(ONE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a != 2 AND 2 <= a < 3 -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunction_Neq2AndRangeGte2Lt3() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        Range range = rangeOf(fa, TWO, true, THREE, false);
        And and = new And(EMPTY, neq, range);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a != 3 AND 2 < a <= 3 -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunction_Neq3AndRangeGt2Lte3() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, THREE);
        Range range = rangeOf(fa, TWO, false, THREE, true);
        And and = new And(EMPTY, neq, range);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a != 2 AND 1 < a < 3
    public void testCombineBinaryComparisonsConjunction_Neq2AndRangeGt1Lt3() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        Range range = rangeOf(fa, ONE, false, THREE, false);
        And and = new And(EMPTY, neq, range);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(And.class, exp.getClass()); // can't optimize
    }

    // a != 2 AND a > 3 -> a > 3
    public void testCombineBinaryComparisonsConjunction_Neq2AndGt3() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, THREE);
        And and = new And(EMPTY, neq, gt);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(gt, exp);
    }

    // a != 2 AND a >= 2 -> a > 2
    public void testCombineBinaryComparisonsConjunction_Neq2AndGte2() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, TWO);
        And and = new And(EMPTY, neq, gte);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(GreaterThan.class, exp.getClass());
        GreaterThan gt = (GreaterThan) exp;
        assertEquals(TWO, gt.right());
    }

    // a != 2 AND a >= 1 -> nop
    public void testCombineBinaryComparisonsConjunction_Neq2AndGte1() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, ONE);
        And and = new And(EMPTY, neq, gte);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(And.class, exp.getClass()); // can't optimize
    }

    // a != 2 AND a <= 3 -> nop
    public void testCombineBinaryComparisonsConjunction_Neq2AndLte3() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, THREE);
        And and = new And(EMPTY, neq, lte);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(and, exp); // can't optimize
    }

    // a != 2 AND a <= 2 -> a < 2
    public void testCombineBinaryComparisonsConjunction_Neq2AndLte2() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, TWO);
        And and = new And(EMPTY, neq, lte);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(LessThan.class, exp.getClass());
        LessThan lt = (LessThan) exp;
        assertEquals(TWO, lt.right());
    }

    // a != 2 AND a <= 1 -> a <= 1
    public void testCombineBinaryComparisonsConjunction_Neq2AndLte1() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, ONE);
        And and = new And(EMPTY, neq, lte);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(lte, exp);
    }

    // Disjunction

    public void testCombineBinaryComparisonsDisjunctionNotComparable() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt1 = greaterThanOf(fa, ONE);
        GreaterThan gt2 = greaterThanOf(fa, FALSE);

        Or or = new Or(EMPTY, gt1, gt2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(exp, or);
    }

    // 2 < a OR 1 < a OR 3 < a -> 1 < a
    public void testCombineBinaryComparisonsDisjunctionLowerBound() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt1 = greaterThanOf(fa, ONE);
        GreaterThan gt2 = greaterThanOf(fa, TWO);
        GreaterThan gt3 = greaterThanOf(fa, THREE);

        Or or = new Or(EMPTY, gt1, new Or(EMPTY, gt2, gt3));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(GreaterThan.class, exp.getClass());

        GreaterThan gt = (GreaterThan) exp;
        assertEquals(ONE, gt.right());
    }

    // 2 < a OR 1 < a OR 3 <= a -> 1 < a
    public void testCombineBinaryComparisonsDisjunctionIncludeLowerBounds() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt1 = greaterThanOf(fa, ONE);
        GreaterThan gt2 = greaterThanOf(fa, TWO);
        GreaterThanOrEqual gte3 = greaterThanOrEqualOf(fa, THREE);

        Or or = new Or(EMPTY, new Or(EMPTY, gt1, gt2), gte3);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(GreaterThan.class, exp.getClass());

        GreaterThan gt = (GreaterThan) exp;
        assertEquals(ONE, gt.right());
    }

    // a < 1 OR a < 2 OR a < 3 -> a < 3
    public void testCombineBinaryComparisonsDisjunctionUpperBound() {
        FieldAttribute fa = getFieldAttribute();

        LessThan lt1 = lessThanOf(fa, ONE);
        LessThan lt2 = lessThanOf(fa, TWO);
        LessThan lt3 = lessThanOf(fa, THREE);

        Or or = new Or(EMPTY, new Or(EMPTY, lt1, lt2), lt3);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(LessThan.class, exp.getClass());

        LessThan lt = (LessThan) exp;
        assertEquals(THREE, lt.right());
    }

    // a < 2 OR a <= 2 OR a < 1 -> a <= 2
    public void testCombineBinaryComparisonsDisjunctionIncludeUpperBounds() {
        FieldAttribute fa = getFieldAttribute();

        LessThan lt1 = lessThanOf(fa, ONE);
        LessThan lt2 = lessThanOf(fa, TWO);
        LessThanOrEqual lte2 = lessThanOrEqualOf(fa, TWO);

        Or or = new Or(EMPTY, lt2, new Or(EMPTY, lte2, lt1));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(LessThanOrEqual.class, exp.getClass());

        LessThanOrEqual lte = (LessThanOrEqual) exp;
        assertEquals(TWO, lte.right());
    }

    // a < 2 OR 3 < a OR a < 1 OR 4 < a -> a < 2 OR 3 < a
    public void testCombineBinaryComparisonsDisjunctionOfLowerAndUpperBounds() {
        FieldAttribute fa = getFieldAttribute();

        LessThan lt1 = lessThanOf(fa, ONE);
        LessThan lt2 = lessThanOf(fa, TWO);

        GreaterThan gt3 = greaterThanOf(fa, THREE);
        GreaterThan gt4 = greaterThanOf(fa, FOUR);

        Or or = new Or(EMPTY, new Or(EMPTY, lt2, gt3), new Or(EMPTY, lt1, gt4));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(Or.class, exp.getClass());

        Or ro = (Or) exp;

        assertEquals(LessThan.class, ro.left().getClass());
        LessThan lt = (LessThan) ro.left();
        assertEquals(TWO, lt.right());
        assertEquals(GreaterThan.class, ro.right().getClass());
        GreaterThan gt = (GreaterThan) ro.right();
        assertEquals(THREE, gt.right());
    }

    // (2 < a < 3) OR (1 < a < 4) -> (1 < a < 4)
    public void testCombineBinaryComparisonsDisjunctionOfIncludedRangeNotComparable() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, TWO, false, THREE, false);
        Range r2 = rangeOf(fa, ONE, false, FALSE, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (2 < a < 3) OR (1 < a < 4) -> (1 < a < 4)
    public void testCombineBinaryComparisonsDisjunctionOfIncludedRange() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, TWO, false, THREE, false);
        Range r2 = rangeOf(fa, ONE, false, FOUR, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(Range.class, exp.getClass());

        Range r = (Range) exp;
        assertEquals(ONE, r.lower());
        assertFalse(r.includeLower());
        assertEquals(FOUR, r.upper());
        assertFalse(r.includeUpper());
    }

    // (2 < a < 3) OR (1 < a < 2) -> same
    public void testCombineBinaryComparisonsDisjunctionOfNonOverlappingBoundaries() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, TWO, false, THREE, false);
        Range r2 = rangeOf(fa, ONE, false, TWO, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (2 < a < 3) OR (2 < a <= 3) -> 2 < a <= 3
    public void testCombineBinaryComparisonsDisjunctionOfUpperEqualsOverlappingBoundaries() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, TWO, false, THREE, false);
        Range r2 = rangeOf(fa, TWO, false, THREE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r2, exp);
    }

    // (2 < a < 3) OR (1 < a < 3) -> 1 < a < 3
    public void testCombineBinaryComparisonsOverlappingUpperBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r2 = rangeOf(fa, TWO, false, THREE, false);
        Range r1 = rangeOf(fa, ONE, false, THREE, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r1, exp);
    }

    // (2 < a <= 3) OR (1 < a < 3) -> same (the <= prevents the ranges from being combined)
    public void testCombineBinaryComparisonsWithDifferentUpperLimitInclusion() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = rangeOf(fa, ONE, false, THREE, false);
        Range r2 = rangeOf(fa, TWO, false, THREE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (a = 1 AND b = 3 AND c = 4) OR (a = 2 AND b = 3 AND c = 4) -> (b = 3 AND c = 4) AND (a = 1 OR a = 2)
    public void testBooleanSimplificationCommonExpressionSubstraction() {
        FieldAttribute fa = TestUtils.getFieldAttribute("a");
        FieldAttribute fb = TestUtils.getFieldAttribute("b");
        FieldAttribute fc = TestUtils.getFieldAttribute("c");

        Expression a1 = equalsOf(fa, ONE);
        Expression a2 = equalsOf(fa, TWO);
        And common = new And(EMPTY, equalsOf(fb, THREE), equalsOf(fc, FOUR));
        And left = new And(EMPTY, a1, common);
        And right = new And(EMPTY, a2, common);
        Or or = new Or(EMPTY, left, right);

        Expression exp = new org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification().rule(or);
        assertEquals(new And(EMPTY, common, new Or(EMPTY, a1, a2)), exp);
    }

    // (0 < a <= 1) OR (0 < a < 2) -> 0 < a < 2
    public void testRangesOverlappingNoLowerBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r2 = rangeOf(fa, ZERO, false, TWO, false);
        Range r1 = rangeOf(fa, ZERO, false, ONE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r2, exp);
    }

    public void testBinaryComparisonAndOutOfRangeNotEqualsDifferentFields() {
        FieldAttribute doubleOne = fieldAttribute("double", DOUBLE);
        FieldAttribute doubleTwo = fieldAttribute("double2", DOUBLE);
        FieldAttribute intOne = fieldAttribute("int", INTEGER);
        FieldAttribute datetimeOne = fieldAttribute("datetime", INTEGER);
        FieldAttribute keywordOne = fieldAttribute("keyword", KEYWORD);
        FieldAttribute keywordTwo = fieldAttribute("keyword2", KEYWORD);

        List<And> testCases = asList(
            // double > 10 AND integer != -10
            new And(
                EMPTY,
                greaterThanOf(doubleOne, new Literal(EMPTY, 10, INTEGER)),
                notEqualsOf(intOne, new Literal(EMPTY, -10, INTEGER))
            ),
            // keyword > '5' AND keyword2 != '48'
            new And(
                EMPTY,
                greaterThanOf(keywordOne, new Literal(EMPTY, "5", KEYWORD)),
                notEqualsOf(keywordTwo, new Literal(EMPTY, "48", KEYWORD))
            ),
            // keyword != '2021' AND datetime <= '2020-12-04T17:48:22.954240Z'
            new And(
                EMPTY,
                notEqualsOf(keywordOne, new Literal(EMPTY, "2021", KEYWORD)),
                lessThanOrEqualOf(datetimeOne, new Literal(EMPTY, "2020-12-04T17:48:22.954240Z", KEYWORD))
            ),
            // double > 10.1 AND double2 != -10.1
            new And(
                EMPTY,
                greaterThanOf(doubleOne, new Literal(EMPTY, 10.1d, DOUBLE)),
                notEqualsOf(doubleTwo, new Literal(EMPTY, -10.1d, DOUBLE))
            )
        );

        for (And and : testCases) {
            CombineBinaryComparisons rule = new CombineBinaryComparisons();
            Expression exp = rule.rule(and);
            assertEquals("Rule should not have transformed [" + and.nodeString() + "]", and, exp);
        }
    }
}
