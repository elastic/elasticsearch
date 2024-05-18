/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.TestUtils.rangeOf;
import static org.elasticsearch.xpack.ql.TestUtils.relation;
import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;

public class OptimizerRulesTests extends ESTestCase {
    private static final Literal ONE = new Literal(Source.EMPTY, 1, DataTypes.INTEGER);
    private static final Literal TWO = new Literal(Source.EMPTY, 2, DataTypes.INTEGER);
    private static final Literal THREE = new Literal(Source.EMPTY, 3, DataTypes.INTEGER);
    private static final Literal FOUR = new Literal(Source.EMPTY, 4, DataTypes.INTEGER);
    private static final Literal FIVE = new Literal(Source.EMPTY, 5, DataTypes.INTEGER);

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
}
