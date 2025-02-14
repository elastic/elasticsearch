/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FIVE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FOUR;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.notEqualsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.rangeOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class PropagateEqualsTests extends ESTestCase {
    private Expression propagateEquals(BinaryLogic e) {
        return new PropagateEquals().rule(e, unboundLogicalOptimizerContext());
    }

    // a == 1 AND a == 2 -> FALSE
    public void testDualEqualsConjunction() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = equalsOf(fa, ONE);
        Equals eq2 = equalsOf(fa, TWO);

        Expression exp = propagateEquals(new And(EMPTY, eq1, eq2));
        assertEquals(FALSE, exp);
    }

    // 1 < a < 10 AND a == 10 -> FALSE
    public void testEliminateRangeByEqualsOutsideInterval() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = equalsOf(fa, new Literal(EMPTY, 10, DataType.INTEGER));
        Range r = rangeOf(fa, ONE, false, new Literal(EMPTY, 10, DataType.INTEGER), false);

        Expression exp = propagateEquals(new And(EMPTY, eq1, r));
        assertEquals(FALSE, exp);
    }

    // a != 3 AND a = 3 -> FALSE
    public void testPropagateEquals_VarNeq3AndVarEq3() {
        FieldAttribute fa = getFieldAttribute();
        NotEquals neq = notEqualsOf(fa, THREE);
        Equals eq = equalsOf(fa, THREE);

        Expression exp = propagateEquals(new And(EMPTY, neq, eq));
        assertEquals(FALSE, exp);
    }

    // a != 4 AND a = 3 -> a = 3
    public void testPropagateEquals_VarNeq4AndVarEq3() {
        FieldAttribute fa = getFieldAttribute();
        NotEquals neq = notEqualsOf(fa, FOUR);
        Equals eq = equalsOf(fa, THREE);

        Expression exp = propagateEquals(new And(EMPTY, neq, eq));
        assertEquals(Equals.class, exp.getClass());
        assertEquals(eq, exp);
    }

    // a = 2 AND a < 2 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLt2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThan lt = lessThanOf(fa, TWO);

        Expression exp = propagateEquals(new And(EMPTY, eq, lt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a <= 2 -> a = 2
    public void testPropagateEquals_VarEq2AndVarLte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThanOrEqual lt = lessThanOrEqualOf(fa, TWO);

        Expression exp = propagateEquals(new And(EMPTY, eq, lt));
        assertEquals(eq, exp);
    }

    // a = 2 AND a <= 1 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLte1() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThanOrEqual lt = lessThanOrEqualOf(fa, ONE);

        Expression exp = propagateEquals(new And(EMPTY, eq, lt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a > 2 -> FALSE
    public void testPropagateEquals_VarEq2AndVarGt2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, TWO);

        Expression exp = propagateEquals(new And(EMPTY, eq, gt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a >= 2 -> a = 2
    public void testPropagateEquals_VarEq2AndVarGte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, TWO);

        Expression exp = propagateEquals(new And(EMPTY, eq, gte));
        assertEquals(eq, exp);
    }

    // a = 2 AND a > 3 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, THREE);

        Expression exp = propagateEquals(new And(EMPTY, eq, gt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a < 3 AND a > 1 AND a != 4 -> a = 2
    public void testPropagateEquals_VarEq2AndVarLt3AndVarGt1AndVarNeq4() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThan lt = lessThanOf(fa, THREE);
        GreaterThan gt = greaterThanOf(fa, ONE);
        NotEquals neq = notEqualsOf(fa, FOUR);

        Expression and = Predicates.combineAnd(asList(eq, lt, gt, neq));
        Expression exp = propagateEquals((And) and);
        assertEquals(eq, exp);
    }

    // a = 2 AND 1 < a < 3 AND a > 0 AND a != 4 -> a = 2
    public void testPropagateEquals_VarEq2AndVarRangeGt1Lt3AndVarGt0AndVarNeq4() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        Range range = rangeOf(fa, ONE, false, THREE, false);
        GreaterThan gt = greaterThanOf(fa, new Literal(EMPTY, 0, DataType.INTEGER));
        NotEquals neq = notEqualsOf(fa, FOUR);

        Expression and = Predicates.combineAnd(asList(eq, range, gt, neq));
        Expression exp = propagateEquals((And) and);
        assertEquals(eq, exp);
    }

    // a = 2 OR a > 1 -> a > 1
    public void testPropagateEquals_VarEq2OrVarGt1() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, ONE);

        Expression exp = propagateEquals(new Or(EMPTY, eq, gt));
        assertEquals(gt, exp);
    }

    // a = 2 OR a > 2 -> a >= 2
    public void testPropagateEquals_VarEq2OrVarGte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, TWO);

        Expression exp = propagateEquals(new Or(EMPTY, eq, gt));
        assertEquals(GreaterThanOrEqual.class, exp.getClass());
        GreaterThanOrEqual gte = (GreaterThanOrEqual) exp;
        assertEquals(TWO, gte.right());
    }

    // a = 2 OR a < 3 -> a < 3
    public void testPropagateEquals_VarEq2OrVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        LessThan lt = lessThanOf(fa, THREE);

        Expression exp = propagateEquals(new Or(EMPTY, eq, lt));
        assertEquals(lt, exp);
    }

    // a = 3 OR a < 3 -> a <= 3
    public void testPropagateEquals_VarEq3OrVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, THREE);
        LessThan lt = lessThanOf(fa, THREE);

        Expression exp = propagateEquals(new Or(EMPTY, eq, lt));
        assertEquals(LessThanOrEqual.class, exp.getClass());
        LessThanOrEqual lte = (LessThanOrEqual) exp;
        assertEquals(THREE, lte.right());
    }

    // a = 2 OR 1 < a < 3 -> 1 < a < 3
    public void testPropagateEquals_VarEq2OrVarRangeGt1Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        Range range = rangeOf(fa, ONE, false, THREE, false);

        Expression exp = propagateEquals(new Or(EMPTY, eq, range));
        assertEquals(range, exp);
    }

    // a = 2 OR 2 < a < 3 -> 2 <= a < 3
    public void testPropagateEquals_VarEq2OrVarRangeGt2Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        Range range = rangeOf(fa, TWO, false, THREE, false);

        Expression exp = propagateEquals(new Or(EMPTY, eq, range));
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

        Expression exp = propagateEquals(new Or(EMPTY, eq, range));
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

        Expression exp = propagateEquals(new Or(EMPTY, eq, neq));
        assertEquals(TRUE, exp);
    }

    // a = 2 OR a != 5 -> a != 5
    public void testPropagateEquals_VarEq2OrVarNeq5() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = equalsOf(fa, TWO);
        NotEquals neq = notEqualsOf(fa, FIVE);

        Expression exp = propagateEquals(new Or(EMPTY, eq, neq));
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

        Expression exp = propagateEquals((Or) Predicates.combineOr(asList(eq, range, neq, gt)));
        assertEquals(TRUE, exp);
    }

    // a == 1 AND a == 2 -> nop for date/time fields
    public void testPropagateEquals_ignoreDateTimeFields() {
        FieldAttribute fa = getFieldAttribute("a", DataType.DATETIME);
        Equals eq1 = equalsOf(fa, ONE);
        Equals eq2 = equalsOf(fa, TWO);
        And and = new And(EMPTY, eq1, eq2);

        Expression exp = propagateEquals(and);
        assertEquals(and, exp);
    }

    // 1 <= a < 10 AND a == 1 -> a == 1
    public void testEliminateRangeByEqualsInInterval() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = equalsOf(fa, ONE);
        Range r = rangeOf(fa, ONE, true, new Literal(EMPTY, 10, DataType.INTEGER), false);

        Expression exp = propagateEquals(new And(EMPTY, eq1, r));
        assertEquals(eq1, exp);
    }
}
