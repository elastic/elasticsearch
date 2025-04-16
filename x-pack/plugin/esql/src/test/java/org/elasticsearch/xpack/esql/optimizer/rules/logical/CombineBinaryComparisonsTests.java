/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FIVE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FOUR;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.SIX;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.notEqualsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class CombineBinaryComparisonsTests extends ESTestCase {
    private Expression combine(BinaryLogic e) {
        return new CombineBinaryComparisons().rule(e, unboundLogicalOptimizerContext());
    }

    public void testCombineBinaryComparisonsNotComparable() {
        FieldAttribute fa = getFieldAttribute();
        LessThanOrEqual lte = lessThanOrEqualOf(fa, SIX);
        LessThan lt = lessThanOf(fa, FALSE);

        And and = new And(EMPTY, lte, lt);
        Expression exp = combine(and);
        assertEquals(exp, and);
    }

    // a <= 6 AND a < 5 -> a < 5
    public void testCombineBinaryComparisonsUpper() {
        FieldAttribute fa = getFieldAttribute();
        LessThanOrEqual lte = lessThanOrEqualOf(fa, SIX);
        LessThan lt = lessThanOf(fa, FIVE);

        Expression exp = combine(new And(EMPTY, lte, lt));
        assertEquals(LessThan.class, exp.getClass());
        LessThan r = (LessThan) exp;
        assertEquals(FIVE, r.right());
    }

    // 6 <= a AND 5 < a -> 6 <= a
    public void testCombineBinaryComparisonsLower() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, SIX);
        GreaterThan gt = greaterThanOf(fa, FIVE);

        Expression exp = combine(new And(EMPTY, gte, gt));
        assertEquals(GreaterThanOrEqual.class, exp.getClass());
        GreaterThanOrEqual r = (GreaterThanOrEqual) exp;
        assertEquals(SIX, r.right());
    }

    // 5 <= a AND 5 < a -> 5 < a
    public void testCombineBinaryComparisonsInclude() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, FIVE);
        GreaterThan gt = greaterThanOf(fa, FIVE);

        Expression exp = combine(new And(EMPTY, gte, gt));
        assertEquals(GreaterThan.class, exp.getClass());
        GreaterThan r = (GreaterThan) exp;
        assertEquals(FIVE, r.right());
    }

    // 3 <= a AND 4 < a AND a <= 7 AND a < 6 -> 4 < a AND a < 6
    public void testCombineMultipleBinaryComparisons() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, THREE);
        GreaterThan gt = greaterThanOf(fa, FOUR);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, L(7));
        LessThan lt = lessThanOf(fa, SIX);

        Expression exp = combine(new And(EMPTY, gte, new And(EMPTY, gt, new And(EMPTY, lt, lte))));
        assertEquals(And.class, exp.getClass());
        And and = (And) exp;
        assertEquals(gt, and.left());
        assertEquals(lt, and.right());
    }

    // 3 <= a AND TRUE AND 4 < a AND a != 5 AND a <= 7 -> 4 < a AND a <= 7 AND a != 5 AND TRUE
    public void testCombineMixedMultipleBinaryComparisons() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, THREE);
        GreaterThan gt = greaterThanOf(fa, FOUR);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, L(7));
        Expression ne = notEqualsOf(fa, FIVE);

        // TRUE AND a != 5 AND 4 < a <= 7
        Expression exp = combine(new And(EMPTY, gte, new And(EMPTY, TRUE, new And(EMPTY, gt, new And(EMPTY, ne, lte)))));
        assertEquals(And.class, exp.getClass());
        And and = ((And) exp);
        assertEquals(And.class, and.right().getClass());
        And right = (And) and.right();
        assertEquals(gt, right.left());
        assertEquals(lte, right.right());
        assertEquals(And.class, and.left().getClass());
        And left = (And) and.left();
        assertEquals(TRUE, left.left());
        assertEquals(ne, left.right());
    }

    // 1 <= a AND a < 5 -> 1 <= a AND a < 5
    public void testCombineComparisonsIntoRange() {
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, ONE);
        LessThan lt = lessThanOf(fa, FIVE);

        Expression exp = combine(new And(EMPTY, gte, lt));
        assertEquals(And.class, exp.getClass());

        And and = (And) exp;
        assertEquals(gte, and.left());
        assertEquals(lt, and.right());
    }

    // a != 2 AND a > 3 -> a > 3
    public void testCombineBinaryComparisonsConjunction_Neq2AndGt3() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        GreaterThan gt = greaterThanOf(fa, THREE);
        And and = new And(EMPTY, neq, gt);

        Expression exp = combine(and);
        assertEquals(gt, exp);
    }

    // a != 2 AND a >= 2 -> a > 2
    public void testCombineBinaryComparisonsConjunction_Neq2AndGte2() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, TWO);
        And and = new And(EMPTY, neq, gte);

        Expression exp = combine(and);
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

        Expression exp = combine(and);
        assertEquals(And.class, exp.getClass()); // can't optimize
    }

    // a != 2 AND a <= 3 -> nop
    public void testCombineBinaryComparisonsConjunction_Neq2AndLte3() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, THREE);
        And and = new And(EMPTY, neq, lte);

        Expression exp = combine(and);
        assertEquals(and, exp); // can't optimize
    }

    // a != 2 AND a <= 2 -> a < 2
    public void testCombineBinaryComparisonsConjunction_Neq2AndLte2() {
        FieldAttribute fa = getFieldAttribute();

        NotEquals neq = notEqualsOf(fa, TWO);
        LessThanOrEqual lte = lessThanOrEqualOf(fa, TWO);
        And and = new And(EMPTY, neq, lte);

        Expression exp = combine(and);
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

        Expression exp = combine(and);
        assertEquals(lte, exp);
    }

    // Disjunction

    public void testCombineBinaryComparisonsDisjunctionNotComparable() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt1 = greaterThanOf(fa, ONE);
        GreaterThan gt2 = greaterThanOf(fa, FALSE);

        Or or = new Or(EMPTY, gt1, gt2);

        Expression exp = combine(or);
        assertEquals(exp, or);
    }

    // 2 < a OR 1 < a OR 3 < a -> 1 < a
    public void testCombineBinaryComparisonsDisjunctionLowerBound() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt1 = greaterThanOf(fa, ONE);
        GreaterThan gt2 = greaterThanOf(fa, TWO);
        GreaterThan gt3 = greaterThanOf(fa, THREE);

        Or or = new Or(EMPTY, gt1, new Or(EMPTY, gt2, gt3));

        Expression exp = combine(or);
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

        Expression exp = combine(or);
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

        Expression exp = combine(or);
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

        Expression exp = combine(or);
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

        Expression exp = combine(or);
        assertEquals(Or.class, exp.getClass());

        Or ro = (Or) exp;

        assertEquals(LessThan.class, ro.left().getClass());
        LessThan lt = (LessThan) ro.left();
        assertEquals(TWO, lt.right());
        assertEquals(GreaterThan.class, ro.right().getClass());
        GreaterThan gt = (GreaterThan) ro.right();
        assertEquals(THREE, gt.right());
    }

    // (a = 1 AND b = 3 AND c = 4) OR (a = 2 AND b = 3 AND c = 4) -> (b = 3 AND c = 4) AND (a = 1 OR a = 2)
    public void testBooleanSimplificationCommonExpressionSubstraction() {
        FieldAttribute fa = EsqlTestUtils.getFieldAttribute("a");
        FieldAttribute fb = EsqlTestUtils.getFieldAttribute("b");
        FieldAttribute fc = EsqlTestUtils.getFieldAttribute("c");

        Expression a1 = equalsOf(fa, ONE);
        Expression a2 = equalsOf(fa, TWO);
        And common = new And(EMPTY, equalsOf(fb, THREE), equalsOf(fc, FOUR));
        And left = new And(EMPTY, a1, common);
        And right = new And(EMPTY, a2, common);
        Or or = new Or(EMPTY, left, right);

        Expression exp = new BooleanSimplification().rule(or, unboundLogicalOptimizerContext());
        assertEquals(new And(EMPTY, common, new Or(EMPTY, a1, a2)), exp);
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
            new And(EMPTY, greaterThanOf(doubleOne, L(10)), notEqualsOf(intOne, L(-10))),
            // keyword > '5' AND keyword2 != '48'
            new And(EMPTY, greaterThanOf(keywordOne, L("5")), notEqualsOf(keywordTwo, L("48"))),
            // keyword != '2021' AND datetime <= '2020-12-04T17:48:22.954240Z'
            new And(EMPTY, notEqualsOf(keywordOne, L("2021")), lessThanOrEqualOf(datetimeOne, L("2020-12-04T17:48:22.954240Z"))),
            // double > 10.1 AND double2 != -10.1
            new And(EMPTY, greaterThanOf(doubleOne, L(10.1d)), notEqualsOf(doubleTwo, L(-10.1d)))
        );

        for (And and : testCases) {
            Expression exp = combine(and);
            assertEquals("Rule should not have transformed [" + and.nodeString() + "]", and, exp);
        }
    }

}
