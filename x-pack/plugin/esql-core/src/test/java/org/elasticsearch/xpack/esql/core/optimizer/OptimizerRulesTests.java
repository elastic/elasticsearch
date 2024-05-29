/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.TestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.BooleanSimplification;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.CombineBinaryComparisons;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.TestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.TestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.lessThanOrEqualOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.notEqualsOf;
import static org.elasticsearch.xpack.esql.core.TestUtils.of;
import static org.elasticsearch.xpack.esql.core.TestUtils.rangeOf;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.KEYWORD;

public class OptimizerRulesTests extends ESTestCase {

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);
    private static final Literal FOUR = L(4);
    private static final Literal FIVE = L(5);
    private static final Literal SIX = L(6);

    public static class DummyBooleanExpression extends Expression {

        private final int id;

        public DummyBooleanExpression(Source source, int id) {
            super(source, Collections.emptyList());
            this.id = id;
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, DummyBooleanExpression::new, id);
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            throw new UnsupportedOperationException("this type of node doesn't have any children");
        }

        @Override
        public Nullability nullable() {
            return Nullability.FALSE;
        }

        @Override
        public DataType dataType() {
            return BOOLEAN;
        }

        @Override
        public int hashCode() {
            int h = getClass().hashCode();
            h = 31 * h + id;
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            return id == ((DummyBooleanExpression) obj).id;
        }
    }

    private static Literal L(Object value) {
        return of(value);
    }

    private static FieldAttribute getFieldAttribute() {
        return TestUtils.getFieldAttribute("a");
    }

    //
    // Range optimization
    //

    // 6 < a <= 5 -> FALSE
    public void testFoldExcludingRangeToFalse() {
        FieldAttribute fa = getFieldAttribute();

        Range r = rangeOf(fa, SIX, false, FIVE, true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // 6 < a <= 5.5 -> FALSE
    public void testFoldExcludingRangeWithDifferentTypesToFalse() {
        FieldAttribute fa = getFieldAttribute();

        Range r = rangeOf(fa, SIX, false, L(5.5d), true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // Conjunction

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
        LessThanOrEqual lte = lessThanOrEqualOf(fa, L(7));
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
        LessThanOrEqual lte = lessThanOrEqualOf(fa, L(7));
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
        assertEquals(L(7), r.upper());
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

        Range r1 = rangeOf(fa, L(0), false, ONE, true);
        Range r2 = rangeOf(fa, L(0), true, TWO, false);

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
        Range range = rangeOf(fa, L(0), false, ONE, false);
        And and = new And(EMPTY, neq, range);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(L(0), r.lower());
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

        Expression exp = new BooleanSimplification().rule(or);
        assertEquals(new And(EMPTY, common, new Or(EMPTY, a1, a2)), exp);
    }

    // (0 < a <= 1) OR (0 < a < 2) -> 0 < a < 2
    public void testRangesOverlappingNoLowerBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r2 = rangeOf(fa, L(0), false, TWO, false);
        Range r1 = rangeOf(fa, L(0), false, ONE, true);

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
            new And(EMPTY, greaterThanOf(doubleOne, L(10)), notEqualsOf(intOne, L(-10))),
            // keyword > '5' AND keyword2 != '48'
            new And(EMPTY, greaterThanOf(keywordOne, L("5")), notEqualsOf(keywordTwo, L("48"))),
            // keyword != '2021' AND datetime <= '2020-12-04T17:48:22.954240Z'
            new And(EMPTY, notEqualsOf(keywordOne, L("2021")), lessThanOrEqualOf(datetimeOne, L("2020-12-04T17:48:22.954240Z"))),
            // double > 10.1 AND double2 != -10.1
            new And(EMPTY, greaterThanOf(doubleOne, L(10.1d)), notEqualsOf(doubleTwo, L(-10.1d)))
        );

        for (And and : testCases) {
            CombineBinaryComparisons rule = new CombineBinaryComparisons();
            Expression exp = rule.rule(and);
            assertEquals("Rule should not have transformed [" + and.nodeString() + "]", and, exp);
        }
    }
}
