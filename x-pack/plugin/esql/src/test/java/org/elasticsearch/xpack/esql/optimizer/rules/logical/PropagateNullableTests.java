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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

public class PropagateNullableTests extends ESTestCase {
    private Expression propagateNullable(And e) {
        return new PropagateNullable().rule(e, unboundLogicalOptimizerContext());
    }

    private LogicalPlan propagateNullable(LogicalPlan p) {
        return new PropagateNullable().apply(p, unboundLogicalOptimizerContext());
    }

    private Literal nullOf(DataType dataType) {
        return new Literal(Source.EMPTY, null, dataType);
    }

    // a IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNull() {
        FieldAttribute fa = getFieldAttribute();

        And and = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        assertEquals(FALSE, propagateNullable(and));
    }

    // a IS NULL AND b IS NOT NULL AND c IS NULL AND d IS NOT NULL AND e IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNullMultiField() {
        FieldAttribute fa = getFieldAttribute();

        And andOne = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, getFieldAttribute()));
        And andTwo = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute()), new IsNotNull(EMPTY, getFieldAttribute()));
        And andThree = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute()), new IsNotNull(EMPTY, fa));

        And and = new And(EMPTY, andOne, new And(EMPTY, andThree, andTwo));

        assertEquals(FALSE, propagateNullable(and));
    }

    // a IS NULL AND a > 1 => a IS NULL AND null > 1
    // (null > 1 folds to null in the next FoldNull pass)
    public void testIsNullAndComparison() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        And and = new And(EMPTY, isNull, greaterThanOf(fa, ONE));
        assertEquals(new And(EMPTY, isNull, greaterThanOf(nullOf(INTEGER), ONE)), propagateNullable(and));
    }

    // a IS NULL AND b < 1 AND c < 1 AND a < 1 => a IS NULL AND b < 1 AND c < 1 AND null < 1
    public void testIsNullAndMultipleComparison() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        And nestedAnd = new And(EMPTY, lessThanOf(getFieldAttribute("b"), ONE), lessThanOf(getFieldAttribute("c"), ONE));
        And and = new And(EMPTY, isNull, nestedAnd);
        And top = new And(EMPTY, and, lessThanOf(fa, ONE));

        Expression optimized = propagateNullable(top);
        // "and" (IsNull + LT(b) + LT(c)) is unchanged; LT(fa, ONE) becomes LT(null, ONE)
        Expression expected = new And(EMPTY, and, lessThanOf(nullOf(INTEGER), ONE));
        assertEquals(Predicates.splitAnd(expected), Predicates.splitAnd(optimized));
    }

    // ((a+1)/2) > 1 AND a+2 > 1 AND a IS NULL AND b < 3
    // => ((null+1)/2) > 1 AND null+2 > 1 AND a IS NULL AND b < 3
    // (the arithmetic folds to null > 1 in the next FoldNull pass)
    public void testIsNullAndDeeplyNestedExpression() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        Expression nullified = new And(
            EMPTY,
            greaterThanOf(new Div(EMPTY, new Add(EMPTY, fa, ONE, TEST_CFG), TWO), ONE),
            greaterThanOf(new Add(EMPTY, fa, TWO, TEST_CFG), ONE)
        );
        Expression kept = new And(EMPTY, isNull, lessThanOf(getFieldAttribute("b"), THREE));
        And and = new And(EMPTY, nullified, kept);

        Expression optimized = propagateNullable(and);

        Literal nullInt = nullOf(INTEGER);
        Expression expected = new And(
            EMPTY,
            new And(
                EMPTY,
                greaterThanOf(new Div(EMPTY, new Add(EMPTY, nullInt, ONE, TEST_CFG), TWO), ONE),
                greaterThanOf(new Add(EMPTY, nullInt, TWO, TEST_CFG), ONE)
            ),
            kept
        );
        assertEquals(Predicates.splitAnd(expected), Predicates.splitAnd(optimized));
    }

    // a IS NULL OR a IS NOT NULL => no change
    // a IS NULL OR a > 1 => no change
    public void testIsNullInDisjunction() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        Filter dummy = new Filter(EMPTY, relation(), or);
        LogicalPlan transformed = propagateNullable(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());

        or = new Or(EMPTY, new IsNull(EMPTY, fa), greaterThanOf(fa, ONE));
        dummy = new Filter(EMPTY, relation(), or);
        transformed = propagateNullable(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());
    }

    // a + 1 AND (a IS NULL OR a > 3) => no change
    public void testIsNullDisjunction() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        Or or = new Or(EMPTY, isNull, greaterThanOf(fa, THREE));
        And and = new And(EMPTY, new Add(EMPTY, fa, ONE, TEST_CFG), or);

        assertEquals(and, propagateNullable(and));
    }

    // IS NULL applied to a constant: LT(ONE, ONE) has both children replaced with null
    public void testIsNullAndMultipleComparisonWithConstants() {
        Literal a = ONE;
        Literal b = ONE;
        FieldAttribute c = getFieldAttribute("c");
        IsNull aIsNull = new IsNull(EMPTY, a);

        And bLT1_AND_cLT1 = new And(EMPTY, lessThanOf(b, ONE), lessThanOf(c, ONE));
        And aIsNull_AND_bLT1_AND_cLT1 = new And(EMPTY, aIsNull, bLT1_AND_cLT1);
        And aIsNull_AND_bLT1_AND_cLT1_AND_aLT1 = new And(EMPTY, aIsNull_AND_bLT1_AND_cLT1, lessThanOf(a, ONE));

        Expression optimized = propagateNullable(aIsNull_AND_bLT1_AND_cLT1_AND_aLT1);
        // ONE occurrences are replaced with null; LT(ONE, ONE) -> LT(null, null), LT(c, ONE) -> LT(c, null)
        Literal nullInt = nullOf(INTEGER);
        assertEquals(
            List.of(aIsNull, lessThanOf(nullInt, nullInt), lessThanOf(c, nullInt), lessThanOf(nullInt, nullInt)),
            Predicates.splitAnd(optimized)
        );
    }

    // (a IS NOT NULL OR b > 1) AND a IS NULL => OR(false, b > 1) AND a IS NULL
    // BooleanSimplification (next pass) will fold OR(false, b > 1) -> b > 1
    public void testIsNullPreservesOrDisjunctionBranch() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNull = new IsNull(EMPTY, fa);

        Expression gt = greaterThanOf(fb, ONE);
        var or = new Or(EMPTY, new IsNotNull(EMPTY, fa), gt);
        var and = new And(EMPTY, or, isNull);

        Expression optimized = propagateNullable(and);

        var falseLiteral = new Literal(EMPTY, Boolean.FALSE, BOOLEAN);
        var expectedOr = new Or(EMPTY, falseLiteral, gt);
        assertEquals(new And(EMPTY, expectedOr, isNull), optimized);
    }

    // (a IS NULL OR b > 1) AND a IS NULL => OR(true, b > 1) AND a IS NULL
    // BooleanSimplification (next pass) will fold OR(true, b > 1) -> true -> a IS NULL
    public void testIsNullPreservesIsNullBranchInOr() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNull = new IsNull(EMPTY, fa);

        Expression gt = greaterThanOf(fb, ONE);
        var or = new Or(EMPTY, new IsNull(EMPTY, fa), gt);
        var and = new And(EMPTY, or, isNull);

        Expression optimized = propagateNullable(and);

        var trueLiteral = new Literal(EMPTY, Boolean.TRUE, BOOLEAN);
        var expectedOr = new Or(EMPTY, trueLiteral, gt);
        assertEquals(new And(EMPTY, expectedOr, isNull), optimized);
    }

    // (a > 5 OR b > 1) AND a IS NULL => (null > 5 OR b > 1) AND a IS NULL
    // FoldNull (next pass) folds null > 5 -> null; BooleanSimplification folds OR(null, b > 1)
    public void testIsNullNullifiesFieldReferenceInOr() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNull = new IsNull(EMPTY, fa);

        Expression gt_b = greaterThanOf(fb, ONE);
        var or = new Or(EMPTY, greaterThanOf(fa, ONE), gt_b);
        var and = new And(EMPTY, or, isNull);

        Expression optimized = propagateNullable(and);

        Expression expectedOrLeft = greaterThanOf(nullOf(INTEGER), ONE);
        var expectedOr = new Or(EMPTY, expectedOrLeft, gt_b);
        assertEquals(new And(EMPTY, expectedOr, isNull), optimized);
    }

    // COALESCE(a, b) AND a IS NULL => COALESCE(b) AND a IS NULL
    // Direct arg 'a' is removed from COALESCE; 'b' has no reference to 'a' so transformDown leaves it unchanged.
    public void testCoalesceDirectArgNullified() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNull = new IsNull(EMPTY, fa);

        var coalesce = new Coalesce(EMPTY, fa, List.of(fb));
        var and = new And(EMPTY, coalesce, isNull);

        Expression optimized = propagateNullable(and);

        var expectedCoalesce = new Coalesce(EMPTY, fb, List.of());
        assertEquals(new And(EMPTY, expectedCoalesce, isNull), optimized);
    }

    // COALESCE(a, b + a) AND a IS NULL => COALESCE(b + null) AND a IS NULL
    // Direct arg 'a' is removed; the nested 'a' inside 'b + a' is also substituted by transformDown.
    public void testCoalesceDirectArgNullifiedAndNestedSubstituted() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNull = new IsNull(EMPTY, fa);

        var addExpr = new Add(EMPTY, fb, fa, TEST_CFG);
        var coalesce = new Coalesce(EMPTY, fa, List.of(addExpr));
        var and = new And(EMPTY, coalesce, isNull);

        Expression optimized = propagateNullable(and);

        var expectedAdd = new Add(EMPTY, fb, nullOf(INTEGER), TEST_CFG);
        var expectedCoalesce = new Coalesce(EMPTY, expectedAdd, List.of());
        assertEquals(new And(EMPTY, expectedCoalesce, isNull), optimized);
    }

    // COALESCE(a) AND a IS NULL => COALESCE(null) AND a IS NULL
    // When all direct args match the null field and are removed, the Coalesce branch is skipped
    // (no surviving children), so transformDown on the original COALESCE(a) substitutes a -> null.
    public void testCoalesceAllArgsRemovedFallsBackToTransformDown() {
        FieldAttribute fa = getFieldAttribute();
        var isNull = new IsNull(EMPTY, fa);

        var coalesce = new Coalesce(EMPTY, fa, List.of());
        var and = new And(EMPTY, coalesce, isNull);

        Expression optimized = propagateNullable(and);

        var expectedCoalesce = new Coalesce(EMPTY, nullOf(INTEGER), List.of());
        assertEquals(new And(EMPTY, expectedCoalesce, isNull), optimized);
    }

    // (a IS NOT NULL OR (b > 1 AND a > 5)) AND a IS NULL
    // => OR(false, AND(b > 1, null > 5)) AND a IS NULL
    // The nested 'a > 5' inside the AND branch is also nullified via transformDown.
    public void testIsNullInOrWithNestedAndBranch() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNull = new IsNull(EMPTY, fa);

        var innerAnd = new And(EMPTY, greaterThanOf(fb, ONE), greaterThanOf(fa, ONE));
        var or = new Or(EMPTY, new IsNotNull(EMPTY, fa), innerAnd);
        var and = new And(EMPTY, or, isNull);

        Expression optimized = propagateNullable(and);

        var expectedInnerAnd = new And(EMPTY, greaterThanOf(fb, ONE), greaterThanOf(nullOf(INTEGER), ONE));
        var expectedOr = new Or(EMPTY, new Literal(EMPTY, Boolean.FALSE, DataType.BOOLEAN), expectedInnerAnd);
        assertEquals(new And(EMPTY, expectedOr, isNull), optimized);
    }

    // (b IS NOT NULL OR c > 1) AND a IS NULL => unchanged
    // Neither 'b' nor 'c' is 'a', so anyMatch returns false and the OR is not nullified.
    public void testIsNullDoesNotModifyUnrelatedOrBranch() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        FieldAttribute fc = getFieldAttribute("c");
        var isNull = new IsNull(EMPTY, fa);

        var or = new Or(EMPTY, new IsNotNull(EMPTY, fb), greaterThanOf(fc, ONE));
        var and = new And(EMPTY, or, isNull);

        assertEquals(and, propagateNullable(and));
    }

    // a IS NULL AND b IS NULL AND (a > 1 OR b > 2)
    // => a IS NULL AND b IS NULL AND (null > 1 OR null > 2)
    // Both null constraints sequentially substitute their field in the shared OR expression.
    public void testMultipleIsNullConstraintsNullifyOr() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNullA = new IsNull(EMPTY, fa);
        var isNullB = new IsNull(EMPTY, fb);

        var or = new Or(EMPTY, greaterThanOf(fa, ONE), greaterThanOf(fb, TWO));
        var and = new And(EMPTY, isNullA, new And(EMPTY, isNullB, or));

        Expression optimized = propagateNullable(and);

        Literal nullInt = nullOf(INTEGER);
        var expectedOr = new Or(EMPTY, greaterThanOf(nullInt, ONE), greaterThanOf(nullInt, TWO));
        assertEquals(List.of(isNullA, isNullB, expectedOr), Predicates.splitAnd(optimized));
    }

    // --- nonNullify tests (symmetric: field is IS NOT NULL) ---

    // (a IS NULL OR b > 1) AND a IS NOT NULL => OR(false, b > 1) AND a IS NOT NULL
    // BooleanSimplification (next pass) will fold OR(false, b > 1) -> b > 1
    public void testIsNotNullPreservesOrDisjunctionBranch() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNotNull = new IsNotNull(EMPTY, fa);

        Expression gt = greaterThanOf(fb, ONE);
        var or = new Or(EMPTY, new IsNull(EMPTY, fa), gt);
        var and = new And(EMPTY, or, isNotNull);

        Expression optimized = propagateNullable(and);

        var falseLiteral = new Literal(EMPTY, Boolean.FALSE, BOOLEAN);
        var expectedOr = new Or(EMPTY, falseLiteral, gt);
        assertEquals(new And(EMPTY, expectedOr, isNotNull), optimized);
    }

    // (a IS NOT NULL OR b > 1) AND a IS NOT NULL => OR(true, b > 1) AND a IS NOT NULL
    // BooleanSimplification (next pass) will fold OR(true, b > 1) -> true, then a IS NOT NULL
    public void testIsNotNullPreservesIsNotNullBranchInOr() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        var isNotNull = new IsNotNull(EMPTY, fa);

        Expression gt = greaterThanOf(fb, ONE);
        var or = new Or(EMPTY, new IsNotNull(EMPTY, fa), gt);
        var and = new And(EMPTY, or, isNotNull);

        Expression optimized = propagateNullable(and);

        var trueLiteral = new Literal(EMPTY, Boolean.TRUE, BOOLEAN);
        var expectedOr = new Or(EMPTY, trueLiteral, gt);
        assertEquals(new And(EMPTY, expectedOr, isNotNull), optimized);
    }

    // a > 1 AND a IS NOT NULL => unchanged
    // nonNullify does NOT substitute the field value itself; we only know it is non-null.
    public void testIsNotNullDoesNotSubstituteFieldReference() {
        FieldAttribute fa = getFieldAttribute();
        var isNotNull = new IsNotNull(EMPTY, fa);

        var and = new And(EMPTY, greaterThanOf(fa, ONE), isNotNull);

        assertEquals(and, propagateNullable(and));
    }

    // (b IS NULL OR c > 1) AND a IS NOT NULL => unchanged
    // Neither 'b' nor 'c' is 'a', so the OR is not touched.
    public void testIsNotNullDoesNotModifyUnrelatedOrBranch() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        FieldAttribute fc = getFieldAttribute("c");
        var isNotNull = new IsNotNull(EMPTY, fa);

        var or = new Or(EMPTY, new IsNull(EMPTY, fb), greaterThanOf(fc, ONE));
        var and = new And(EMPTY, or, isNotNull);

        assertEquals(and, propagateNullable(and));
    }

    // (a IS NOT NULL OR b > 1 OR c > 2) AND a IS NULL
    // => OR(false, OR(b > 1, c > 2)) AND a IS NULL
    // All three OR branches are preserved; IS_NOT_NULL(a) becomes false, the rest unchanged.
    public void testIsNullInOrWithThreeBranches() {
        FieldAttribute fa = getFieldAttribute();
        FieldAttribute fb = getFieldAttribute("b");
        FieldAttribute fc = getFieldAttribute("c");
        var isNull = new IsNull(EMPTY, fa);

        Expression gt_b = greaterThanOf(fb, ONE);
        Expression gt_c = greaterThanOf(fc, TWO);
        var innerOr = new Or(EMPTY, gt_b, gt_c);
        var or = new Or(EMPTY, new IsNotNull(EMPTY, fa), innerOr);
        var and = new And(EMPTY, or, isNull);

        Expression optimized = propagateNullable(and);

        var falseLiteral = new Literal(EMPTY, Boolean.FALSE, DataType.BOOLEAN);
        var expectedOr = new Or(EMPTY, falseLiteral, innerOr);
        assertEquals(new And(EMPTY, expectedOr, isNull), optimized);
    }
}
