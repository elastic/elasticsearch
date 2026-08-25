/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SubstituteTransportVersionAwareExpressionsTests extends ESTestCase {
    private static final TransportVersion ESQL_SUM_LONG_OVERFLOW_FIX = TransportVersion.fromName("esql_sum_long_overflow_fix");
    private static final TransportVersion ESQL_PROMQL_NON_FINITE_MATH = TransportVersion.fromName("esql_promql_non_finite_math");

    public void testSumNotReplacedWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, oldVersion);
        assertThat(result, sameInstance(sum));
    }

    public void testSumReplacedWithCurrentVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).longOverflowMode(), is(Sum.LONG_OVERFLOW_WARN));
        assertThat(result, not(sameInstance(sum)));
    }

    /**
     * Checks that if an overflowing sum receives an old transport version, it won't be changed.
     * <p>
     *     This tests idempotence.
     * </p>
     */
    public void testSumAlreadyOverflowingWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(
            EMPTY,
            field,
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            SummationMode.COMPENSATED_LITERAL,
            Sum.LONG_OVERFLOW_THROW
        );
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, oldVersion);
        assertThat(result, sameInstance(sum));
    }

    /**
     * Checks that an overflowing sum with a new transport version gets upgraded to safe long mode.
     */
    public void testSumOverflowingWithNewVersionUpgraded() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(
            EMPTY,
            field,
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            SummationMode.COMPENSATED_LITERAL,
            Sum.LONG_OVERFLOW_THROW
        );
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).longOverflowMode(), is(Sum.LONG_OVERFLOW_WARN));
        assertThat(result, not(sameInstance(sum)));
    }

    /**
     * Checks that a safe long sum with a new transport version is not changed (idempotent).
     */
    public void testSumAlreadySafeWithNewVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(
            EMPTY,
            field,
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            SummationMode.COMPENSATED_LITERAL,
            Sum.LONG_OVERFLOW_WARN
        );
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, sameInstance(sum));
    }

    public void testSumDoubleFieldWithNewVersion() {
        Expression field = getFieldAttribute("f", DataType.DOUBLE);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).longOverflowMode(), is(Sum.LONG_OVERFLOW_WARN));
    }

    public void testNonTransportVersionAwareUnchanged() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(field, oldVersion);
        assertThat(result, sameInstance(field));
    }

    public void testNonFiniteUnaryMathDowngradedWithOldVersion() {
        assertNonFiniteMathDowngradedAndIdempotent(new Sqrt(EMPTY, getFieldAttribute("f", DataType.DOUBLE), true));
    }

    public void testNonFiniteUnaryMathNotChangedWithCurrentVersion() {
        Expression lenient = new Sqrt(EMPTY, getFieldAttribute("f", DataType.DOUBLE), true);
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_PROMQL_NON_FINITE_MATH);
        assertThat(SubstituteTransportVersionAwareExpressions.rule(lenient, newVersion), sameInstance(lenient));
    }

    public void testStrictMathUnchangedWithOldVersion() {
        Expression strict = new Sqrt(EMPTY, getFieldAttribute("f", DataType.DOUBLE), false);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_PROMQL_NON_FINITE_MATH);
        assertThat(SubstituteTransportVersionAwareExpressions.rule(strict, oldVersion), sameInstance(strict));
    }

    public void testNonFiniteBinaryMathDowngradedWithOldVersion() {
        Expression left = getFieldAttribute("l", DataType.DOUBLE);
        Expression right = getFieldAttribute("r", DataType.DOUBLE);
        assertNonFiniteMathDowngradedAndIdempotent(new Pow(EMPTY, left, right, true));
        assertNonFiniteMathDowngradedAndIdempotent(new Mul(EMPTY, left, right, true));
        assertNonFiniteMathDowngradedAndIdempotent(new Add(EMPTY, left, right, TEST_CFG, true));
        assertNonFiniteMathDowngradedAndIdempotent(new Sub(EMPTY, left, right, TEST_CFG, true));
        assertNonFiniteMathDowngradedAndIdempotent(new Div(EMPTY, left, right, DataType.DOUBLE, true));
        assertNonFiniteMathDowngradedAndIdempotent(new Mod(EMPTY, left, right, true));
    }

    public void testNonFiniteArithmeticDowngradePreservesConfiguration() {
        Add add = new Add(EMPTY, getFieldAttribute("l", DataType.DOUBLE), getFieldAttribute("r", DataType.DOUBLE), TEST_CFG, true);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_PROMQL_NON_FINITE_MATH);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(add, oldVersion);
        assertThat(result, instanceOf(Add.class));
        assertThat(result, not(sameInstance(add)));
        assertFalse(((Add) result).allowNonFinite());
        assertThat(((Add) result).configuration(), sameInstance(TEST_CFG));
    }

    public void testNonFiniteLogPreservesUnaryAndBinaryForms() {
        Expression value = getFieldAttribute("v", DataType.DOUBLE);
        Expression base = getFieldAttribute("b", DataType.DOUBLE);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_PROMQL_NON_FINITE_MATH);

        Expression downgradedUnary = SubstituteTransportVersionAwareExpressions.rule(new Log(EMPTY, value, null, true), oldVersion);
        assertThat(downgradedUnary, instanceOf(Log.class));
        assertThat(downgradedUnary.children(), hasSize(1));

        Expression downgradedBinary = SubstituteTransportVersionAwareExpressions.rule(new Log(EMPTY, base, value, true), oldVersion);
        assertThat(downgradedBinary, instanceOf(Log.class));
        assertThat(downgradedBinary.children(), hasSize(2));
    }

    /**
     * A non-finite-preserving expression and its strict variant evaluate different math, so they must not compare equal.
     * Expression tree transformations detect changes via {@link Expression#equals}; if the two variants are equal, every
     * substitution of one for the other is silently discarded.
     */
    public void testLenientAndStrictVariantsAreNotEqual() {
        Expression f = getFieldAttribute("f", DataType.DOUBLE);
        Expression g = getFieldAttribute("g", DataType.DOUBLE);

        assertVariantsDiffer(new Sqrt(EMPTY, f, true), new Sqrt(EMPTY, f, false));
        assertVariantsDiffer(new Log10(EMPTY, f, true), new Log10(EMPTY, f, false));
        assertVariantsDiffer(new Log(EMPTY, f, g, true), new Log(EMPTY, f, g, false));
        assertVariantsDiffer(new ToDegrees(EMPTY, f, true), new ToDegrees(EMPTY, f, false));
        assertVariantsDiffer(new Acos(EMPTY, f, true), new Acos(EMPTY, f, false));
        assertVariantsDiffer(new Acosh(EMPTY, f, true), new Acosh(EMPTY, f, false));
        assertVariantsDiffer(new Asin(EMPTY, f, true), new Asin(EMPTY, f, false));
        assertVariantsDiffer(new Atanh(EMPTY, f, true), new Atanh(EMPTY, f, false));
        assertVariantsDiffer(new Cosh(EMPTY, f, true), new Cosh(EMPTY, f, false));
        assertVariantsDiffer(new Sinh(EMPTY, f, true), new Sinh(EMPTY, f, false));
        assertVariantsDiffer(new Pow(EMPTY, f, g, true), new Pow(EMPTY, f, g, false));
        assertVariantsDiffer(new Add(EMPTY, f, g, TEST_CFG, true), new Add(EMPTY, f, g, TEST_CFG, false));
        assertVariantsDiffer(new Sub(EMPTY, f, g, TEST_CFG, true), new Sub(EMPTY, f, g, TEST_CFG, false));
        assertVariantsDiffer(new Mul(EMPTY, f, g, true), new Mul(EMPTY, f, g, false));
        assertVariantsDiffer(new Div(EMPTY, f, g, DataType.DOUBLE, true), new Div(EMPTY, f, g, DataType.DOUBLE, false));
        assertVariantsDiffer(new Mod(EMPTY, f, g, true), new Mod(EMPTY, f, g, false));
    }

    /**
     * The variants must differ under both {@code equals} and {@code hashCode}. Transformations detect a substitution
     * via {@code equals}, and expressions are also used as map keys, so an {@code equals} override without a matching
     * {@code hashCode} would leave the two variants colliding.
     */
    private static void assertVariantsDiffer(Expression lenient, Expression strict) {
        String name = lenient.getClass().getSimpleName();
        assertNotEquals(name, lenient, strict);
        assertNotEquals(name, lenient.hashCode(), strict.hashCode());
    }

    /**
     * The downgrade must survive being applied through a {@link LogicalPlan}, which is how the rule runs in production.
     * A nested expression is only rebuilt when the transformation observes that a child changed, so a downgrade that is
     * not visible to {@link Expression#equals} never reaches the plan.
     */
    public void testNonFiniteDowngradeAppliedThroughPlan() {
        Expression field = getFieldAttribute("f", DataType.DOUBLE);
        Alias lenient = new Alias(EMPTY, "x", new Sqrt(EMPTY, field, true));
        LogicalPlan plan = new Eval(EMPTY, relation(), List.of(lenient));

        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_PROMQL_NON_FINITE_MATH);
        LogicalPlan optimized = new SubstituteTransportVersionAwareExpressions().apply(
            plan,
            new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), oldVersion)
        );

        Expression evaluated = ((Eval) optimized).fields().getFirst().child();
        assertThat(evaluated, instanceOf(Sqrt.class));
        assertFalse("lenient math must be downgraded on a cluster that predates non-finite support", ((Sqrt) evaluated).allowNonFinite());
    }

    /**
     * On an old cluster the non-finite-preserving variant is downgraded to a new (strict) instance of the same type.
     */
    private static void assertNonFiniteMathDowngradedAndIdempotent(Expression lenient) {
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_PROMQL_NON_FINITE_MATH);
        Expression downgraded = SubstituteTransportVersionAwareExpressions.rule(lenient, oldVersion);
        assertThat(downgraded, instanceOf(lenient.getClass()));
        assertThat(downgraded, not(sameInstance(lenient)));
        assertFalse(((NonFiniteSupport) downgraded).allowNonFinite());
        assertThat(SubstituteTransportVersionAwareExpressions.rule(downgraded, oldVersion), sameInstance(downgraded));
    }

    private static EsRelation relation() {
        return new EsRelation(EMPTY, randomIdentifier(), IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
    }
}
