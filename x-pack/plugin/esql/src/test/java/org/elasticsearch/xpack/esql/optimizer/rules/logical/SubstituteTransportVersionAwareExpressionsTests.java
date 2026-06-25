/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;

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
    }

    /**
     * Arithmetic operations that carry a {@link org.elasticsearch.xpack.esql.session.Configuration} must keep it when
     * downgraded to the strict variant.
     */
    public void testNonFiniteArithmeticDowngradePreservesConfiguration() {
        Add add = new Add(EMPTY, getFieldAttribute("l", DataType.DOUBLE), getFieldAttribute("r", DataType.DOUBLE), TEST_CFG, true);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_PROMQL_NON_FINITE_MATH);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(add, oldVersion);
        assertThat(result, instanceOf(Add.class));
        assertThat(result, not(sameInstance(add)));
        assertThat(((Add) result).configuration(), sameInstance(TEST_CFG));
    }

    /**
     * {@code log} can be unary ({@code ln}) or binary; the downgrade must preserve whichever arity the original had.
     */
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
     * On an old cluster the non-finite-preserving variant is downgraded to a new (strict) instance of the same type.
     * Re-applying the rule to that result is a no-op, which confirms the non-finite flag was actually cleared (the flag
     * is captured in the node's constructor lambda rather than exposed as a property, so it cannot be asserted directly).
     */
    private static void assertNonFiniteMathDowngradedAndIdempotent(Expression lenient) {
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_PROMQL_NON_FINITE_MATH);
        Expression downgraded = SubstituteTransportVersionAwareExpressions.rule(lenient, oldVersion);
        assertThat(downgraded, instanceOf(lenient.getClass()));
        assertThat(downgraded, not(sameInstance(lenient)));
        assertThat(SubstituteTransportVersionAwareExpressions.rule(downgraded, oldVersion), sameInstance(downgraded));
    }
}
