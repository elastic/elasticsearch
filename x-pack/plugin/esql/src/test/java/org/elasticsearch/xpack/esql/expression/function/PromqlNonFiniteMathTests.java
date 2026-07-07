/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
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

import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for the non-finite-preserving variants of math expressions used by PromQL translation. In a PromQL
 * context, operations whose IEEE-754 result is {@code NaN} or {@code ±Inf} must surface that value rather than be
 * rejected to {@code null} (which would drop the series). Each expression exposes a constructor variant that enables
 * this behavior; the default ES|QL constructors keep rejecting non-finite results to {@code null} and are covered by
 * the per-function {@code *Tests} suites.
 * <p>
 * PromQL translation also guarantees the IEEE-754 ({@code double}) path is taken regardless of the metric's stored type.
 * For functions with type-specific evaluators (e.g. {@link Sqrt}, {@link Log10}) the input is wrapped in {@link ToDouble};
 * these tests mirror that by wrapping non-double inputs in {@link ToDouble}, the same shape produced by
 * {@code unaryNonFiniteValueTransformation}. Functions that already cast their operands to {@code double} internally
 * (the trigonometric functions and {@link Log}) need no wrap, and the tests construct them directly with the flag set.
 */
public class PromqlNonFiniteMathTests extends ESTestCase {

    /**
     * {@code sqrt(-x)} yields {@code NaN} (IEEE-754) when the non-finite-preserving variant is used. The fold runs
     * without throwing, so no warning is registered.
     */
    public void testSqrtOfNegativeDoubleYieldsNaN() {
        assertFoldsToNaN(new Sqrt(Source.EMPTY, Literal.fromDouble(Source.EMPTY, -1.0), true));
    }

    /**
     * A negative {@code long} metric must also yield {@code NaN}: the input is coerced to {@code double} so the
     * negative-domain guard is the lenient (double) one rather than the strict long evaluator that would drop the series.
     */
    public void testSqrtOfNegativeLongYieldsNaN() {
        Expression negativeLong = new Literal(Source.EMPTY, -5L, DataType.LONG);
        assertFoldsToNaN(new Sqrt(Source.EMPTY, new ToDouble(Source.EMPTY, negativeLong), true));
    }

    /**
     * A negative {@code integer} metric must also yield {@code NaN}, for the same reason as the {@code long} case.
     */
    public void testSqrtOfNegativeIntYieldsNaN() {
        Expression negativeInt = new Literal(Source.EMPTY, -5, DataType.INTEGER);
        assertFoldsToNaN(new Sqrt(Source.EMPTY, new ToDouble(Source.EMPTY, negativeInt), true));
    }

    /** {@code log10(-x)} yields {@code NaN} and {@code log10(0)} yields {@code -Inf} (IEEE-754). */
    public void testLog10NonPositiveYieldsNonFinite() {
        assertFoldsToNaN(new Log10(Source.EMPTY, Literal.fromDouble(Source.EMPTY, -1.0), true));
        assertFoldsTo(new Log10(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 0.0), true), Double.NEGATIVE_INFINITY);
    }

    /** A negative {@code long} metric must also yield {@code NaN}, via the {@code double}-coerced lenient path. */
    public void testLog10OfNegativeLongYieldsNaN() {
        Expression negativeLong = new Literal(Source.EMPTY, -5L, DataType.LONG);
        assertFoldsToNaN(new Log10(Source.EMPTY, new ToDouble(Source.EMPTY, negativeLong), true));
    }

    /** {@code ln(-x)} yields {@code NaN} and {@code ln(0)} yields {@code -Inf} (IEEE-754). */
    public void testLnNonPositiveYieldsNonFinite() {
        assertFoldsToNaN(new Log(Source.EMPTY, Literal.fromDouble(Source.EMPTY, -1.0), null, true));
        assertFoldsTo(new Log(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 0.0), null, true), Double.NEGATIVE_INFINITY);
    }

    /** {@code log2(-x)} yields {@code NaN} and {@code log2(0)} yields {@code -Inf} (IEEE-754). */
    public void testLog2NonPositiveYieldsNonFinite() {
        assertFoldsToNaN(log2(Literal.fromDouble(Source.EMPTY, -1.0)));
        assertFoldsTo(log2(Literal.fromDouble(Source.EMPTY, 0.0)), Double.NEGATIVE_INFINITY);
    }

    /** A negative {@code long} metric must also yield {@code NaN} for natural log, via the {@code double}-coerced path. */
    public void testLnOfNegativeLongYieldsNaN() {
        Expression negativeLong = new Literal(Source.EMPTY, -5L, DataType.LONG);
        assertFoldsToNaN(new Log(Source.EMPTY, new ToDouble(Source.EMPTY, negativeLong), null, true));
    }

    /** {@code asin}/{@code acos} of an out-of-range input ({@code |x|>1}) yields {@code NaN}. */
    public void testAsinAcosOutOfRangeYieldsNaN() {
        assertFoldsToNaN(new Asin(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 2.0), true));
        assertFoldsToNaN(new Acos(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 2.0), true));
    }

    /** {@code acosh} of an input below 1 yields {@code NaN}. */
    public void testAcoshBelowOneYieldsNaN() {
        assertFoldsToNaN(new Acosh(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 0.5), true));
    }

    /** {@code atanh(±1)} yields {@code ±Inf} and {@code atanh(|x|>1)} yields {@code NaN} (IEEE-754). */
    public void testAtanhBoundaryYieldsNonFinite() {
        assertFoldsTo(new Atanh(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 1.0), true), Double.POSITIVE_INFINITY);
        assertFoldsTo(new Atanh(Source.EMPTY, Literal.fromDouble(Source.EMPTY, -1.0), true), Double.NEGATIVE_INFINITY);
        assertFoldsToNaN(new Atanh(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 2.0), true));
    }

    /** The issue's headline case: {@code metric * Inf} surfaces {@code ±Inf}, and {@code metric * NaN} surfaces {@code NaN}. */
    public void testMulByNonFiniteScalarIsPreserved() {
        assertFoldsTo(mul(2.0, Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
        assertFoldsTo(mul(-2.0, Double.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY);
        assertFoldsToNaN(mul(2.0, Double.NaN));
        // 0 * Inf is NaN under IEEE-754, and must be surfaced rather than dropped.
        assertFoldsToNaN(mul(0.0, Double.POSITIVE_INFINITY));
    }

    /** {@code pow(-1, 0.5)} yields {@code NaN} and {@code pow(0, -1)} yields {@code +Inf} (IEEE-754). */
    public void testPowNonFiniteIsPreserved() {
        assertFoldsToNaN(new Pow(Source.EMPTY, Literal.fromDouble(Source.EMPTY, -1.0), Literal.fromDouble(Source.EMPTY, 0.5), true));
        assertFoldsTo(
            new Pow(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 0.0), Literal.fromDouble(Source.EMPTY, -1.0), true),
            Double.POSITIVE_INFINITY
        );
    }

    /** Non-finite sums/differences ({@code MAX+MAX → +Inf}, {@code Inf-Inf → NaN}) are preserved. */
    public void testAddSubNonFiniteIsPreserved() {
        assertFoldsTo(add(Double.MAX_VALUE, Double.MAX_VALUE), Double.POSITIVE_INFINITY);
        assertFoldsToNaN(add(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        assertFoldsTo(sub(-Double.MAX_VALUE, Double.MAX_VALUE), Double.NEGATIVE_INFINITY);
        assertFoldsToNaN(sub(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
    }

    /**
     * Division by zero matches Prometheus: {@code x/0 → ±Inf} and {@code 0/0 → NaN}, with the series kept. Verified
     * against Prometheus {@code vectorElemBinop} (DIV returns {@code lhs / rhs} with {@code keep == true}).
     */
    public void testDivByZeroFollowsPrometheus() {
        assertFoldsTo(div(1.0, 0.0), Double.POSITIVE_INFINITY);
        assertFoldsTo(div(-1.0, 0.0), Double.NEGATIVE_INFINITY);
        assertFoldsToNaN(div(0.0, 0.0));
    }

    private static Mul mul(double lhs, double rhs) {
        return new Mul(Source.EMPTY, Literal.fromDouble(Source.EMPTY, lhs), Literal.fromDouble(Source.EMPTY, rhs), true);
    }

    private static Add add(double lhs, double rhs) {
        return new Add(
            Source.EMPTY,
            Literal.fromDouble(Source.EMPTY, lhs),
            Literal.fromDouble(Source.EMPTY, rhs),
            EsqlTestUtils.TEST_CFG,
            true
        );
    }

    private static Sub sub(double lhs, double rhs) {
        return new Sub(
            Source.EMPTY,
            Literal.fromDouble(Source.EMPTY, lhs),
            Literal.fromDouble(Source.EMPTY, rhs),
            EsqlTestUtils.TEST_CFG,
            true
        );
    }

    private static Div div(double lhs, double rhs) {
        return new Div(Source.EMPTY, Literal.fromDouble(Source.EMPTY, lhs), Literal.fromDouble(Source.EMPTY, rhs), null, true);
    }

    /**
     * The remainder matches Prometheus {@code math.Mod}: {@code x % 0 → NaN} with the series kept, while a regular
     * remainder is unchanged.
     */
    public void testModByZeroFollowsPrometheus() {
        assertFoldsToNaN(new Mod(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 5.0), Literal.fromDouble(Source.EMPTY, 0.0), true));
        assertFoldsTo(new Mod(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 5.0), Literal.fromDouble(Source.EMPTY, 3.0), true), 2.0);
    }

    /** {@code sinh}/{@code cosh} overflow to {@code ±Inf}/{@code +Inf} instead of being dropped. */
    public void testSinhCoshOverflowYieldsInf() {
        assertFoldsTo(new Sinh(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 1000.0), true), Double.POSITIVE_INFINITY);
        assertFoldsTo(new Sinh(Source.EMPTY, Literal.fromDouble(Source.EMPTY, -1000.0), true), Double.NEGATIVE_INFINITY);
        assertFoldsTo(new Cosh(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 1000.0), true), Double.POSITIVE_INFINITY);
    }

    /** {@code deg} (to_degrees) preserves non-finite inputs and an overflowing result ({@code +Inf}). */
    public void testToDegreesNonFiniteIsPreserved() {
        assertFoldsTo(new ToDegrees(Source.EMPTY, Literal.fromDouble(Source.EMPTY, Double.MAX_VALUE), true), Double.POSITIVE_INFINITY);
        assertFoldsTo(
            new ToDegrees(Source.EMPTY, Literal.fromDouble(Source.EMPTY, Double.POSITIVE_INFINITY), true),
            Double.POSITIVE_INFINITY
        );
        assertFoldsToNaN(new ToDegrees(Source.EMPTY, Literal.fromDouble(Source.EMPTY, Double.NaN), true));
    }

    private static Log log2(Expression value) {
        return new Log(Source.EMPTY, Literal.fromDouble(Source.EMPTY, 2.0), value, true);
    }

    private static void assertFoldsToNaN(Expression expression) {
        Object result = expression.fold(FoldContext.small());
        assertThat(result, instanceOf(Double.class));
        assertTrue("expected NaN but got [" + result + "]", Double.isNaN((Double) result));
    }

    private static void assertFoldsTo(Expression expression, double expected) {
        Object result = expression.fold(FoldContext.small());
        assertThat(result, instanceOf(Double.class));
        assertEquals("unexpected fold result", expected, (Double) result, 0.0);
    }
}
