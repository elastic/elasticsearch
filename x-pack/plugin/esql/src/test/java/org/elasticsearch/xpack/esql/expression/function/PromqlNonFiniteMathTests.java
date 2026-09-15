/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;

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
 * ({@link Log}) need no wrap, and the tests construct them directly with the flag set.
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
