/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.operator.math;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Tests {@link Maths#round} and {@link Maths#truncate} for whole numbers with a negative precision.
 *
 * <p>Rounding at a negative precision used to take a truncation shortcut: a number with no more digits
 * than the scale was assumed to round to zero. That holds for truncation, but not for rounding, because
 * a leading digit of 5 or more rounds up to the next power of ten. {@code ROUND(5, -1)} returned 0
 * rather than 10, while {@code ROUND(5.0, -1)} - which never used that shortcut - returned 10.0.
 */
public class MathsTests extends ESTestCase {

    public void testRoundsUpToTheNextPowerOfTen() {
        assertEquals(10L, (long) Maths.round(5L, -1));
        assertEquals(10L, (long) Maths.round(9L, -1));
        assertEquals(100L, (long) Maths.round(50L, -2));
        assertEquals(100L, (long) Maths.round(99L, -2));
        assertEquals(1000L, (long) Maths.round(999L, -3));
    }

    public void testRoundsDownWhenBelowHalfTheScale() {
        assertEquals(0L, (long) Maths.round(4L, -1));
        assertEquals(0L, (long) Maths.round(49L, -2));
        assertEquals(0L, (long) Maths.round(499L, -3));
    }

    /**
     * Rounding is half away from zero, so negative values round away from zero too.
     */
    public void testRoundsNegativeValuesAwayFromZero() {
        assertEquals(-10L, (long) Maths.round(-5L, -1));
        assertEquals(-10L, (long) Maths.round(-9L, -1));
        assertEquals(-100L, (long) Maths.round(-50L, -2));
        assertEquals(0L, (long) Maths.round(-4L, -1));
    }

    /**
     * Values with more digits than the scale never took the shortcut and were already correct. They are
     * covered here because the fix reworked the arithmetic they run through.
     */
    public void testRoundsValuesLongerThanTheScale() {
        assertEquals(20L, (long) Maths.round(15L, -1));
        assertEquals(12000L, (long) Maths.round(12345L, -3));
        assertEquals(0L, (long) Maths.round(12345L, -10));
        assertEquals(-20L, (long) Maths.round(-15L, -1));
    }

    /**
     * The same value must round identically whether it arrives as a whole number or as a double.
     * These disagreed before the fix: the long path returned 0 and the double path returned 10.0.
     */
    public void testWholeNumbersAgreeWithDoubles() {
        for (long value : new long[] { 5L, 9L, 50L, 99L, 999L, -5L, -50L, 4L, 15L }) {
            for (int precision : new int[] { -1, -2, -3 }) {
                double asDouble = Maths.round(Double.valueOf(value), precision).doubleValue();
                long asLong = Maths.round(value, precision);
                assertEquals("ROUND(" + value + ", " + precision + ")", asDouble, asLong, 0.0);
            }
        }
    }

    public void testThrowsWhenTheRoundedValueDoesNotFitInALong() {
        // rounds to 9223372036854775810, one step beyond Long.MAX_VALUE
        expectThrows(ArithmeticException.class, () -> Maths.round(Long.MAX_VALUE, -1));
        // rounds to +/-10^19, which no long can hold
        expectThrows(ArithmeticException.class, () -> Maths.round(5_000_000_000_000_000_000L, -19));
        expectThrows(ArithmeticException.class, () -> Maths.round(Long.MIN_VALUE, -19));
    }

    public void testRoundsToZeroBelowHalfOfTenPowNineteen() {
        assertEquals(0L, (long) Maths.round(4_999_999_999_999_999_999L, -19));
        assertEquals(0L, (long) Maths.round(-4_999_999_999_999_999_999L, -19));
    }

    /**
     * {@code -Long.MIN_VALUE} overflows back to {@code Long.MIN_VALUE}, which used to leave the scale
     * negative: rounding then skipped its zero shortcut and returned a value built from a wrapped scale,
     * and truncating divided by zero.
     */
    public void testHandlesPrecisionOfLongMinValue() {
        assertEquals(0L, (long) Maths.round(12345L, Long.MIN_VALUE));
        assertEquals(0L, (long) Maths.round(-12345L, Long.MIN_VALUE));
        assertEquals(0L, (long) Maths.round(1L, Long.MIN_VALUE));
        assertEquals(0L, (Maths.truncate(12345L, Long.MIN_VALUE)).longValue());
        assertEquals(0L, (Maths.truncate(-12345L, Long.MIN_VALUE)).longValue());
    }

    /**
     * Truncation genuinely does drop to zero once the scale covers every digit, so the shortcut the
     * rounding path lost is still correct here.
     */
    public void testTruncateStillDropsToZero() {
        assertEquals(0L, (Maths.truncate(5L, -1)).longValue());
        assertEquals(0L, (Maths.truncate(9L, -1)).longValue());
        assertEquals(0L, (Maths.truncate(99L, -2)).longValue());
        assertEquals(12000L, (Maths.truncate(12345L, -3)).longValue());
    }

    /**
     * Compares against {@link BigDecimal} with {@link RoundingMode#HALF_UP}, which is half away from
     * zero and so matches the documented behaviour of {@code ROUND}.
     */
    public void testMatchesBigDecimalForRandomValues() {
        for (int i = 0; i < 2000; i++) {
            long value = switch (between(0, 2)) {
                case 0 -> randomLong();
                case 1 -> between(-100_000, 100_000);
                default -> randomLongBetween(-1_000_000_000L, 1_000_000_000L);
            };
            if (value == 0) {
                continue;
            }
            int precision = between(-19, 0);

            BigInteger expected = new BigDecimal(BigInteger.valueOf(value)).setScale(precision, RoundingMode.HALF_UP).toBigInteger();
            boolean fits = expected.bitLength() <= 63;

            String description = "ROUND(" + value + ", " + precision + ")";
            if (fits) {
                assertEquals(description, expected.longValueExact(), (long) Maths.round(value, precision));
            } else {
                expectThrows(ArithmeticException.class, description, () -> Maths.round(value, precision));
            }
        }
    }
}
