/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

/**
 * Narrowing a batch {@code double} to the {@code float} the row path would have parsed from the same JSON text.
 *
 * <p>The row path parses a decimal mapped as a float straight to {@code float}, which is correctly rounded. A batch carries the
 * value as the correctly rounded {@code double} instead, and {@code (float) d} rounds a second time. Both roundings agree except
 * when {@code d} is exactly the midpoint between two adjacent floats: rounding to the nearest double is monotonic and every float
 * midpoint is itself a double, so a text below a midpoint never gives a {@code d} above it and vice versa, and only a {@code d}
 * that lands on the midpoint loses which side the text was on. Only in that case does the text have to be parsed again, which
 * {@link #isFloatMidpoint} tells callers.
 */
public final class FloatNarrowing {

    /** The value one float ulp past {@link Float#MAX_VALUE}, where a float rounds to infinity; a double, as it is no float. */
    private static final double FLOAT_OVERFLOW = 0x1p128;

    private FloatNarrowing() {}

    /**
     * Whether {@code (float) d} may differ from parsing the text {@code d} was parsed from straight to {@code float}: true only when
     * {@code d} is exactly halfway between two adjacent floats. Callers then parse the text with {@link Float#parseFloat}.
     */
    public static boolean isFloatMidpoint(double d) {
        final float f = (float) d;
        if ((double) f == d || Double.isNaN(d) || Double.isInfinite(d)) {
            return false;
        }
        // The two floats around d. Past Float.MAX_VALUE, (float) d is infinite and the float above is taken as 2^128.
        final float down = (double) f < d ? f : Math.nextDown(f);
        final float up = (double) f > d ? f : Math.nextUp(f);
        final double downValue = Float.isInfinite(down) ? Math.copySign(FLOAT_OVERFLOW, down) : down;
        final double upValue = Float.isInfinite(up) ? Math.copySign(FLOAT_OVERFLOW, up) : up;
        // Exact: two adjacent floats need at most 25 significant bits, so their sum and its half are doubles.
        return d == (downValue + upValue) / 2;
    }

    /**
     * Narrows {@code d} as the row path parses {@code text} to {@code float}. {@code text} must be the text {@code d} was parsed
     * from, or any text whose nearest double is {@code d} and whose nearest float is the original one, such as the
     * {@code Double.toString} rendering when the client wrote exactly that.
     */
    public static float narrow(double d, String text) {
        return isFloatMidpoint(d) ? Float.parseFloat(text) : (float) d;
    }
}
