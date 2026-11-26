/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.test.ESTestCase;

public class FloatNarrowingTests extends ESTestCase {

    /** Every midpoint between two adjacent floats is one, from subnormals up to the one past {@link Float#MAX_VALUE}. */
    public void testMidpoints() {
        assertTrue(FloatNarrowing.isFloatMidpoint(Float.MIN_VALUE / 2.0));
        assertTrue(FloatNarrowing.isFloatMidpoint(((double) Float.MAX_VALUE + 0x1p128) / 2));
        assertTrue(FloatNarrowing.isFloatMidpoint(-((double) Float.MAX_VALUE + 0x1p128) / 2));
        for (int i = 0; i < 10_000; i++) {
            float f;
            do {
                f = Float.intBitsToFloat(randomInt());
            } while (Float.isFinite(f) == false || Math.abs(f) == Float.MAX_VALUE);
            double midpoint = ((double) f + (double) Math.nextUp(f)) / 2;
            assertTrue(f + " up", FloatNarrowing.isFloatMidpoint(midpoint));
            assertFalse(f + " up + ulp", FloatNarrowing.isFloatMidpoint(Math.nextUp(midpoint)));
            assertFalse(f + " up - ulp", FloatNarrowing.isFloatMidpoint(Math.nextDown(midpoint)));
        }
    }

    public void testNotMidpoints() {
        assertFalse(FloatNarrowing.isFloatMidpoint(0.0));
        assertFalse(FloatNarrowing.isFloatMidpoint(Double.NaN));
        assertFalse(FloatNarrowing.isFloatMidpoint(Double.POSITIVE_INFINITY));
        assertFalse(FloatNarrowing.isFloatMidpoint(Double.NEGATIVE_INFINITY));
        assertFalse(FloatNarrowing.isFloatMidpoint(Float.MAX_VALUE));
        assertFalse(FloatNarrowing.isFloatMidpoint(0.0123));
        for (int i = 0; i < 10_000; i++) {
            float f = Float.intBitsToFloat(randomInt());
            assertFalse(Float.toString(f), FloatNarrowing.isFloatMidpoint(f));
        }
    }

    /** The low-bits rejection agrees with the full computation, which does not rely on it. */
    public void testAgreesWithFullComputation() {
        for (int i = 0; i < 100_000; i++) {
            double d = randomBoolean() ? Double.longBitsToDouble(randomLong()) : randomDouble();
            if (randomBoolean()) {
                // clear the low bits so that d is often near a float or on a midpoint
                d = Double.longBitsToDouble(Double.doubleToRawLongBits(d) & ~0x0FFFFFFFL);
            }
            assertEquals(Double.toString(d), isFloatMidpointFull(d), FloatNarrowing.isFloatMidpoint(d));
        }
    }

    /** {@link FloatNarrowing#isFloatMidpoint} without the low-bits rejection. */
    private static boolean isFloatMidpointFull(double d) {
        final float f = (float) d;
        if ((double) f == d || Double.isNaN(d) || Double.isInfinite(d)) {
            return false;
        }
        final float down = (double) f < d ? f : Math.nextDown(f);
        final float up = (double) f > d ? f : Math.nextUp(f);
        final double downValue = Float.isInfinite(down) ? Math.copySign(0x1p128, down) : down;
        final double upValue = Float.isInfinite(up) ? Math.copySign(0x1p128, up) : up;
        return d == (downValue + upValue) / 2;
    }
}
