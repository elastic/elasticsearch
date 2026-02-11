/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.Matchers.equalTo;

public class FastMathTests extends ESTestCase {

    // Maximum relative error allowed for acosh comparisons.
    // The log1p-based formula for values close to 1 can have up to ~1.5e-13 relative error.
    private static final double ACOSH_RELATIVE_ERROR = 2e-13;

    // Canonical acosh formula used as reference for comparison
    private static double canonicalAcosh(double x) {
        return Math.log(x + Math.sqrt(x * x - 1));
    }

    private static Matcher<Double> closeTo(double expected, double maxRelativeError) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Double actual) {
                double absExpected = Math.abs(expected);
                double absDiff = Math.abs(expected - actual);
                if (absExpected < 1e-15) {
                    return absDiff <= maxRelativeError;
                }
                return (absDiff / absExpected) <= maxRelativeError;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a numeric value within relative error ")
                    .appendValue(maxRelativeError)
                    .appendText(" of ")
                    .appendValue(expected);
            }

            @Override
            protected void describeMismatchSafely(Double actual, Description mismatchDescription) {
                double absExpected = Math.abs(expected);
                double absDiff = Math.abs(expected - actual);
                double relError = absExpected < 1e-15 ? absDiff : absDiff / absExpected;
                mismatchDescription.appendValue(actual).appendText(" had relative error ").appendValue(relError);
            }
        };
    }

    public void testAcoshNaN() {
        assertTrue(Double.isNaN(FastMath.acosh(Double.NaN)));
    }

    public void testAcoshLessThanOne() {
        assertTrue(Double.isNaN(FastMath.acosh(0.5)));
        assertTrue(Double.isNaN(FastMath.acosh(0.0)));
        assertTrue(Double.isNaN(FastMath.acosh(-1.0)));
        assertTrue(Double.isNaN(FastMath.acosh(Double.NEGATIVE_INFINITY)));
    }

    public void testAcoshOne() {
        assertThat(FastMath.acosh(1.0), equalTo(0.0));
    }

    public void testAcoshPositiveInfinity() {
        assertThat(FastMath.acosh(Double.POSITIVE_INFINITY), equalTo(Double.POSITIVE_INFINITY));
    }

    public void testAcoshCloseToOne() {
        // Values in range (1, 2] use the log1p formula for better precision
        double[] values = { 1.0001, 1.001, 1.01, 1.1, 1.5, 2.0 };
        for (double x : values) {
            assertThat("acosh(" + x + ")", FastMath.acosh(x), closeTo(canonicalAcosh(x), ACOSH_RELATIVE_ERROR));
        }
    }

    public void testAcoshModerateValues() {
        // Values in range (2, 2^28) use the standard formula
        double[] values = { 2.5, 3.0, 5.0, 10.0, 100.0, 1000.0, 1e6, 1e7 };
        for (double x : values) {
            assertThat("acosh(" + x + ")", FastMath.acosh(x), closeTo(canonicalAcosh(x), ACOSH_RELATIVE_ERROR));
        }
    }

    public void testAcoshLargeValues() {
        // Values >= 2^28 use simplified formula: log(x) + ln(2)
        double threshold = (double) (1L << 28);
        double[] values = { threshold, threshold * 2, 1e10, 1e15, 1e100, Double.MAX_VALUE };
        for (double x : values) {
            double actual = FastMath.acosh(x);
            // For very large values, the canonical formula may lose precision due to x^2 overflow
            if (x > 1e150) {
                assertTrue("acosh(" + x + ") should be positive", actual > 0);
                assertTrue("acosh(" + x + ") should be finite", Double.isFinite(actual));
            } else {
                assertThat("acosh(" + x + ")", actual, closeTo(canonicalAcosh(x), ACOSH_RELATIVE_ERROR));
            }
        }
    }

    public void testAcoshBoundaryConsistency() {
        // Boundary between log1p formula and standard formula is at x = 2
        double justBelow2 = Math.nextDown(2.0);
        double justAbove2 = Math.nextUp(2.0);
        assertThat("acosh just below 2", FastMath.acosh(justBelow2), closeTo(canonicalAcosh(justBelow2), ACOSH_RELATIVE_ERROR));
        assertThat("acosh just above 2", FastMath.acosh(justAbove2), closeTo(canonicalAcosh(justAbove2), ACOSH_RELATIVE_ERROR));

        // Boundary at 2^28 between standard formula and large value formula
        double threshold = (double) (1L << 28);
        double justBelowThreshold = Math.nextDown(threshold);
        assertThat(
            "acosh just below 2^28",
            FastMath.acosh(justBelowThreshold),
            closeTo(canonicalAcosh(justBelowThreshold), ACOSH_RELATIVE_ERROR)
        );
        assertThat("acosh at 2^28", FastMath.acosh(threshold), closeTo(canonicalAcosh(threshold), ACOSH_RELATIVE_ERROR));
    }
}
