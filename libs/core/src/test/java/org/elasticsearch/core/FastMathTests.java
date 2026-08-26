/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import ch.obermuhlner.math.big.BigDecimalMath;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.math.BigDecimal;

import static java.math.MathContext.DECIMAL128;
import static org.hamcrest.Matchers.equalTo;

public class FastMathTests extends ESTestCase {

    // Canonical formula:
    // https://en.wikipedia.org/wiki/Inverse_hyperbolic_functions#Definitions_in_terms_of_logarithms
    // acosh(x) = ln(x + sqrt(x^2 - 1))
    private static double canonicalAcosh(double x) {
        BigDecimal bx = new BigDecimal(x);
        BigDecimal arg = bx.add(bx.multiply(bx, DECIMAL128).subtract(BigDecimal.ONE).sqrt(DECIMAL128));
        return BigDecimalMath.log(arg, DECIMAL128).doubleValue();
    }

    // Canonical formula:
    // https://en.wikipedia.org/wiki/Inverse_hyperbolic_functions#Definitions_in_terms_of_logarithms
    // asinh(x) = ln(x + sqrt(x^2 + 1))
    private static double canonicalAsinh(double x) {
        BigDecimal abs = new BigDecimal(Math.abs(x));
        BigDecimal sqrt = abs.multiply(abs, DECIMAL128).add(BigDecimal.ONE).sqrt(DECIMAL128);
        double result = BigDecimalMath.log(abs.add(sqrt), DECIMAL128).doubleValue();
        return Math.copySign(result, x);
    }

    private static Matcher<Double> withinOneUlpOf(double expected) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Double actual) {
                return Math.abs(expected - actual) <= Math.ulp(expected);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a numeric value within 1 ULP of ").appendValue(expected);
            }

            @Override
            protected void describeMismatchSafely(Double actual, Description mismatchDescription) {
                double absDiff = Math.abs(expected - actual);
                double ulps = absDiff / Math.ulp(expected);
                mismatchDescription.appendValue(actual).appendText(" was ").appendValue(ulps).appendText(" ULPs away");
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
            assertThat("acosh(" + x + ")", FastMath.acosh(x), withinOneUlpOf(canonicalAcosh(x)));
        }
    }

    public void testAcoshModerateValues() {
        // Values in range (2, 2^28) use the standard formula
        double[] values = { 2.5, 3.0, 5.0, 10.0, 100.0, 1000.0, 1e6, 1e7 };
        for (double x : values) {
            assertThat("acosh(" + x + ")", FastMath.acosh(x), withinOneUlpOf(canonicalAcosh(x)));
        }
    }

    public void testAcoshLargeValues() {
        // Values >= 2^28 use simplified formula: log(x) + ln(2)
        double threshold = (double) (1L << 28);
        double[] values = { threshold, threshold * 2, 1e10, 1e15, 1e100, Double.MAX_VALUE };
        for (double x : values) {
            assertThat("acosh(" + x + ")", FastMath.acosh(x), withinOneUlpOf(canonicalAcosh(x)));
        }
    }

    public void testAcoshBoundaryConsistency() {
        // Boundary between log1p formula and standard formula is at x = 2
        double justBelow2 = Math.nextDown(2.0);
        double justAbove2 = Math.nextUp(2.0);
        assertThat("acosh just below 2", FastMath.acosh(justBelow2), withinOneUlpOf(canonicalAcosh(justBelow2)));
        assertThat("acosh just above 2", FastMath.acosh(justAbove2), withinOneUlpOf(canonicalAcosh(justAbove2)));

        // Boundary at 2^28 between standard formula and large value formula
        double threshold = (double) (1L << 28);
        double justBelowThreshold = Math.nextDown(threshold);
        assertThat("acosh just below 2^28", FastMath.acosh(justBelowThreshold), withinOneUlpOf(canonicalAcosh(justBelowThreshold)));
        assertThat("acosh at 2^28", FastMath.acosh(threshold), withinOneUlpOf(canonicalAcosh(threshold)));
    }

    // -----------------------------------------------------------------------
    // ASINH tests
    // -----------------------------------------------------------------------

    public void testAsinhNaN() {
        assertTrue(Double.isNaN(FastMath.asinh(Double.NaN)));
    }

    public void testAsinhZero() {
        assertThat(FastMath.asinh(0.0), equalTo(0.0));
        assertThat(FastMath.asinh(-0.0), equalTo(-0.0));
    }

    public void testAsinhInfinity() {
        assertThat(FastMath.asinh(Double.POSITIVE_INFINITY), equalTo(Double.POSITIVE_INFINITY));
        assertThat(FastMath.asinh(Double.NEGATIVE_INFINITY), equalTo(Double.NEGATIVE_INFINITY));
    }

    public void testAsinhNearZero() {
        // Values with x < 2^-28 use the identity asinh(x) ≈ x
        double[] values = { 1e-10, 1e-15, 1e-20, -1e-10, -1e-15 };
        for (double x : values) {
            assertThat("asinh(" + x + ")", FastMath.asinh(x), equalTo(x));
        }
    }

    public void testAsinhSmallValues() {
        // Values in range [2^-28, 2] use the log1p formula
        double[] values = { 0.001, 0.01, 0.1, 0.5, 1.0, 1.5, 2.0, -0.5, -1.0, -2.0 };
        for (double x : values) {
            assertThat("asinh(" + x + ")", FastMath.asinh(x), withinOneUlpOf(canonicalAsinh(x)));
        }
    }

    public void testAsinhModerateValues() {
        // Values in range (2, 2^28) use the standard formula
        double[] values = { 3.0, 5.0, 10.0, 100.0, 1000.0, 1e6, -3.0, -10.0, -1000.0 };
        for (double x : values) {
            assertThat("asinh(" + x + ")", FastMath.asinh(x), withinOneUlpOf(canonicalAsinh(x)));
        }
    }

    public void testAsinhLargeValues() {
        // Values with x >= 2^28 use simplified formula: sign(x) * (log(x) + ln(2))
        double threshold = (double) (1L << 28);
        double[] values = { threshold, 1e10, 1e15, 1e100, Double.MAX_VALUE, -threshold, -1e10, -Double.MAX_VALUE };
        for (double x : values) {
            assertThat("asinh(" + x + ")", FastMath.asinh(x), withinOneUlpOf(canonicalAsinh(x)));
        }
    }

    public void testAsinhOddFunction() {
        // asinh is an odd function: asinh(-x) == -asinh(x)
        double[] values = { 0.5, 1.0, 2.0, 10.0, 1000.0, 1e10 };
        for (double x : values) {
            assertThat("asinh(-" + x + ") == -asinh(" + x + ")", FastMath.asinh(-x), equalTo(-FastMath.asinh(x)));
        }
    }

    public void testAsinhBoundaryConsistency() {
        // Boundary between log1p formula and standard formula at x = 2
        double justBelow2 = Math.nextDown(2.0);
        double justAbove2 = Math.nextUp(2.0);
        assertThat("asinh just below 2", FastMath.asinh(justBelow2), withinOneUlpOf(canonicalAsinh(justBelow2)));
        assertThat("asinh just above 2", FastMath.asinh(justAbove2), withinOneUlpOf(canonicalAsinh(justAbove2)));

        // Boundary at 2^28 between standard formula and large value formula
        double threshold = (double) (1L << 28);
        double justBelowThreshold = Math.nextDown(threshold);
        assertThat("asinh just below 2^28", FastMath.asinh(justBelowThreshold), withinOneUlpOf(canonicalAsinh(justBelowThreshold)));
        assertThat("asinh at 2^28", FastMath.asinh(threshold), withinOneUlpOf(canonicalAsinh(threshold)));
    }

}
