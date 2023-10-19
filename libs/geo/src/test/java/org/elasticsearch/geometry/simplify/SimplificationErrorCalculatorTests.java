/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.test.ESTestCase;

import static java.lang.Math.toRadians;
import static org.hamcrest.Matchers.closeTo;

public class SimplificationErrorCalculatorTests extends ESTestCase {

    public void testAreaCalculation() {
        assertTriangleAreas(new SimplificationErrorCalculator.TriangleAreaCalculator(), 6.182E9);
    }

    public void testCartesianAreaCalculation() {
        assertTriangleAreas(new SimplificationErrorCalculator.CartesianTriangleAreaCalculator(), 0.5);
    }

    public void testFrechetCalculation() {
        var calculator = new SimplificationErrorCalculator.CartesianHeightAndBackpathDistanceCalculator();
        var ao = new TestPoint(0, 0);
        var co = new TestPoint(1, 0);
        for (double degrees = 0; degrees < 360; degrees += 45) {
            TestPoint c = co.rotated(degrees, ao);
            for (double x = -2; x <= 3; x += 0.5) {
                var b = new TestPoint(x, 1).rotated(degrees, ao);
                double error = calculator.calculateError(ao, b, c);
                double expected = 1.0;  // triangle height is 1.0
                if (x < -1 || x > 2) {
                    // Back-paths dominate, so assert on that, otherwise assert on triangle height
                    expected = x < 0 ? -x : x - 1;
                }
                assertThat("Expect a unit offset when bx=" + x + " rotated " + degrees, error, closeTo(expected, 1e-10));
            }
        }
    }

    /**
     * The area calculation has a fix for flat triangles that previously resulted in NaN. This test asserts better behaviour.
     */
    public void testFlatTriangleArea() {
        var calculator = new SimplificationErrorCalculator.TriangleAreaCalculator();
        var a = new TestPoint(0, 0);
        var c = new TestPoint(2, 0);
        double previous = 1.23637e10 * 2;
        for (double y = 1; y >= 0.000000001; y /= 2) {
            var b = new TestPoint(1, y);
            double area = calculator.calculateError(a, b, c);
            double expected = previous / 2;
            double error = Math.max(1e4, expected / 1e5);
            assertThat("Triangle area should be approach zero", area, closeTo(expected, error));
            previous = area;
        }
    }

    private void assertTriangleAreas(SimplificationErrorCalculator calculator, double expected) {
        var ao = new TestPoint(0, 0);
        var co = new TestPoint(1, 0);
        for (double degrees = 0; degrees < 360; degrees += 45) {
            TestPoint c = co.rotated(degrees, ao);
            for (double x = -1; x <= 2; x += 0.5) {
                var b = new TestPoint(x, 1).rotated(degrees, ao);
                assertAreaCalculationResult("Triangle area with b=" + x + " and rotated " + degrees, calculator, expected, ao, b, c);
            }
        }
    }

    record TestPoint(double x, double y) implements SimplificationErrorCalculator.PointLike {

        private TestPoint rotated(double degrees, TestPoint origin) {
            TestPoint point = new TestPoint(x, y);
            return rotateCCW(degrees, origin, point);
        }
    }

    /** Note that this rotation is only accurate in cartesian coordinates, take that into account when testing */
    private static TestPoint rotateCCW(double degrees, TestPoint origin, TestPoint point) {
        double radians = toRadians(degrees);
        double cos = Math.cos(radians);
        double sin = Math.sin(radians);
        double x = (point.x - origin.x);
        double y = (point.y - origin.y);
        return new TestPoint(origin.x + x * cos - y * sin, origin.y + x * sin + y * cos);
    }

    @SuppressWarnings("SameParameterValue")
    private void assertAreaCalculationResult(
        String message,
        SimplificationErrorCalculator calculator,
        double expected,
        TestPoint a,
        TestPoint b,
        TestPoint c
    ) {
        double error = Math.max(1e-10, expected / 1e3);
        assertThat(message, calculator.calculateError(a, b, c), closeTo(expected, error));
        assertThat(message, calculator.calculateError(b, c, a), closeTo(expected, error));
        assertThat(message, calculator.calculateError(c, a, b), closeTo(expected, error));
    }
}
