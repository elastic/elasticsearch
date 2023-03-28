/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class SimplificationErrorCalculatorTests extends ESTestCase {

    public void testAreaCalculation() {
        var calculator = new SimplificationErrorCalculator.TriangleAreaCalculator();
        var ao = new TestPoint(0, 0);
        var co = new TestPoint(1, 0);
        for (double degrees = 0; degrees < 360; degrees += 45) {
            TestPoint c = co.rotated(degrees, ao);
            for (double x = -1; x <= 2; x += 0.5) {
                var b = new TestPoint(x, 1).rotated(degrees, ao);
                assertCalculationResult("Expect half a unit triangle when rotated " + degrees, calculator, 0.5, ao, b, c);
            }
        }
    }

    public void testFrechetCalculation() {
        var calculator = new SimplificationErrorCalculator.FrechetErrorCalculator();
        var ao = new TestPoint(0, 0);
        var co = new TestPoint(1, 0);
        for (double degrees = 0; degrees < 360; degrees += 45) {
            TestPoint c = co.rotated(degrees, ao);
            for (double x = -1; x <= 2; x += 0.5) {
                var b = new TestPoint(x, 1).rotated(degrees, ao);
                double error = calculator.calculateError(ao, b, c);
                // TODO: change test once Frechet calculation includes back-paths
                assertThat("Expect a unit offset when bx=" + x + " rotated " + degrees, error, closeTo(1.0, 1e-10));
            }
        }
    }

    static class TestPoint implements SimplificationErrorCalculator.PointLike {
        double x;
        double y;

        TestPoint(double x, double y) {
            this.x = x;
            this.y = y;
        }

        private TestPoint rotated(double degrees, TestPoint origin) {
            TestPoint point = new TestPoint(x, y);
            rotateCCW(degrees, origin, point);
            return point;
        }

        @Override
        public double getX() {
            return x;
        }

        @Override
        public double getY() {
            return y;
        }

        @Override
        public String toString() {
            return "POINT( " + x + " " + y + " )";
        }
    }

    private static void rotateCCW(double degrees, TestPoint origin, TestPoint point) {
        double radians = Math.toRadians(degrees);
        double cos = Math.cos(radians);
        double sin = Math.sin(radians);
        double x = (point.x - origin.x);
        double y = (point.y - origin.y);
        point.x = origin.x + x * cos - y * sin;
        point.y = origin.y + x * sin + y * cos;
    }

    @SuppressWarnings("SameParameterValue")
    private void assertCalculationResult(
        String message,
        SimplificationErrorCalculator calculator,
        double expected,
        TestPoint a,
        TestPoint b,
        TestPoint c
    ) {
        assertThat(message, calculator.calculateError(a, b, c), closeTo(expected, 1e-10));
        assertThat(message, calculator.calculateError(b, c, a), closeTo(expected, 1e-10));
        assertThat(message, calculator.calculateError(c, a, b), closeTo(expected, 1e-10));
    }

}
