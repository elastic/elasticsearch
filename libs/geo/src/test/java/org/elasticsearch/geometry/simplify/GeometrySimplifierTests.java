/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.PriorityQueue;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public abstract class GeometrySimplifierTests extends ESTestCase {
    protected abstract SimplificationErrorCalculator calculator();

    public void testPoint() {
        GeometrySimplifier<Point> simplifier = new GeometrySimplifier.Points();
        Point point = new Point(0, 0);
        Point simplified = simplifier.simplify(point);
        assertThat("Same point", simplified, equalTo(point));
    }

    public void testShortLine() {
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(10, calculator());
        Line line = new Line(new double[] { -1, 0, 1 }, new double[] { -1, 0, 1 });

        // Test full geometry simplification
        Line simplified = simplifier.simplify(line);
        assertThat("Same line", simplified, equalTo(line));

        // Test streaming simplification
        simplifier.reset();
        for (int i = 0; i < line.length(); i++) {
            simplifier.consume(line.getX(i), line.getY(i));
        }
        Line streamSimplified = simplifier.produce();
        assertThat("Same line", streamSimplified, equalTo(simplified));
    }

    public void testStraightLine() {
        int maxPoints = 10;
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator());
        double[] x = new double[100];
        double[] y = new double[100];
        for (int i = 0; i < 100; i++) {
            x[i] = -1.0 + 2.0 * i / 100.0;
            y[i] = -1.0 + 2.0 * i / 100.0;
        }
        Line line = new Line(x, y);

        // Test full geometry simplification
        Line simplified = simplifier.simplify(line);
        assertLineEnds(maxPoints, simplified, line);

        // Test streaming simplification
        simplifier.reset();
        for (int i = 0; i < line.length(); i++) {
            simplifier.consume(line.getX(i), line.getY(i));
        }
        Line streamSimplified = simplifier.produce();
        assertLineEnds(maxPoints, streamSimplified, line);
        for (int i = 0; i < line.length(); i++) {
            double px = line.getX(i);
            double py = line.getY(i);
            String onLine = "Expect point (" + px + "," + py + ") to lie on line";
            assertTrue(onLine, pointExistsOnLine(px, py, simplified));
            assertTrue(onLine, pointExistsOnLine(px, py, streamSimplified));
        }
    }

    public void testZigZagLine() {
        int maxPoints = 10;
        int zigSize = 2;
        int lineLength = (maxPoints - 1) * zigSize + 1; // chosen to get rid of all y-crossings during simplification
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator());
        double[] x = new double[lineLength];
        double[] y = new double[lineLength];
        for (int i = 0; i < lineLength; i++) {
            x[i] = -1.0 + 2.0 * i / (maxPoints * zigSize);
            int offset = i % (2 * zigSize);
            if (offset > zigSize) {
                offset = 2 * zigSize - offset;
            }
            y[i] = -1.0 + offset;
        }
        Line line = new Line(x, y);
        // Test full geometry simplification
        Line simplified = simplifier.simplify(line);
        assertLineEnds(maxPoints, simplified, line);
        assertLinePointNeverHave(simplified, 0.0);

        // Test streaming simplification
        simplifier.reset();
        for (int i = 0; i < line.length(); i++) {
            simplifier.consume(line.getX(i), line.getY(i));
        }
        Line streamSimplified = simplifier.produce();
        assertLineEnds(maxPoints, streamSimplified, line);
        assertLinePointNeverHave(simplified, 0.0);
        for (int i = 0; i < line.length(); i++) {
            double px = line.getX(i);
            double py = line.getY(i);
            String onLine = "Expect point (" + px + "," + py + ") to lie on line";
            assertTrue(onLine, pointExistsOnLine(px, py, simplified));
            assertTrue(onLine, pointExistsOnLine(px, py, streamSimplified));
        }
        assertThat("Same line", streamSimplified, equalTo(simplified));
    }

    private boolean pointExistsOnLine(double x, double y, Line line) {
        for (int i = 0; i < line.length() - 1; i++) {
            if (pointInSegment(x, y, line.getX(i), line.getY(i), line.getX(i + 1), line.getY(i + 1))) {
                return true;
            }
        }
        return false;
    }

    private boolean pointInSegment(double x, double y, double x1, double y1, double x2, double y2) {
        boolean betweenX = (x1 <= x && x <= x2) || (x2 <= x && x <= x1);
        boolean betweenY = (y1 <= y && y <= y2) || (y2 <= y && y <= y1);
        if (betweenX && betweenY) {
            double deltaX = x2 - x1;
            double deltaY = y2 - y1;
            if (deltaX > deltaY) {
                return interpolationMatch(x, y, x1, y1, deltaX, deltaY);
            } else {
                return interpolationMatch(y, x, y1, x1, deltaY, deltaX);
            }
        }
        return false;
    }

    private boolean interpolationMatch(double a, double b, double a1, double b1, double deltaA, double deltaB) {
        double epsilon = Math.max(1e-10, deltaB / 1e5);
        double fraction = (a - a1) / deltaA;
        double expectedB = b1 + fraction * deltaB;
        return Math.abs(b - expectedB) < epsilon;
    }

    public void testPriorityQueue() {
        PriorityQueue<Integer> queue = new PriorityQueue<>();
        for (int i = 0; i < 10; i++) {
            queue.add(randomInt(100));
        }
        int previous = 0;
        while (queue.isEmpty() == false) {
            Integer number = queue.poll();
            // System.out.println(number);
            assertThat("Should be sorted", number, greaterThanOrEqualTo(previous));
        }
    }

    public void testPriorityQueuePointError() {
        PriorityQueue<GeometrySimplifier.PointError> queue = new PriorityQueue<>();
        GeometrySimplifier.PointError[] errors = new GeometrySimplifier.PointError[10];
        for (int i = 0; i < 10; i++) {
            GeometrySimplifier.PointError error = new GeometrySimplifier.PointError(i, 0, 0);
            error.error = randomDouble();
            queue.add(error);
            errors[i] = error;
        }
        GeometrySimplifier.PointError previous = null;
        while (queue.isEmpty() == false) {
            GeometrySimplifier.PointError error = queue.poll();
            System.out.println(error);
            if (previous != null) {
                assertThat("Should be sorted", error.error, greaterThanOrEqualTo(previous.error));
            }
            previous = error;
        }
        // Add back with original error values
        queue.addAll(Arrays.asList(errors).subList(0, 10));
        // Shuffle in place, testing that add/remove from the queue is sufficient to re-sort the queue
        for (int i = 0; i < 10; i++) {
            errors[i].error = randomDouble();
            if (queue.remove(errors[i])) {
                queue.add(errors[i]);
            }
        }
        previous = null;
        while (queue.isEmpty() == false) {
            GeometrySimplifier.PointError error = queue.poll();
            System.out.println(error);
            if (previous != null) {
                assertThat("Should be sorted", error.error, greaterThanOrEqualTo(previous.error));
            }
            previous = error;
        }
    }

    private void assertLineEnds(int maxPoints, Line simplified, Line line) {
        assertThat("Line shortened", simplified.length(), equalTo(maxPoints));
        assertThat("First point X", simplified.getX(0), equalTo(line.getX(0)));
        assertThat("First point Y", simplified.getY(0), equalTo(line.getY(0)));
        assertThat("Last point X", simplified.getX(simplified.length() - 1), equalTo(line.getX(line.length() - 1)));
        assertThat("Last point Y", simplified.getY(simplified.length() - 1), equalTo(line.getY(line.length() - 1)));
    }

    private void assertLinePointNeverHave(Line line, double y) {
        for (int i = 0; i < line.length(); i++) {
            assertThat("Expect points[" + i + "] with y=" + y + " to be removed", line.getY(i), not(closeTo(y, 1e-10)));
        }
    }
}
