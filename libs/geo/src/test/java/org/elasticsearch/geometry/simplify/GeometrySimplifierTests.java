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

public class GeometrySimplifierTests extends ESTestCase {

    public void testPoint() {
        GeometrySimplifier<Point> simplifier = new GeometrySimplifier.Points();
        Point point = new Point(0, 0);
        Point simplified = simplifier.simplify(point);
        assertThat("Same point", simplified, equalTo(point));
    }

    public void testShortLine() {
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(10);
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
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints);
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
        // TODO: add assertion that lines lie on top of one another, even if with different points
        // Alternatively add increased simplification where co-linear points are removed
        // assertThat("Same line", streamSimplified, equalTo(simplified));
    }

    public void testZigZagLine() {
        int maxPoints = 10;
        int zigSize = 2;
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints);
        double[] x = new double[maxPoints * zigSize + 1];
        double[] y = new double[maxPoints * zigSize + 1];
        for (int i = 0; i < maxPoints * zigSize + 1; i++) {
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
        // TODO: add assertion that lines lie on top of one another, even if with different points
        // Alternatively add increased simplification where co-linear points are removed
        // assertThat("Same line", streamSimplified, equalTo(simplified));
    }

    public void testPriorityQueue() {
        PriorityQueue<Integer> queue = new PriorityQueue<>();
        for (int i = 0; i < 10; i++) {
            queue.add(randomInt(100));
        }
        int previous = 0;
        while (!queue.isEmpty()) {
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
        while (!queue.isEmpty()) {
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
        while (!queue.isEmpty()) {
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
