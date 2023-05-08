/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.CircleUtils;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public abstract class GeometrySimplifierTests extends ESTestCase {
    protected abstract SimplificationErrorCalculator calculator();

    /**
     * The GeometrySimplifier.Monitor allows external code to be notified of simplification events.
     * This can be used, for example, to generate images and animations of the simplification process,
     * as done in the geometry-simplifier-debug project. Or, as done here, it can allow for collecting
     * information for later test assertions.
     */
    private static class TestMonitor implements GeometrySimplifier.Monitor {
        private final String name;
        private final int maxPoints;
        private int added = 0;
        private int removed = 0;
        private final HashMap<String, TestSimplificationState> simplifications = new HashMap<>();

        private record TestSimplificationState(String description, int maxPoints, int count, boolean completed) {
            private TestSimplificationState(String description, int maxPoints) {
                this(description, maxPoints, 1, false);
            }

            private TestSimplificationState asCompleted() {
                return new TestSimplificationState(this.description, this.maxPoints, this.count, true);
            }

            private TestSimplificationState runAgain() {
                return new TestSimplificationState(this.description, this.maxPoints, this.count + 1, false);
            }
        }

        private TestMonitor(String name, int maxPoints) {
            this.name = name;
            this.maxPoints = maxPoints;
        }

        @Override
        public void pointAdded(String status, List<SimplificationErrorCalculator.PointLike> points) {
            this.added++;
        }

        @Override
        public void pointRemoved(
            String status,
            List<SimplificationErrorCalculator.PointLike> points,
            SimplificationErrorCalculator.PointLike removed,
            double error,
            SimplificationErrorCalculator.PointLike previous,
            SimplificationErrorCalculator.PointLike next
        ) {
            this.added++;
            this.removed++;
        }

        @Override
        public void startSimplification(String description, int maxPoints) {
            assertThat("Simplification " + name + " cannot have higher maxPoints", maxPoints, lessThanOrEqualTo(this.maxPoints));
            simplifications.compute(description, (d, previous) -> {
                if (previous == null) {
                    return new TestSimplificationState(description, maxPoints);
                } else {
                    assertTrue("Simplification " + name + " already started", previous.completed);
                    return previous.runAgain();
                }
            });
        }

        @Override
        public void endSimplification(String description, List<SimplificationErrorCalculator.PointLike> points) {
            assertTrue("Simplification " + name + " not started", simplifications.containsKey(description));
            assertFalse("Simplification " + name + " already completed", simplifications.get(description).completed);
            simplifications.computeIfPresent(description, (k, v) -> v.asCompleted());
        }

        public void assertCompleted(int simplificationCount, int shouldHaveAdded, int shouldHaveRemoved) {
            assertCompleted(simplificationCount, shouldHaveAdded, shouldHaveRemoved, Matchers::equalTo);
        }

        public void assertCompleted(
            int simplificationCount,
            int shouldHaveAdded,
            int shouldHaveRemoved,
            Function<Integer, Matcher<Integer>> matcher
        ) {
            int totalCount = simplifications.values().stream().mapToInt(v -> v.count).sum();
            assertThat("Expected " + simplificationCount + " simplifications", totalCount, matcher.apply(simplificationCount));
            for (TestSimplificationState state : simplifications.values()) {
                assertTrue("Simplification should be completed: " + state.description, state.completed);
            }
            assertThat(
                "Expected " + shouldHaveAdded + " points added when maxPoints was " + maxPoints,
                added,
                matcher.apply(shouldHaveAdded)
            );
            assertThat(
                "Expected " + shouldHaveRemoved + " points removed when maxPoints was " + maxPoints,
                removed,
                matcher.apply(shouldHaveRemoved)
            );
        }
    }

    public void testShortLine() {
        int maxPoints = 10;
        var monitor = new TestMonitor("ShortLine", maxPoints);
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator(), monitor);
        Line line = new Line(new double[] { -1, 0, 1 }, new double[] { -1, 0, 1 });

        // Test full geometry simplification
        Line simplified = simplifier.simplify(line);
        assertThat("Same line", simplified, equalTo(line));
        monitor.assertCompleted(0, 0, 0); // simplification never actually used

        // Test streaming simplification
        simplifier.reset();
        for (int i = 0; i < line.length(); i++) {
            simplifier.consume(line.getX(i), line.getY(i));
        }
        Line streamSimplified = simplifier.produce();
        assertThat("Same line", streamSimplified, equalTo(simplified));
        assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(line.length()));
        monitor.assertCompleted(0, 3, 0);  // stream used for less than maxPoints
    }

    public void testStraightLine() {
        int maxPoints = 10;
        var monitor = new TestMonitor("StraightLine", maxPoints);
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator(), monitor);
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
        monitor.assertCompleted(1, line.length(), line.length() - maxPoints);

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
        assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
        monitor.assertCompleted(1, 2 * line.length(), 2 * (line.length() - maxPoints));
    }

    public void testZigZagLine() {
        int maxPoints = 10;
        int zigSize = 2;
        int lineLength = (maxPoints - 1) * zigSize + 1; // chosen to get rid of all y-crossings during simplification
        var monitor = new TestMonitor("ZigZagLine", maxPoints);
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator(), monitor);
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
        monitor.assertCompleted(1, line.length(), line.length() - maxPoints);

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
        assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
        monitor.assertCompleted(1, 2 * line.length(), 2 * (line.length() - maxPoints));
    }

    public void testCircularLine() {
        int maxPoints = 10;
        int countFactor = 10;
        double centerX = 6;
        double centerY = 48;
        double radius = 10;
        Line line = makeCircularLine(maxPoints, countFactor, centerX, centerY, radius);
        assertPointsOnCircle(centerX, centerY, radius, line);
        var monitor = new TestMonitor("CircularLine", maxPoints);
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator(), monitor);
        // Test full geometry simplification
        Line simplified = simplifier.simplify(line);
        assertLineEnds(maxPoints, simplified, line);
        assertPointsOnCircle(centerX, centerY, radius, simplified);

        // Test streaming simplification
        simplifier.reset();
        simplifier.notifyMonitorSimplificationStart();
        for (int i = 0; i < line.length(); i++) {
            simplifier.consume(line.getX(i), line.getY(i));
        }
        simplifier.notifyMonitorSimplificationEnd();
        Line streamSimplified = simplifier.produce();
        assertLineEnds(maxPoints, streamSimplified, line);
        assertPointsOnCircle(centerX, centerY, radius, streamSimplified);
        assertThat("Same line", streamSimplified, equalTo(simplified));
        int shouldHaveAdded = 2 * maxPoints * countFactor;
        int shouldHaveRemoved = shouldHaveAdded - maxPoints * 2;
        monitor.assertCompleted(2, shouldHaveAdded, shouldHaveRemoved);
        assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
    }

    public void testCircularPolygon() {
        int maxPoints = 10;
        int countFactor = 10;
        double centerX = 6;
        double centerY = 48;
        double radius = 10;
        Polygon polygon = makeCircularPolygon(maxPoints, countFactor, centerX, centerY, radius);
        assertPointsOnCircle(centerX, centerY, radius, polygon);
        var monitor = new TestMonitor("CircularPolygon", maxPoints);
        GeometrySimplifier<Polygon> simplifier = new GeometrySimplifier.Polygons(maxPoints, calculator(), monitor);
        // Test full geometry simplification
        Polygon simplified = simplifier.simplify(polygon);
        assertPolygonEnds("Polygon", maxPoints, simplified, polygon);
        assertPointsOnCircle(centerX, centerY, radius, simplified);

        // Test streaming simplification
        simplifier.reset();
        simplifier.notifyMonitorSimplificationStart();
        LinearRing ring = polygon.getPolygon();
        for (int i = 0; i < ring.length(); i++) {
            simplifier.consume(ring.getX(i), ring.getY(i));
        }
        simplifier.notifyMonitorSimplificationEnd();
        Polygon streamSimplified = simplifier.produce();
        assertPolygonEnds("Polygon", maxPoints, simplified, polygon);
        assertPointsOnCircle(centerX, centerY, radius, streamSimplified);
        assertThat("Same line", streamSimplified, equalTo(simplified));
        int shouldHaveAdded = 2 * maxPoints * countFactor + 2;
        int shouldHaveRemoved = shouldHaveAdded - maxPoints * 2;
        monitor.assertCompleted(2, shouldHaveAdded, shouldHaveRemoved);
        assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
    }

    public void testLineWithBackPaths() {
        double[] x = new double[] { -1.0, -0.5, -0.4, -0.2, -0.4, -0.3, 0.0, -0.1, -0.2, -0.1, 0.1, 0.3, 0.5, 0.8, 0.6, 0.85, 1.0 };
        double[] y = new double[] { 0.0, -0.15, 0.2, 0.1, 0.0, -0.05, 0.05, -0.1, -0.12, -0.17, -0.15, 0.05, 0.15, 0.0, -0.12, -0.17, 0.0 };
        Line line = new Line(x, y);
        int maxPoints = 7;
        var monitor = new TestMonitor("LineWithBackPaths", maxPoints);
        GeometrySimplifier.LineStrings simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator(), monitor);
        Line simplified = simplifier.simplify(line);
        assertLineEnds(maxPoints, simplified, line);
        monitor.assertCompleted(1, x.length, x.length - maxPoints);
        assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
    }

    public void testLineWithBackPaths2() throws IOException, ParseException {
        String lineString = """
            LINESTRING(-1.0 0.0, -0.9 -0.07, -0.8 -0.1, -0.7 -0.08, -0.6 -0.05, -0.55 -0.08, -0.5 -0.15, -0.45 -0.045, -0.4 0.2, -0.3 0.17,
            -0.2 0.1, -0.32 0.06, -0.4 0.0, -0.36 -0.03, -0.3 -0.045, -0.16 -0.005, -0.04 0.05, 0.0 0.03, -0.04 -0.038, -0.1 -0.1,
            -0.16 -0.1, -0.17 -0.13, -0.16 -0.16, -0.1 -0.175, 0.008 -0.175, 0.1 -0.15, 0.2 -0.05, 0.3 0.05, 0.4 0.12, 0.5 0.15,
            0.67 0.1, 0.8 0.0, 0.7 -0.07, 0.6 -0.12, 0.7 -0.185, 0.85 -0.18, 0.96 -0.09, 1.0 0.0))""";
        Line line = (Line) WellKnownText.fromWKT(GeometryValidator.NOOP, true, lineString);
        int[] maxPointsArray = new int[] { 15, 7 };
        var monitor = new TestMonitor("LineWithBackPaths2", maxPointsArray[0]);
        for (int maxPoints : maxPointsArray) {
            var simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator(), monitor);
            simplifier.simplify(line);
            assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
        }
        int shouldHaveAdded = maxPointsArray.length * line.length();
        int shouldHaveRemoved = shouldHaveAdded - Arrays.stream(maxPointsArray).sum();
        monitor.assertCompleted(2, shouldHaveAdded, shouldHaveRemoved);
    }

    public void testLineWithNarrowSpikes() {
        int length = 200;
        double spikeFactor = 10.0;
        int maxPoints = 20;
        double[] x = new double[length];
        double[] y = new double[length];
        int count = 0;
        for (int i = 0; i < length; i++) {
            int direction = (i % 2) * 2 - 1;
            x[i] = -1.0 + 2.0 * i / length;
            y[i] = 0.0 + direction / spikeFactor;
            if ((i + 1) % 29 == 0) {
                y[i] *= spikeFactor;
                count++;
            }
        }
        Line line = new Line(x, y);
        var monitor = new TestMonitor("LineWithNarrowSpikes", maxPoints);
        GeometrySimplifier.LineStrings simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator(), monitor);
        Line simplified = simplifier.simplify(line);
        assertLineWithNarrowSpikes(simplified, count);
        monitor.assertCompleted(1, x.length, x.length - maxPoints);
        assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
    }

    protected abstract void assertLineWithNarrowSpikes(Line simplified, int spikeCount);

    protected void assertLineSpikes(String description, Line simplified, int count) {
        double[] simplifiedY = simplified.getY();
        int found = 0;
        for (double v : simplifiedY) {
            if (Math.abs(v) > 0.9) {
                found++;
            }
        }
        assertThat(description + " expect all original spikes to remain", found, equalTo(count));
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public void testComplexGeoJsonPolygon() throws IOException, ParseException {
        int maxPoints1 = 2000;
        int maxPoints2 = 100;
        Polygon[] polygons = makePolygonsFromGeoJsonFile("us.json.gz");
        Polygon polygon = Arrays.stream(polygons).reduce((v, p) -> p.getPolygon().length() > v.getPolygon().length() ? p : v).get();
        for (int maxPoints : new int[] { maxPoints1, maxPoints2 }) {
            var monitor = new TestMonitor("us-polygon-" + maxPoints, maxPoints);
            var simplifier = new GeometrySimplifier.Polygons(maxPoints, calculator(), monitor);
            Polygon simplified = simplifier.simplify(polygon);
            assertPolygonEnds("Polygon", maxPoints, simplified, polygon);
            monitor.assertCompleted(1, polygon.getPolygon().length(), polygon.getPolygon().length() - maxPoints);
            assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
        }
    }

    public void testComplexGeoJsonPolygons() throws IOException, ParseException {
        int maxPoints = 100;
        Polygon[] polygons = makePolygonsFromGeoJsonFile("us.json.gz");
        for (int i = 0; i < polygons.length; i++) {
            Polygon polygon = polygons[i];
            int length = polygon.getPolygon().length();
            if (length > maxPoints * 2 && length < maxPoints * 10) {
                var monitor = new TestMonitor("us-polygon-" + i + "-" + maxPoints, maxPoints);
                var simplifier = new GeometrySimplifier.Polygons(maxPoints, calculator(), monitor);
                Polygon simplified = simplifier.simplify(polygon);
                assertPolygonEnds("Polygon", maxPoints, simplified, polygon);
                monitor.assertCompleted(1, length, length - maxPoints);
                assertThat("Should create exactly the needed number of PointError objects", simplifier.objCount, equalTo(maxPoints + 1));
            }
        }
    }

    public void testComplexGeoJsonMultiPolygon() throws IOException, ParseException {
        int maxPoints1 = 2000;
        int maxPoints2 = 100;
        Polygon[] polygons = makePolygonsFromGeoJsonFile("us.json.gz");
        int maxPolyLength = Arrays.stream(polygons).reduce(0, (v, p) -> Math.max(v, p.getPolygon().length()), Math::max);
        MultiPolygon multiPolygon = new MultiPolygon(Arrays.asList(polygons));
        for (int maxPoints : new int[] { maxPoints1, maxPoints2 }) {
            var monitor = new TestMonitor("us-polygon-mp-" + maxPoints, maxPoints);
            var simplifier = new GeometrySimplifier.MultiPolygons(maxPoints, calculator(), monitor);
            MultiPolygon simplifiedMultiPolygon = simplifier.simplify(multiPolygon);
            for (int i = 0; i < simplifiedMultiPolygon.size(); i++) {
                Polygon simplified = simplifiedMultiPolygon.get(i);
                Polygon polygon = multiPolygon.get(simplifier.indexOf(i));
                double simplificationFactor = (double) maxPoints / maxPolyLength;
                int maxPolyPoints = Math.max(4, (int) (simplificationFactor * polygon.getPolygon().length()));
                assertPolygonEnds("Polygon[" + i + "]", maxPolyPoints, simplified, polygon);
            }
            monitor.assertCompleted(
                simplifiedMultiPolygon.size(),
                maxPolyLength,
                maxPolyLength - maxPoints,
                Matchers::greaterThanOrEqualTo
            );
        }
    }

    public void testErrorOnEmptySimplifier() {
        for (GeometrySimplifier<?> simplifier : new GeometrySimplifier<?>[] {
            new GeometrySimplifier.LineStrings(10, calculator()),
            new GeometrySimplifier.LinearRings(10, calculator()),
            new GeometrySimplifier.Polygons(10, calculator()),
            new GeometrySimplifier.MultiPolygons(10, calculator()) }) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, simplifier::produce);
            String name = simplifier.getClass().getSimpleName();
            String expected = name.startsWith("MultiPolygon")
                ? "the list of shapes cannot be null or empty"
                : "No points have been consumed";
            assertThat(simplifier.getClass().getSimpleName(), ex.getMessage(), containsString(expected));
        }
    }

    public void testShortValidLinearRing() {
        double[] x = new double[] { 0, 1, 1, 0, 0 };
        double[] y = new double[] { 0, 0, 1, 1, 0 };
        var simplifier = new GeometrySimplifier.LinearRings(10, calculator());
        for (int i = 0; i < x.length; i++) {
            simplifier.consume(x[i], y[i]);
        }
        var result = simplifier.produce();
        assertThat(result.length(), equalTo(x.length));
    }

    public void testShortInvalidLinearRing() {
        double[] x = new double[] { 0, 1, 1 };
        double[] y = new double[] { 0, 0, 1 };
        var simplifier = new GeometrySimplifier.LinearRings(10, calculator());
        for (int i = 0; i < x.length; i++) {
            simplifier.consume(x[i], y[i]);
        }
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, simplifier::produce);
        assertThat(ex.getMessage(), containsString("cannot have less than 4 points"));
    }

    public void testInvalidLinearRing() {
        double[] x = new double[] { 0, 1, 1, 0 };
        double[] y = new double[] { 0, 0, 1, 1 };
        var simplifier = new GeometrySimplifier.LinearRings(10, calculator());
        for (int i = 0; i < x.length; i++) {
            simplifier.consume(x[i], y[i]);
        }
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, simplifier::produce);
        assertThat(ex.getMessage(), containsString("first and last points of the linear ring must be the same"));
    }

    public void testMultiPolygonDisallowsStreaming() {
        var simplifier = new GeometrySimplifier.MultiPolygons(10, calculator());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> simplifier.consume(0, 0));
        assertThat(ex.getMessage(), containsString("simplifier cannot work in streaming mode"));
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
            if (previous != null) {
                assertThat("Should be sorted", error.error, greaterThanOrEqualTo(previous.error));
            }
            previous = error;
        }
    }

    private Line makeCircularLine(int maxPoints, int countFactor, double centerX, double centerY, double radiusDegrees) {
        int length = maxPoints * countFactor;
        double radiusMeters = 40000000 * radiusDegrees / 360;
        var polygon = CircleUtils.createRegularGeoShapePolygon(new Circle(centerX, centerY, radiusMeters), length);
        var ring = polygon.getPolygon();
        double[] x = new double[length];
        double[] y = new double[length];
        for (int i = 0; i < length; i++) {
            x[i] = ring.getX(i);
            y[i] = ring.getY(i);
        }
        Line line = new Line(x, y);
        assertPointsOnCircle(centerX, centerY, radiusDegrees, line);
        return line;
    }

    private Polygon makeCircularPolygon(int maxPoints, int countFactor, double centerX, double centerY, double radiusDegrees) {
        int length = maxPoints * countFactor;
        double radiusMeters = 40000000 * radiusDegrees / 360;
        var polygon = CircleUtils.createRegularGeoShapePolygon(new Circle(centerX, centerY, radiusMeters), length);
        assertPointsOnCircle(centerX, centerY, radiusDegrees, polygon);
        return polygon;
    }

    @SuppressWarnings("SameParameterValue")
    private Polygon[] makePolygonsFromGeoJsonFile(String filename) throws IOException, ParseException {
        String json = loadJsonFile(filename);
        org.apache.lucene.geo.Polygon[] lucenePolygons = org.apache.lucene.geo.Polygon.fromGeoJSON(json);
        Polygon[] polygons = new Polygon[lucenePolygons.length];
        for (int i = 0; i < lucenePolygons.length; i++) {
            assert lucenePolygons[i].numHoles() == 0;
            double[] x = lucenePolygons[i].getPolyLons();
            double[] y = lucenePolygons[i].getPolyLats();
            assertThat("First and last points are the same", x[x.length - 1], equalTo(x[0]));
            assertThat("First and last points are the same", y[y.length - 1], equalTo(y[0]));
            polygons[i] = new Polygon(new LinearRing(x, y));
        }
        return polygons;
    }

    private String loadJsonFile(String name) throws IOException {
        InputStream is = getClass().getResourceAsStream(name);
        if (is == null) {
            throw new FileNotFoundException("classpath resource not found: " + name);
        }
        if (name.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder();
        reader.lines().forEach(builder::append);
        return builder.toString();
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

    private void assertLineEnds(int maxPoints, Line simplified, Line line) {
        assertThat("Line shortened", simplified.length(), equalTo(maxPoints));
        assertThat("First point X", simplified.getX(0), equalTo(line.getX(0)));
        assertThat("First point Y", simplified.getY(0), equalTo(line.getY(0)));
        assertThat("Last point X", simplified.getX(simplified.length() - 1), equalTo(line.getX(line.length() - 1)));
        assertThat("Last point Y", simplified.getY(simplified.length() - 1), equalTo(line.getY(line.length() - 1)));
    }

    private void assertPolygonEnds(String prefix, int maxPoints, Polygon simplified, Polygon original) {
        LinearRing ring = simplified.getPolygon();
        LinearRing originalRing = original.getPolygon();
        if (maxPoints > 0) {
            assertThat(prefix + " shortened", ring.length(), equalTo(Math.min(maxPoints, original.getPolygon().length())));
        }
        assertThat(prefix + " first point X", ring.getX(0), equalTo(originalRing.getX(0)));
        assertThat(prefix + " first point Y", ring.getY(0), equalTo(originalRing.getY(0)));
        assertThat(prefix + " last point X", ring.getX(ring.length() - 1), equalTo(originalRing.getX(originalRing.length() - 1)));
        assertThat(prefix + " last point Y", ring.getY(ring.length() - 1), equalTo(originalRing.getY(originalRing.length() - 1)));
    }

    @SuppressWarnings("SameParameterValue")
    private void assertLinePointNeverHave(Line line, double y) {
        for (int i = 0; i < line.length(); i++) {
            assertThat("Expect points[" + i + "] with y=" + y + " to be removed", line.getY(i), not(closeTo(y, 1e-10)));
        }
    }

    private void assertPointsOnCircle(double centerX, double centerY, double radius, Line line) {
        assertPointsOnCircle(centerX, centerY, radius, line.getX(), line.getY());
    }

    private void assertPointsOnCircle(double centerX, double centerY, double radius, Polygon polygon) {
        assertPointsOnCircle(centerX, centerY, radius, polygon.getPolygon().getX(), polygon.getPolygon().getY());
    }

    private void assertPointsOnCircle(double centerX, double centerY, double radius, double[] x, double[] y) {
        double radiusMeters = radius * 40000000 / 360;
        double delta = radiusMeters / 1e7;
        for (int i = 0; i < x.length; i++) {
            double pRadius = slowHaversin(centerY, centerX, y[i], x[i]);
            String onLine = "Expect point (" + x[i] + "," + y[i] + ") to lie on circle with radius " + radiusMeters;
            assertThat(onLine, pRadius, closeTo(radiusMeters, delta));
        }
    }

    // simple incorporation of the wikipedia formula
    // Improved method implementation for better performance can be found in `SloppyMath.haversinMeters` which
    // is included in `org.apache.lucene:lucene-core`.
    // Do not use this method for performance critical cases
    private static double slowHaversin(double lat1, double lon1, double lat2, double lon2) {
        double h1 = (1 - StrictMath.cos(StrictMath.toRadians(lat2) - StrictMath.toRadians(lat1))) / 2;
        double h2 = (1 - StrictMath.cos(StrictMath.toRadians(lon2) - StrictMath.toRadians(lon1))) / 2;
        double h = h1 + StrictMath.cos(StrictMath.toRadians(lat1)) * StrictMath.cos(StrictMath.toRadians(lat2)) * h2;
        return 2 * 6371008.7714 * StrictMath.asin(Math.min(1, Math.sqrt(h)));
    }
}
