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
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public abstract class GeometrySimplifierTests extends ESTestCase {
    protected abstract SimplificationErrorCalculator calculator();

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

    public void testCircularLine() {
        int maxPoints = 10;
        double centerX = 6;
        double centerY = 48;
        double radius = 10;
        Line line = makeCircularLine(maxPoints, centerX, centerY, radius);
        assertPointsOnCircle(centerX, centerY, radius, line);
        GeometrySimplifier<Line> simplifier = new GeometrySimplifier.LineStrings(maxPoints, calculator());
        // Test full geometry simplification
        Line simplified = simplifier.simplify(line);
        assertLineEnds(maxPoints, simplified, line);
        assertPointsOnCircle(centerX, centerY, radius, simplified);

        // Test streaming simplification
        simplifier.reset();
        for (int i = 0; i < line.length(); i++) {
            simplifier.consume(line.getX(i), line.getY(i));
        }
        Line streamSimplified = simplifier.produce();
        assertLineEnds(maxPoints, streamSimplified, line);
        assertPointsOnCircle(centerX, centerY, radius, streamSimplified);
        assertThat("Same line", streamSimplified, equalTo(simplified));
    }

    public void testCircularPolygon() {
        int maxPoints = 10;
        double centerX = 6;
        double centerY = 48;
        double radius = 10;
        Polygon polygon = makeCircularPolygon(maxPoints, centerX, centerY, radius);
        assertPointsOnCircle(centerX, centerY, radius, polygon);
        GeometrySimplifier<Polygon> simplifier = new GeometrySimplifier.Polygons(maxPoints, calculator());
        // Test full geometry simplification
        Polygon simplified = simplifier.simplify(polygon);
        assertPolygonEnds("Polygon", maxPoints, simplified, polygon);
        assertPointsOnCircle(centerX, centerY, radius, simplified);

        // Test streaming simplification
        simplifier.reset();
        LinearRing ring = polygon.getPolygon();
        for (int i = 0; i < ring.length(); i++) {
            simplifier.consume(ring.getX(i), ring.getY(i));
        }
        Polygon streamSimplified = simplifier.produce();
        assertPolygonEnds("Polygon", maxPoints, simplified, polygon);
        assertPointsOnCircle(centerX, centerY, radius, streamSimplified);
        assertThat("Same line", streamSimplified, equalTo(simplified));
        StringBuilder sb = new StringBuilder("GEOMETRYCOLLECTION(");
        sb.append(debugPolygon(polygon));
        sb.append(", ");
        sb.append(debugPolygon(simplified));
        // System.out.println(sb.append(")"));
    }

    public void testComplexGeoJsonMultiPolygon() throws IOException, ParseException {
        int maxPoints1 = 2000;
        int maxPoints2 = 100;
        StringBuilder sb = new StringBuilder();
        Polygon[] polygons = makePolygonsFromGeoJsonFile("us.json.gz");
        int maxPolyLength = Arrays.stream(polygons).reduce(0, (v, p) -> Math.max(v, p.getPolygon().length()), Math::max);
        MultiPolygon multiPolygon = new MultiPolygon(Arrays.asList(polygons));
        for (int maxPoints : new int[] { maxPoints1, maxPoints2 }) {
            var simplifier = new GeometrySimplifier.MultiPolygons(maxPoints, calculator());
            MultiPolygon simplifiedMultiPolygon = simplifier.simplify(multiPolygon);
            for (int i = 0; i < simplifiedMultiPolygon.size(); i++) {
                Polygon simplified = simplifiedMultiPolygon.get(i);
                Polygon polygon = multiPolygon.get(simplifier.indexOf(i));
                double simplificationFactor = (double) maxPoints / maxPolyLength;
                int maxPolyPoints = Math.max(4, (int) (simplificationFactor * polygon.getPolygon().length()));
                assertPolygonEnds("Polygon[" + i + "]", maxPolyPoints, simplified, polygon);
                if (sb.length() == 0) {
                    sb.append("GEOMETRYCOLLECTION(");
                } else {
                    sb.append(", ");
                }
                sb.append(debugPolygon(simplified));
            }
        }
        // System.out.println(sb.append(")"));
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

    private String debugPolygon(Polygon polygon) {
        StringBuilder sb = new StringBuilder("POLYGON((");
        LinearRing ring = polygon.getPolygon();
        for (int i = 0; i < ring.length(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(ring.getX(i)).append(" ").append(ring.getY(i));
        }
        return sb.append("))").toString();
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

    private Line makeCircularLine(int maxPoints, double centerX, double centerY, double radiusDegrees) {
        int length = maxPoints * 10;
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

    private Polygon makeCircularPolygon(int maxPoints, double centerX, double centerY, double radiusDegrees) {
        int length = maxPoints * 10;
        double radiusMeters = 40000000 * radiusDegrees / 360;
        var polygon = CircleUtils.createRegularGeoShapePolygon(new Circle(centerX, centerY, radiusMeters), length);
        assertPointsOnCircle(centerX, centerY, radiusDegrees, polygon);
        return polygon;
    }

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
