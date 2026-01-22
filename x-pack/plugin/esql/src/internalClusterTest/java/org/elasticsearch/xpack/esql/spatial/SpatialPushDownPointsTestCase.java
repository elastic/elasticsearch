/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.closeTo;

/**
 * Base class to check that a query than can be pushed down gives the same result
 * if it is actually pushed down and when it is executed by the compute engine,
 *
 * For doing that we create two indices, one fully indexed and another with index
 * and doc values disabled. Then we index the same data in both indices and we check
 * that the same ES|QL queries produce the same results in both.
 */
public abstract class SpatialPushDownPointsTestCase extends SpatialPushDownTestCase {
    public void testSimplePointInPolygon() throws IOException, ParseException {
        assumeTrue("Test for points only", fieldType().contains("point"));
        initIndexes();

        ArrayList<MultiPointTest> data = new ArrayList<>();
        // data that intersects and is within the polygon
        data.add(new MultiPointTest("[[5,5],[-5,5]]", true, true, new TestCentroid(0, 10, 2)));
        data.add(new MultiPointTest("[\"0,1\",\"1,0\"]", true, true, new TestCentroid(1, 1, 2)));
        data.add(new MultiPointTest("\"POINT(9 9)\"", true, true, new TestCentroid(9, 9, 1)));
        data.add(new MultiPointTest("[\"POINT(-9 -9)\",\"POINT(9 9)\"]", true, true, new TestCentroid(0, 0, 2)));
        // data that intersects but is not within the polygon
        data.add(new MultiPointTest("[[5,5],[15,15]]", true, false, new TestCentroid(20, 20, 2)));
        data.add(new MultiPointTest("[\"0,0\",\"11,11\"]", true, false, new TestCentroid(11, 11, 2)));
        data.add(new MultiPointTest("[\"POINT(-9 -19)\",\"POINT(9 9)\"]", true, false, new TestCentroid(0, -10, 2)));
        // data that does not intersect
        data.add(new MultiPointTest("[[5,15],[15,5]]", false, false, new TestCentroid(20, 20, 2)));
        data.add(new MultiPointTest("[\"0,11\",\"11,0\"]", false, false, new TestCentroid(11, 11, 2)));
        data.add(new MultiPointTest("\"POINT(19 9)\"", false, false, new TestCentroid(19, 9, 1)));
        data.add(new MultiPointTest("[\"POINT(-9 -19)\",\"POINT(19 9)\"]", false, false, new TestCentroid(10, -10, 2)));

        int expectedIntersects = 0;
        int expectedWithin = 0;
        int expectedDisjoint = 0;
        CentroidCalculator intersectsCentroid = new CentroidCalculator();
        CentroidCalculator withinCentroid = new CentroidCalculator();
        CentroidCalculator disjointCentroid = new CentroidCalculator();
        for (int i = 0; i < data.size(); i++) {
            addToIndexes(i, data.get(i).data, "indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");
            if (data.get(i).intersects) {
                expectedIntersects++;
                data.get(i).centroid.addTo(intersectsCentroid);
            } else {
                expectedDisjoint++;
                data.get(i).centroid.addTo(disjointCentroid);
            }
            if (data.get(i).within) {
                expectedWithin++;
                data.get(i).centroid.addTo(withinCentroid);
            }
        }
        refresh("indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");

        for (String polygon : new String[] {
            "POLYGON ((-10 -10, -10 10, 10 10, 10 -10, -10 -10))",
            "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))" }) {
            assertFunction("ST_WITHIN", polygon, expectedWithin, withinCentroid);
            assertFunction("ST_INTERSECTS", polygon, expectedIntersects, intersectsCentroid);
            assertFunction("ST_DISJOINT", polygon, expectedDisjoint, disjointCentroid);
        }
    }

    protected void assertFunction(String spatialFunction, String wkt, long expected, CentroidCalculator centroid) throws IOException,
        ParseException {
        List<String> queries = getQueries(String.format(Locale.ROOT, """
            FROM indexed | WHERE %s(location, %s("%s")) | STATS COUNT(*), ST_CENTROID_AGG(location)
            """, spatialFunction, castingFunction(), wkt));
        try (TestQueryResponseCollection responses = new TestQueryResponseCollection(queries)) {
            for (int i = 0; i < ALL_INDEXES.length; i++) {
                Object resultCount = responses.getResponse(i, 0);
                Object resultCentroid = responses.getResponse(i, 1);
                assertEquals(spatialFunction + "[expected=" + expected + "] for " + ALL_INDEXES[i], expected, resultCount);
                assertThat(
                    spatialFunction + "[expected=" + toString(centroid) + "] for " + ALL_INDEXES[i],
                    centroid,
                    matchesCentroid(resultCentroid)
                );
            }
            long allIndexesCount = (long) responses.getResponse(ALL_INDEXES.length, 0);
            assertEquals(spatialFunction + "[expected=" + expected + "] for all indexes", expected * 4, allIndexesCount);
            Object allIndexesCentroid = responses.getResponse(ALL_INDEXES.length, 1);
            assertThat(
                spatialFunction + "[expected=" + toString(centroid) + "] for all indexes",
                centroid,
                matchesCentroid(allIndexesCentroid)
            );
        }
    }

    public void testPushedDownDistanceSingleValue() throws RuntimeException {
        assertPushedDownDistance(false);
    }

    public void testPushedDownDistanceMultiValue() throws RuntimeException {
        assertPushedDownDistance(true);
    }

    private void assertPushedDownDistance(boolean multiValue) throws RuntimeException {
        initIndexes();
        for (int i = 0; i < random().nextInt(50, 100); i++) {
            if (multiValue) {
                final String[] values = new String[randomIntBetween(1, 5)];
                for (int j = 0; j < values.length; j++) {
                    values[j] = "\"" + WellKnownText.toWKT(getIndexGeometry()) + "\"";
                }
                addToIndexes(i, Arrays.toString(values), "indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");
            } else {
                final String value = WellKnownText.toWKT(getIndexGeometry());
                addToIndexes(i, "\"" + value + "\"", "indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");
            }
        }

        refresh("indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");

        for (int i = 0; i < 10; i++) {
            final Geometry geometry = getIndexGeometry();
            final String wkt = WellKnownText.toWKT(geometry);
            assertDistanceFunction(wkt);
        }
    }

    protected abstract double searchDistance();

    protected void assertDistanceFunction(String wkt) {
        String spatialFunction = "ST_DISTANCE";
        String castingFunction = castingFunction().replaceAll("SHAPE", "POINT");
        List<String> queries = getQueries(String.format(Locale.ROOT, """
            FROM index | WHERE %s(location, %s("%s")) < %.1f | STATS COUNT(*)
            """, spatialFunction, castingFunction, wkt, searchDistance()));
        try (TestQueryResponseCollection responses = new TestQueryResponseCollection(queries)) {
            Object indexedResult = responses.getResponse(0, 0);
            for (int i = 1; i < ALL_INDEXES.length; i++) {
                Object result = responses.getResponse(i, 0);
                assertEquals(spatialFunction + " for " + ALL_INDEXES[i], indexedResult, result);
            }
            long allIndexesResult = (long) responses.getResponse(ALL_INDEXES.length, 0);
            assertEquals(spatialFunction + " for all indexes", (long) indexedResult * 4, allIndexesResult);
        }
    }

    private String toString(CentroidCalculator centroid) {
        return "Centroid (x:" + centroid.getX() + ", y:" + centroid.getY() + ")";
    }

    private record MultiPointTest(String data, boolean intersects, boolean within, TestCentroid centroid) {}

    private static class TestCentroid {
        private final double x;
        private final double y;
        private final long count;

        TestCentroid(double x, double y, long count) {
            this.x = x;
            this.y = y;
            this.count = count;
        }

        private void addTo(CentroidCalculator calculator) {
            for (long i = 0; i < count; i++) {
                calculator.add(asPoint());
            }
        }

        private double x() {
            return x / count;
        }

        private double y() {
            return y / count;
        }

        private Point asPoint() {
            return new Point(x(), y());
        }
    }

    private Matcher<CentroidCalculator> matchesCentroid(Object result) throws IOException, ParseException {
        Point point = (Point) WellKnownText.fromWKT(GeometryValidator.NOOP, false, result.toString());
        return matchesCentroid(point);
    }

    private Matcher<CentroidCalculator> matchesCentroid(Point point) {
        return new TestCentroidMatcher(point.getX(), point.getY());
    }

    private static class TestCentroidMatcher extends TypeSafeMatcher<CentroidCalculator> {
        private final Matcher<Double> xMatcher;
        private final Matcher<Double> yMatcher;

        private TestCentroidMatcher(double x, double y) {
            this.xMatcher = matchDouble(x);
            this.yMatcher = matchDouble(y);
        }

        private Matcher<Double> matchDouble(double value) {
            return closeTo(value, 0.0000001);
        }

        @Override
        public boolean matchesSafely(CentroidCalculator actualCentroid) {
            return xMatcher.matches(actualCentroid.getX()) && yMatcher.matches(actualCentroid.getY());
        }

        @Override
        public void describeMismatchSafely(CentroidCalculator actualCentroid, Description description) {
            describeSubMismatch(xMatcher, actualCentroid.getX(), "X value", description);
            describeSubMismatch(yMatcher, actualCentroid.getY(), "Y value", description);
        }

        private void describeSubMismatch(Matcher<Double> matcher, double value, String name, Description description) {
            if (matcher.matches(value) == false) {
                description.appendText("\n\t" + name + " ");
                matcher.describeMismatch(value, description);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("Centroid (x:" + xMatcher.toString() + ", y:" + yMatcher + ")");
        }
    }
}
