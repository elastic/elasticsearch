/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class SpatialPushDownShapeTestCase extends SpatialPushDownTestCase<Geometry> {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SpatialPlugin.class, EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    public void testSimpleShapeContainsPolygon() {
        assumeTrue("Test for shapes only", fieldType().contains("shape"));
        initIndexes();
        ArrayList<ShapeContainsTest> data = new ArrayList<>();

        // data that intersects and contains the polygon
        data.add(new ShapeContainsTest(true, true, square(0, 0, 15)));
        data.add(new ShapeContainsTest(true, true, square(0, 0, 15), square(20, 20, 5)));
        // data that intersects but does not contain the polygon
        data.add(new ShapeContainsTest(true, false, square(0, 0, 5)));
        data.add(new ShapeContainsTest(true, false, square(0, 0, 5), square(20, 20, 5)));
        data.add(new ShapeContainsTest(true, false, square(-5, -5, 5), square(5, 5, 5)));
        // data that does not intersect the polygon
        data.add(new ShapeContainsTest(false, false, square(20, 20, 5)));
        data.add(new ShapeContainsTest(false, false, square(-20, -20, 5), square(20, 20, 5)));
        // data that geometrically contains the polygon but due to lucene's triangle-tree implementation, it cannot
        data.add(new ShapeContainsTest(true, false, square(0, 0, 15), square(10, 10, 5)));

        int expectedIntersects = 0;
        int expectedContains = 0;
        for (int i = 0; i < data.size(); i++) {
            index("indexed", i + "", "{\"location\" : " + data.get(i).toJson() + " }");
            index("not-indexed", i + "", "{\"location\" : " + data.get(i).toJson() + " }");
            expectedIntersects += data.get(i).intersects ? 1 : 0;
            expectedContains += data.get(i).contains ? 1 : 0;
        }
        refresh("indexed", "not-indexed");
        int expectedDisjoint = data.size() - expectedIntersects;

        for (String polygon : new String[] {
            "POLYGON ((-10 -10, -10 10, 10 10, 10 -10, -10 -10))",
            "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))" }) {
            assertFunction("ST_CONTAINS", polygon, expectedContains);
            assertFunction("ST_INTERSECTS", polygon, expectedIntersects);
            assertFunction("ST_DISJOINT", polygon, expectedDisjoint);
        }
    }

    public void testSimpleShapeContainsTwoPolygons() {
        assumeTrue("Test for shapes only", fieldType().contains("shape"));
        initIndexes();
        String twoPolygonsInCollection = "GEOMETRYCOLLECTION(" + square(-5, -5, 4) + "," + square(5, 5, 4) + ")";
        String twoPolygonsInMultiPolygon = collectionToMultiPolygon(twoPolygonsInCollection);
        String twoPolygonsInCollectionField = "GEOMETRYCOLLECTION(" + square(-5, -5, 5) + "," + square(5, 5, 5) + ")";
        String twoPolygonsInMultiPolygonField = collectionToMultiPolygon(twoPolygonsInCollectionField);

        ArrayList<ShapeContainsTest> data = new ArrayList<>();
        // data that intersects and contains the geometrycollection
        data.add(new ShapeContainsTest(true, true, twoPolygonsInCollectionField));
        data.add(new ShapeContainsTest(true, true, twoPolygonsInMultiPolygonField));
        data.add(new ShapeContainsTest(true, true, square(0, 0, 15)));
        data.add(new ShapeContainsTest(true, true, square(0, 0, 15), square(20, 20, 5)));
        data.add(new ShapeContainsTest(true, true, square(-5, -5, 5), square(5, 5, 5)));
        // data that intersects but does not contain the geometrycollection
        data.add(new ShapeContainsTest(true, false, square(0, 0, 5)));
        data.add(new ShapeContainsTest(true, false, square(0, 0, 5), square(20, 20, 5)));
        data.add(new ShapeContainsTest(true, false, square(-5, -5, 4), square(5, 5, 4)));
        // data that does not intersect the geometrycollection
        data.add(new ShapeContainsTest(false, false, square(20, 20, 5)));
        data.add(new ShapeContainsTest(false, false, square(-20, -20, 5), square(20, 20, 5)));
        // data that geometrically contains the geometrycollection but due to lucene's triangle-tree implementation, it cannot
        data.add(new ShapeContainsTest(true, false, square(0, 0, 15), square(10, 10, 5)));
        data.add(new ShapeContainsTest(true, false, square(-5, -5, 6), square(5, 5, 6)));

        int expectedIntersects = 0;
        int expectedContains = 0;
        for (int i = 0; i < data.size(); i++) {
            index("indexed", i + "", "{\"location\" : " + data.get(i).toJson() + " }");
            index("not-indexed", i + "", "{\"location\" : " + data.get(i).toJson() + " }");
            expectedIntersects += data.get(i).intersects ? 1 : 0;
            expectedContains += data.get(i).contains ? 1 : 0;
        }
        refresh("indexed", "not-indexed");
        int expectedDisjoint = data.size() - expectedIntersects;

        for (String twoPolygons : new String[] { twoPolygonsInCollection, twoPolygonsInMultiPolygon }) {
            assertFunction("ST_CONTAINS", twoPolygons, expectedContains);
            assertFunction("ST_INTERSECTS", twoPolygons, expectedIntersects);
            assertFunction("ST_DISJOINT", twoPolygons, expectedDisjoint);
        }
    }

    public void testMultiShapeContainsMultiPolygon() {
        assumeTrue("Test for shapes only", fieldType().contains("shape"));
        initIndexes();
        ArrayList<MultiShapeContainsTest> data = new ArrayList<>();

        // Contains succeeds with multiple non-intersecting polygons
        data.add(new MultiShapeContainsTest(true).small(square(0, 0, 4), square(20, 20, 4)).big(square(0, 0, 5), square(20, 20, 5)));
        data.add(new MultiShapeContainsTest(true).small(square(-5, -5, 3), square(5, 5, 3)).big(square(-5, -5, 4), square(5, 5, 4)));

        // Contains fails with multiple intersecting polygons due to the way it is implemented in the lucene triangle tree
        data.add(new MultiShapeContainsTest(false).small(square(0, 0, 14), square(10, 10, 4)).big(square(0, 0, 15), square(10, 10, 5)));
        data.add(new MultiShapeContainsTest(false).small(square(-5, -5, 5), square(5, 5, 5)).big(square(-5, -5, 6), square(5, 5, 6)));

        for (int i = 0; i < data.size(); i++) {
            index("indexed", "" + i, "{\"location\" : " + data.get(i).smallJson() + " }");
            index("not-indexed", "" + i, "{\"location\" : " + data.get(i).smallJson() + " }");
            index("indexed", "" + (i + 100), "{\"location\" : " + data.get(i).bigJson() + " }");
            index("not-indexed", "" + (i + 100), "{\"location\" : " + data.get(i).bigJson() + " }");
        }
        refresh("indexed", "not-indexed");

        for (int i = 0; i < data.size(); i++) {
            MultiShapeContainsTest datum = data.get(i);
            assertFunction(i, datum.smallJson(), "ST_WITHIN", datum.bigQuery(), true);
            assertFunction(i, datum.smallJson(), "ST_INTERSECTS", datum.bigQuery(), true);
            assertFunction(i + 100, datum.bigJson(), "ST_CONTAINS", datum.smallQuery(), datum.contains);
            assertFunction(i + 100, datum.bigJson(), "ST_INTERSECTS", datum.smallQuery(), true);
        }
    }

    private String collectionToMultiPolygon(String geometrytCollection) {
        return geometrytCollection.replace("POLYGON", "").replace("GEOMETRYCOLLECTION", "MULTIPOLYGON");
    }

    protected void assertFunction(String spatialFunction, String wkt, long expected) {
        final String query1 = String.format(Locale.ROOT, """
            FROM indexed | WHERE %s(location, %s("%s")) | STATS COUNT(*)
            """, spatialFunction, castingFunction(), wkt);
        final String query2 = String.format(Locale.ROOT, """
             FROM not-indexed | WHERE %s(location, %s("%s")) | STATS COUNT(*)
            """, spatialFunction, castingFunction(), wkt);
        try (
            EsqlQueryResponse response1 = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query1)).actionGet();
            EsqlQueryResponse response2 = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query2)).actionGet();
        ) {
            Object indexedResult = response1.response().column(0).iterator().next();
            Object notIndexedResult = response2.response().column(0).iterator().next();
            assertEquals(spatialFunction + "[expected=" + expected + "]", expected, indexedResult);
            assertEquals(spatialFunction + "[expected=" + expected + "]", indexedResult, notIndexedResult);
        }
    }

    private void assertFunction(int id, String expected, String spatialFunction, String wkt, boolean expectToFind) {
        expected = expected.replaceAll("\\.0+", ".0").replace("\"", "");
        final String predicate = String.format(Locale.ROOT, "WHERE %s(location, %s(\"%s\"))", spatialFunction, castingFunction(), wkt);
        final String query1 = "FROM indexed METADATA _id | " + predicate + " | KEEP _id, location";
        final String query2 = "FROM not-indexed METADATA _id | " + predicate + " | KEEP _id, location";
        try (
            EsqlQueryResponse response1 = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query1)).actionGet();
            EsqlQueryResponse response2 = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query2)).actionGet();
        ) {
            record Result(int id, String location) {
                Result(Iterator<Object> iterator) {
                    this(Integer.parseInt(iterator.next().toString()), iterator.next().toString());
                }
            }
            ArrayList<Result> indexedResults = new ArrayList<>();
            ArrayList<Result> notIndexedResults = new ArrayList<>();
            response1.response().rows().forEach(row -> indexedResults.add(new Result(row.iterator())));
            response2.response().rows().forEach(row -> notIndexedResults.add(new Result(row.iterator())));
            assertThat("No results found at all", indexedResults.size() + notIndexedResults.size(), greaterThanOrEqualTo(0));
            boolean found = false;
            ArrayList<Result> missingFromIndexedResults = new ArrayList<>();
            ArrayList<Result> missingFromNotIndexedResults = new ArrayList<>();
            for (int i = 0, j = 0; i < indexedResults.size() && j < notIndexedResults.size();) {
                Result indexedResult = indexedResults.get(i);
                Result notIndexedResult = indexedResults.get(j);
                if (indexedResult.id() == notIndexedResult.id()) {
                    if (indexedResult.id() == id) {
                        assertEquals(spatialFunction + "[expected=" + expected + "]", expected, indexedResult.location);
                        assertEquals(spatialFunction + "[expected=" + expected + "]", indexedResult, notIndexedResult);
                        found = true;
                    }
                    i++;
                    j++;
                } else {
                    if (indexedResult.id() < notIndexedResult.id()) {
                        missingFromNotIndexedResults.add(indexedResult);
                        i++;
                    } else {
                        missingFromIndexedResults.add(notIndexedResult);
                        j++;
                    }
                }
            }
            if (missingFromIndexedResults.isEmpty() == false || missingFromNotIndexedResults.isEmpty() == false) {
                StringBuilder sb = new StringBuilder("Mismatching results between indexed and not-indexed: ");
                if (missingFromIndexedResults.isEmpty() == false) {
                    sb.append("Missing from indexed: ").append(missingFromIndexedResults);
                }
                if (missingFromNotIndexedResults.isEmpty() == false) {
                    sb.append("Missing from not-indexed: ").append(missingFromNotIndexedResults);
                }
                fail(sb.toString());
            }
            if (expectToFind && found == false) {
                fail("Expected result not found: " + expected);
            }
            if (expectToFind == false && found) {
                fail("Unexpected result found: " + expected);
            }
        }
    }

    private String square(double x, double y, double size) {
        return String.format(
            Locale.ROOT,
            "POLYGON ((%.2f %.2f, %.2f %.2f, %.2f %.2f, %.2f %.2f, %.2f %.2f))",
            x - size,
            y - size,
            x + size,
            y - size,
            x + size,
            y + size,
            x - size,
            y + size,
            x - size,
            y - size
        );
    }

    protected record ShapeContainsTest(boolean intersects, boolean contains, String... data) {
        private String toJson() {
            return Arrays.toString(Arrays.stream(data).map(s -> "\"" + s + "\"").toArray());
        }
    }

    protected record MultiShapeContainsTest(boolean contains, String[] bigger, String[] smaller) {
        private MultiShapeContainsTest(boolean contains) {
            this(contains, new String[] {}, new String[] {});
        }

        private MultiShapeContainsTest small(String... smaller) {
            return new MultiShapeContainsTest(contains, bigger, smaller);
        }

        private MultiShapeContainsTest big(String... bigger) {
            return new MultiShapeContainsTest(contains, bigger, smaller);
        }

        private String bigQuery() {
            return query(bigger);
        }

        private String smallQuery() {
            return query(smaller);
        }

        private String smallJson() {
            return json(smaller);
        }

        private String bigJson() {
            return json(bigger);
        }

        private static String json(String[] data) {
            return Arrays.toString(Arrays.stream(data).map(s -> "\"" + s + "\"").toArray());
        }

        private static String query(String[] data) {
            return data.length == 1 ? data[0] : "GEOMETRYCOLLECTION(" + String.join(", ", data) + ")";
        }
    }

    public void testStatsExtentOneShard() throws IOException, ParseException {
        statsExtentManyShards(1);
    }

    public void testStatsExtentManyShards() throws IOException, ParseException {
        statsExtentManyShards(16);
    }

    private void statsExtentManyShards(int numShards) throws IOException, ParseException {
        assumeTrue("Test for shapes only", fieldType().contains("shape"));
        initIndexes(numShards);
        boolean isCartesian = fieldType().equals("shape");

        ArrayList<ExtentTest> data = new ArrayList<>();
        // data that intersects and is within the polygon
        data.add(new ExtentTest("LINESTRING(5 5,-5 5)", true, true, isCartesian));
        data.add(new ExtentTest("LINESTRING(0 1,1 0)", true, true, isCartesian));
        data.add(new ExtentTest("POINT(9 9)", true, true, isCartesian));
        data.add(new ExtentTest("MULTIPOINT(-9 -9, 9 9)", true, true, isCartesian));
        data.add(new ExtentTest("POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5))", true, true, isCartesian));
        // data that intersects but is not within the polygon
        data.add(new ExtentTest("LINESTRING(5 5,15 15)", true, false, isCartesian));
        data.add(new ExtentTest("LINESTRING(0 0, 11 11)", true, false, isCartesian));
        data.add(new ExtentTest("MULTIPOINT(-9 -19, 9 9)", true, false, isCartesian));
        data.add(new ExtentTest("POLYGON ((-15 -15, 15 -15, 15 15, -15 15, -15 -15))", true, false, isCartesian));
        // data that does not intersect
        data.add(new ExtentTest("LINESTRING(5 20,20 5)", false, false, isCartesian));
        data.add(new ExtentTest("LINESTRING(0 11, 11 11, 11 0)", false, false, isCartesian));
        data.add(new ExtentTest("POINT(19 9)", false, false, isCartesian));
        data.add(new ExtentTest("MULTIPOINT(-9 -19, 19 9)", false, false, isCartesian));
        data.add(new ExtentTest("POLYGON ((11 11, 15 11, 15 15, 11 15, 11 11))", false, false, isCartesian));
        // Data that does not contain any geometries (to get null extents)
        data.add(new ExtentTest(null, false, false, null));
        data.add(new ExtentTest(null, false, false, null));
        data.add(new ExtentTest(null, false, false, null));
        data.add(new ExtentTest(null, false, false, null));

        int expectedIntersects = 0;
        int expectedWithin = 0;
        int expectedDisjoint = 0;
        TestExtentBuilder intersectsExtent = new TestExtentBuilder();
        TestExtentBuilder withinExtent = new TestExtentBuilder();
        TestExtentBuilder disjointExtent = new TestExtentBuilder();
        TestExtentBuilder totalExtent = new TestExtentBuilder();
        for (int i = 0; i < data.size(); i++) {
            if (data.get(i).data == null) {
                addEmptyToIndexes(i, "indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");
            } else {
                addToIndexes(i, "\"" + data.get(i).data + "\"", "indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");
            }
            if (data.get(i).extent != null) {
                totalExtent.add(data.get(i).extent);
                if (data.get(i).intersects) {
                    data.get(i).checkIntersects("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))", isCartesian);
                    expectedIntersects++;
                    intersectsExtent.add(data.get(i).extent);
                } else {
                    expectedDisjoint++;
                    disjointExtent.add(data.get(i).extent);
                }
                if (data.get(i).within) {
                    data.get(i).checkWithin("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))", isCartesian);
                    expectedWithin++;
                    withinExtent.add(data.get(i).extent);
                }
            }
        }
        refresh("indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");

        for (String polygon : new String[] {
            "POLYGON ((-10 -10, -10 10, 10 10, 10 -10, -10 -10))",
            "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))" }) {
            assertFunction("ST_WITHIN", polygon, expectedWithin, withinExtent.toRectangle(), numShards);
            assertFunction("ST_INTERSECTS", polygon, expectedIntersects, intersectsExtent.toRectangle(), numShards);
            assertFunction("ST_DISJOINT", polygon, expectedDisjoint, disjointExtent.toRectangle(), numShards);
            assertFunction(null, polygon, data.size(), totalExtent.toRectangle(), numShards);
        }
    }

    protected void addEmptyToIndexes(int id, String... indexes) {
        for (String index : indexes) {
            index(index, id + "", "{\"dummy\" : \"" + id + "\" }");
        }
    }

    protected void assertFunction(String spatialFunction, String wkt, long expected, Rectangle expectedExtent, int expectedShards)
        throws IOException, ParseException {
        String prefix = spatialFunction == null ? "ALL[expected=" : spatialFunction + "[expected=";
        String filter = spatialFunction == null
            ? ""
            : String.format(Locale.ROOT, " | WHERE %s(location, %s(\"%s\"))", spatialFunction, castingFunction(), wkt);
        List<String> queries = getQueries("FROM indexed" + filter + " | STATS COUNT(*), ST_EXTENT_AGG(location)");
        try (TestQueryResponseCollection responses = new TestQueryResponseCollection(queries)) {
            for (int i = 0; i < ALL_INDEXES.length; i++) {
                NumShards numShards = getNumShards(ALL_INDEXES[i]);
                assertThat("Number of shards for " + ALL_INDEXES[i], numShards.numPrimaries, equalTo(expectedShards));
                Object resultCount = responses.getResponse(i, 0);
                Object resultExtent = responses.getResponse(i, 1);
                assertEquals(prefix + expected + "] for " + ALL_INDEXES[i], expected, resultCount);
                assertThat(prefix + expectedExtent + "] for " + ALL_INDEXES[i], expectedExtent, matchesExtent(resultExtent));
            }
            long allIndexesCount = (long) responses.getResponse(ALL_INDEXES.length, 0);
            assertEquals(prefix + expected + "] for all indexes", expected * 4, allIndexesCount);
            Object allIndexesExtent = responses.getResponse(ALL_INDEXES.length, 1);
            assertThat(prefix + expectedExtent + "] for all indexes", expectedExtent, matchesExtent(allIndexesExtent));
        }
    }

    private record ExtentTest(String data, boolean intersects, boolean within, Rectangle extent) {
        private ExtentTest(String data, boolean intersects, boolean within, boolean isCartesian) throws IOException, ParseException {
            this(data, intersects, within, getExtent(data, isCartesian));
        }

        private void checkIntersects(String wkt, boolean isCartesian) throws IOException, ParseException {
            Rectangle testBox = getExtent(WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt), isCartesian);
            if (extent == null
                || extent.getMaxX() < testBox.getMinX()
                || extent.getMinX() > testBox.getMaxX()
                || extent.getMaxY() < testBox.getMinY()
                || extent.getMinY() > testBox.getMaxY()) {
                fail("Extent[" + extent + "] does not intersect test box[" + testBox + "]");
            }
        }

        private void checkWithin(String wkt, boolean isCartesian) throws IOException, ParseException {
            Rectangle testBox = getExtent(WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt), isCartesian);
            checkIntersects(wkt, isCartesian);
            if (extent.getMaxX() > testBox.getMaxX()
                || extent.getMinX() < testBox.getMinX()
                || extent.getMaxY() > testBox.getMaxY()
                || extent.getMinY() < testBox.getMinY()) {
                fail("Extent[" + extent + "] is not within test box[" + testBox + "]");
            }
        }
    }

    private Matcher<Rectangle> matchesExtent(Object result) throws IOException, ParseException {
        Geometry shape = WellKnownText.fromWKT(GeometryValidator.NOOP, false, result.toString());
        return matchesExtent(shape);
    }

    private static Rectangle getExtent(String wkt, boolean isCartesian) throws IOException, ParseException {
        Geometry shape = WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt);
        return getExtent(shape, isCartesian);
    }

    private static Rectangle getExtent(Geometry shape, boolean isCartesian) {
        SpatialEnvelopeVisitor.PointVisitor pointVisitor = isCartesian
            ? new SpatialEnvelopeVisitor.CartesianPointVisitor()
            : new SpatialEnvelopeVisitor.GeoPointVisitor(SpatialEnvelopeVisitor.WrapLongitude.WRAP);
        SpatialEnvelopeVisitor envelopeVisitor = new SpatialEnvelopeVisitor(pointVisitor);
        if (shape.visit(envelopeVisitor)) {
            return pointVisitor.getResult();
        }
        throw new IllegalArgumentException("Geometry is empty");
    }

    private Matcher<Rectangle> matchesExtent(Geometry geometry) {
        return new TestExtentMatcher(getExtent(geometry, fieldType().equals("shape")));
    }

    private static class TestExtentMatcher extends TypeSafeMatcher<Rectangle> {
        private final Matcher<Double> xMaxMatcher;
        private final Matcher<Double> yMaxMatcher;
        private final Matcher<Double> xMinMatcher;
        private final Matcher<Double> yMinMatcher;

        private TestExtentMatcher(Rectangle extent) {
            this.xMaxMatcher = matchDouble(extent.getMaxX());
            this.yMaxMatcher = matchDouble(extent.getMaxY());
            this.xMinMatcher = matchDouble(extent.getMinX());
            this.yMinMatcher = matchDouble(extent.getMinY());
        }

        private Matcher<Double> matchDouble(double value) {
            return closeTo(value, 0.0000001);
        }

        @Override
        public boolean matchesSafely(Rectangle actualExtent) {
            return xMaxMatcher.matches(actualExtent.getMaxX())
                && yMaxMatcher.matches(actualExtent.getMaxY())
                && xMinMatcher.matches(actualExtent.getMinX())
                && yMinMatcher.matches(actualExtent.getMinY());
        }

        @Override
        public void describeMismatchSafely(Rectangle actualExtent, Description description) {
            describeSubMismatch(xMaxMatcher, actualExtent.getMaxX(), "X Max", description);
            describeSubMismatch(yMaxMatcher, actualExtent.getMaxY(), "Y Max", description);
            describeSubMismatch(xMinMatcher, actualExtent.getMinX(), "X Min", description);
            describeSubMismatch(yMinMatcher, actualExtent.getMinY(), "Y Min", description);
        }

        private void describeSubMismatch(Matcher<Double> matcher, double value, String name, Description description) {
            if (matcher.matches(value) == false) {
                description.appendText("\n\t" + name + " ");
                matcher.describeMismatch(value, description);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(
                "Extent (xMax:" + xMaxMatcher + ", yMax:" + yMaxMatcher + ", xMin:" + xMinMatcher + ", yMin:" + yMinMatcher + ")"
            );
        }
    }

    protected static class TestExtentBuilder {
        double xMin = Double.POSITIVE_INFINITY;
        double xMax = Double.NEGATIVE_INFINITY;
        double yMin = Double.POSITIVE_INFINITY;
        double yMax = Double.NEGATIVE_INFINITY;

        void addTo(TestExtentBuilder other) {
            other.xMin = Math.min(other.xMin, xMin);
            other.xMax = Math.max(other.xMax, xMax);
            other.yMin = Math.min(other.yMin, yMin);
            other.yMax = Math.max(other.yMax, yMax);
        }

        void add(Rectangle rectangle) {
            xMin = Math.min(xMin, rectangle.getMinX());
            xMax = Math.max(xMax, rectangle.getMaxX());
            yMin = Math.min(yMin, rectangle.getMinY());
            yMax = Math.max(yMax, rectangle.getMaxY());
        }

        Rectangle toRectangle() {
            return new Rectangle(xMin, xMax, yMax, yMin);
        }
    }

    @Override
    protected Geometry quantize(Geometry shape) {
        TestQuantizedGeometryVisitor visitor = new TestQuantizedGeometryVisitor();
        return shape.visit(visitor);
    }

    private List<Double> getExtentBoundSorted(List<Rectangle> extents, Function<Rectangle, Double> extractor) {
        return extents.stream().map(extractor).sorted().toList();
    }

    private List<Double> getResponseSorted(TestQueryResponseCollection responses, int index, int column) {
        return responses.getResponses(index, column).stream().map(o -> (Double) o).sorted().toList();
    }

    @Override
    protected void assertQuantizedXY() {
        List<String> queries = getQueries("""
            FROM index
            | EVAL envelope = ST_ENVELOPE(location)
            | EVAL xmin = ST_XMIN(location)
            | EVAL xmax = ST_XMAX(location)
            | EVAL ymin = ST_YMIN(location)
            | EVAL ymax = ST_YMAX(location)
            | SORT xmin ASC, ymin ASC
            """);
        boolean isCartesian = fieldType().equals("shape");
        try (TestQueryResponseCollection responses = new TestQueryResponseCollection(queries)) {
            List<Geometry> quantizedShapes = getQuantizedResponsesAsType(responses, 0, 0, Geometry.class);
            List<Rectangle> quantizedExtents = quantizedShapes.stream().map(s -> getExtent(s, isCartesian)).toList();
            List<Double> xMinQuantized = getExtentBoundSorted(quantizedExtents, Rectangle::getMinX);
            List<Double> xMaxQuantized = getExtentBoundSorted(quantizedExtents, Rectangle::getMaxX);
            List<Double> yMinQuantized = getExtentBoundSorted(quantizedExtents, Rectangle::getMinY);
            List<Double> yMaxQuantized = getExtentBoundSorted(quantizedExtents, Rectangle::getMaxY);
            for (int index = 0; index < ALL_INDEXES.length; index++) {
                List<Geometry> resultShapes = getResponsesAsType(responses, index, 0, Geometry.class);
                int countDifferent = 0;
                for (int i = 0; i < quantizedShapes.size(); i++) {
                    if (quantizedShapes.get(i).equals(resultShapes.get(i)) == false) {
                        countDifferent++;
                    }
                }
                assertThat(
                    "Expected some different results in set of " + resultShapes.size() + " shapes for " + ALL_INDEXES[index],
                    countDifferent,
                    greaterThan(0)
                );
                for (int column = 1; column < 6; column++) {
                    if (index > 0) {
                        if (column == 1) {
                            // Envelope
                            List<Geometry> result = responses.getResponses(index, column).stream().map(o -> parse(o.toString())).toList();
                            assertEquals("Expected same number of rows " + ALL_INDEXES[index], quantizedShapes.size(), result.size());
                        } else {
                            List<Double> result = getResponseSorted(responses, index, column);
                            assertEquals("Expected same number of rows " + ALL_INDEXES[index], quantizedShapes.size(), result.size());
                            if (column == 2) {
                                // xmin
                                assertEquals("Same xmin values " + ALL_INDEXES[index], xMinQuantized, result);
                            } else if (column == 3) {
                                // xmax
                                assertEquals("Same xmax values " + ALL_INDEXES[index], xMaxQuantized, result);
                            } else if (column == 4) {
                                // ymin
                                assertEquals("Same ymin values " + ALL_INDEXES[index], yMinQuantized, result);
                            } else {
                                // ymax
                                assertEquals("Same ymax values " + ALL_INDEXES[index], yMaxQuantized, result);
                            }
                        }
                    }
                }
            }
        }
    }

    protected class TestQuantizedGeometryVisitor implements GeometryVisitor<Geometry, RuntimeException> {
        @Override
        public Geometry visit(Circle circle) throws RuntimeException {
            Point center = quantizePoint(new Point(circle.getX(), circle.getY()));
            return new Circle(center.getX(), center.getY(), circle.getRadiusMeters());
        }

        @Override
        public Geometry visit(GeometryCollection<?> collection) throws RuntimeException {
            List<Geometry> quantizedGeometries = new ArrayList<>();
            for (Geometry geometry : collection) {
                quantizedGeometries.add(geometry.visit(this));
            }
            return new GeometryCollection<>(quantizedGeometries);
        }

        @Override
        public Geometry visit(Line line) throws RuntimeException {
            double[] x = new double[line.length()];
            double[] y = new double[line.length()];
            for (int i = 0; i < line.length(); i++) {
                Point quantizedPoint = quantizePoint(new Point(line.getX(i), line.getY(i)));
                x[i] = quantizedPoint.getX();
                y[i] = quantizedPoint.getY();
            }
            return new Line(x, y);
        }

        @Override
        public Geometry visit(LinearRing ring) throws RuntimeException {
            double[] x = new double[ring.length()];
            double[] y = new double[ring.length()];
            for (int i = 0; i < ring.length(); i++) {
                Point quantizedPoint = quantizePoint(new Point(ring.getX(i), ring.getY(i)));
                x[i] = quantizedPoint.getX();
                y[i] = quantizedPoint.getY();
            }
            return new LinearRing(x, y);
        }

        @Override
        public Geometry visit(MultiLine multiLine) throws RuntimeException {
            List<Line> lines = new ArrayList<>();
            for (Line line : multiLine) {
                lines.add((Line) line.visit(this));
            }
            return new MultiLine(lines);
        }

        @Override
        public Geometry visit(MultiPoint multiPoint) throws RuntimeException {
            List<Point> points = new ArrayList<>();
            for (Point point : multiPoint) {
                points.add((Point) point.visit(this));
            }
            return new MultiPoint(points);
        }

        @Override
        public Geometry visit(MultiPolygon multiPolygon) throws RuntimeException {
            List<Polygon> polygons = new ArrayList<>();
            for (Polygon polygon : multiPolygon) {
                polygons.add((Polygon) polygon.visit(this));
            }
            return new MultiPolygon(polygons);
        }

        @Override
        public Geometry visit(Point point) throws RuntimeException {
            return quantizePoint(point);
        }

        @Override
        public Geometry visit(Polygon polygon) throws RuntimeException {
            LinearRing shell = (LinearRing) polygon.getPolygon().visit(this);
            List<LinearRing> holes = new ArrayList<>();
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                holes.add((LinearRing) polygon.getHole(i).visit(this));
            }
            return new Polygon(shell, holes);
        }

        @Override
        public Geometry visit(Rectangle rectangle) throws RuntimeException {
            Point minPoint = quantizePoint(new Point(rectangle.getMinX(), rectangle.getMinY()));
            Point maxPoint = quantizePoint(new Point(rectangle.getMaxX(), rectangle.getMaxY()));
            return new Rectangle(minPoint.getX(), maxPoint.getX(), maxPoint.getY(), minPoint.getY());
        }
    }
}
