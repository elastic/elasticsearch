/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class SpatialPushDownShapeTestCase extends SpatialPushDownTestCase {

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
            EsqlQueryResponse response1 = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query1).get();
            EsqlQueryResponse response2 = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query2).get();
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
            EsqlQueryResponse response1 = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query1).get();
            EsqlQueryResponse response2 = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query2).get();
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
}
