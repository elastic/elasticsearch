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
import java.util.Locale;

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
}
