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
        // data that does not intersect
        data.add(new ShapeContainsTest(false, false, square(20, 20, 5)));
        data.add(new ShapeContainsTest(false, false, square(-20, -20, 5), square(20, 20, 5)));
        data.clear();
        // data that geometrically contains the polygon but due to lucene's triangle-tree implementation, it cannot
        data.add(new ShapeContainsTest(true, false, square(0, 0, 15), square(10, 10, 5)));

        int expectedIntersects = 0;
        int expectedContains = 0;
        for (int i = 0; i < data.size(); i++) {
            index("indexed", i + "", "{\"location\" : " + Arrays.toString(data.get(i).data) + " }");
            index("not-indexed", i + "", "{\"location\" : " + Arrays.toString(data.get(i).data) + " }");
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
            "\"POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))\"",
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

    private record ShapeContainsTest(boolean intersects, boolean contains, String... data) {}
}
