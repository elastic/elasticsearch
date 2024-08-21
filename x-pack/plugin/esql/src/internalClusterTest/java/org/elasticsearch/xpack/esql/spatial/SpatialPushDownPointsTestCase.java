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
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Base class to check that a query than can be pushed down gives the same result
 * if it is actually pushed down and when it is executed by the compute engine,
 *
 * For doing that we create two indices, one fully indexed and another with index
 * and doc values disabled. Then we index the same data in both indices and we check
 * that the same ES|QL queries produce the same results in both.
 */
public abstract class SpatialPushDownPointsTestCase extends SpatialPushDownTestCase {
    public void testSimplePointInPolygon() {
        assumeTrue("Test for points only", fieldType().contains("point"));

        assertAcked(prepareCreate("indexed").setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
               "location": { "type" : "%s" }
              }
            }
            """, fieldType())));

        assertAcked(prepareCreate("not-indexed").setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
               "location": { "type" : "%s",  "index" : false, "doc_values" : false }
              }
            }
            """, fieldType())));

        ArrayList<MultiPointTest> data = new ArrayList<>();
        // data that intersects and is within the polygon
        data.add(new MultiPointTest("[[5,-5],[-5,5]]", true, true));
        data.add(new MultiPointTest("[\"0,0\",\"1,0\"]", true, true));
        data.add(new MultiPointTest("\"POINT(9 9)\"", true, true));
        data.add(new MultiPointTest("[\"POINT(-9 -9)\",\"POINT(9 9)\"]", true, true));
        // data that intersects but is not within the polygon
        data.add(new MultiPointTest("[[5,5],[15,15]]", true, false));
        data.add(new MultiPointTest("[\"0,0\",\"11,0\"]", true, false));
        data.add(new MultiPointTest("[\"POINT(-9 -19)\",\"POINT(9 9)\"]", true, false));
        // data that does not intersect
        data.add(new MultiPointTest("[[5,15],[15,5]]", false, false));
        data.add(new MultiPointTest("[\"0,11\",\"11,0\"]", false, false));
        data.add(new MultiPointTest("\"POINT(19 9)\"", false, false));
        data.add(new MultiPointTest("[\"POINT(-9 -19)\",\"POINT(19 9)\"]", false, false));

        int expectedIntersects = 0;
        int expectedWithin = 0;
        for (int i = 0; i < data.size(); i++) {
            index("indexed", i + "", "{\"location\" : " + data.get(i).data + " }");
            index("not-indexed", i + "", "{\"location\" : " + data.get(i).data + " }");
            expectedIntersects += data.get(i).intersects ? 1 : 0;
            expectedWithin += data.get(i).within ? 1 : 0;
        }
        refresh("indexed", "not-indexed");
        int expectedDisjoint = data.size() - expectedIntersects;

        for (String polygon : new String[] {
            "POLYGON ((-10 -10, -10 10, 10 10, 10 -10, -10 -10))",
            "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))" }) {
            System.out.println("Querying with " + polygon);
            assertFunction("ST_WITHIN", polygon, expectedWithin);
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
            System.out.println("[" + spatialFunction + "]:\t" + indexedResult + "\t" + notIndexedResult);
            assertEquals(spatialFunction + "[expected=" + expected + "]", expected, indexedResult);
            assertEquals(spatialFunction + "[expected=" + expected + "]", indexedResult, notIndexedResult);
        }
    }

    private record MultiPointTest(String data, boolean intersects, boolean within) {}
}
