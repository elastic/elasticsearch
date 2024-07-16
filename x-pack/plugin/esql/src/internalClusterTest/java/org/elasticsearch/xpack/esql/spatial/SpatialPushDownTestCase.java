/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
public abstract class SpatialPushDownTestCase extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPlugin.class, SpatialPlugin.class);
    }

    /**
     * Elasticsearch field type
     */
    protected abstract String fieldType();

    /**
     * A random {@link Geometry} to be indexed.
     */
    protected abstract Geometry getIndexGeometry();

    /**
     * A random {@link Geometry} to be used for querying.
     */
    protected abstract Geometry getQueryGeometry();

    /**
     * Necessary to build a ES|QL query. It should be "TO_GEOSHAPE" for geo
     * fields and "TO_CARTESIANSHAPE" for cartesian fields.
     */
    protected abstract String castingFunction();

    public void testPushedDownQueriesSingleValue() throws RuntimeException {
        assertPushedDownQueries(false);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/110830")
    public void testPushedDownQueriesMultiValue() throws RuntimeException {
        assertPushedDownQueries(true);
    }

    private void assertPushedDownQueries(boolean multiValue) throws RuntimeException {
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
        for (int i = 0; i < random().nextInt(50, 100); i++) {
            if (multiValue) {
                final String[] values = new String[randomIntBetween(1, 5)];
                for (int j = 0; j < values.length; j++) {
                    values[j] = "\"" + WellKnownText.toWKT(getIndexGeometry()) + "\"";
                }
                index("indexed", i + "", "{\"location\" : " + Arrays.toString(values) + " }");
                index("not-indexed", i + "", "{\"location\" : " + Arrays.toString(values) + " }");
            } else {
                final String value = WellKnownText.toWKT(getIndexGeometry());
                index("indexed", i + "", "{\"location\" : \"" + value + "\" }");
                index("not-indexed", i + "", "{\"location\" : \"" + value + "\" }");
            }
        }

        refresh("indexed", "not-indexed");

        for (int i = 0; i < 10; i++) {
            final Geometry geometry = getQueryGeometry();
            final String wkt = WellKnownText.toWKT(geometry);
            assertFunction("ST_INTERSECTS", wkt);
            assertFunction("ST_DISJOINT", wkt);
            assertFunction("ST_CONTAINS", wkt);
            // within and lines are not globally supported so we avoid it here
            if (containsLine(geometry) == false) {
                assertFunction("ST_WITHIN", wkt);
            }
        }
    }

    private void assertFunction(String spatialFunction, String wkt) {
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
            assertEquals(response1.response().column(0).iterator().next(), response2.response().column(0).iterator().next());
        }
    }

    private static boolean containsLine(Geometry geometry) {
        if (geometry instanceof GeometryCollection<?> collection) {
            for (Geometry g : collection) {
                if (containsLine(g)) {
                    return true;
                }
            }
            return false;
        } else {
            return geometry.type() == ShapeType.LINESTRING || geometry.type() == ShapeType.MULTILINESTRING;
        }
    }
}
