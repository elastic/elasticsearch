/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Base class to check that a query that can be pushed down gives the same result
 * if it is actually pushed down and when it is executed by the compute engine,
 * <p>
 * For doing that, we create two indices, one fully indexed and another with index
 * and doc values disabled. Then we index the same data in both indices, and we check
 * that the same ES|QL queries produce the same results in both.
 */
public abstract class SpatialPushDownTestCase<T extends Geometry> extends ESIntegTestCase {

    protected static final String[] ALL_INDEXES = new String[] { "indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values" };

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

    public void testPushedDownQueriesMultiValue() throws RuntimeException {
        assertPushedDownQueries(true);
    }

    protected void initIndexes() {
        initIndexes(Settings.builder());
    }

    protected void initIndexes(int numberOfShards) {
        initIndexes(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards));
    }

    protected void initIndexes(Settings.Builder indexSettings) {
        assertAcked(prepareCreate("indexed", indexSettings).setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
               "location": { "type" : "%s" }
              }
            }
            """, fieldType())));

        assertAcked(prepareCreate("not-indexed", indexSettings).setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
               "location": { "type" : "%s",  "index" : false, "doc_values" : true }
              }
            }
            """, fieldType())));

        assertAcked(prepareCreate("not-indexed-nor-doc-values", indexSettings).setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
               "location": { "type" : "%s",  "index" : false, "doc_values" : false }
              }
            }
            """, fieldType())));

        assertAcked(prepareCreate("no-doc-values", indexSettings).setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
               "location": { "type" : "%s",  "index" : true, "doc_values" : false }
              }
            }
            """, fieldType())));
    }

    protected void addToIndexes(int id, String values, String... indexes) {
        for (String index : indexes) {
            index(index, id + "", "{\"location\" : " + values + " }");
        }
    }

    private void assertPushedDownQueries(boolean multiValue) throws RuntimeException {
        initIndexes();
        for (int i = 0; i < random().nextInt(50, 100); i++) {
            if (multiValue) {
                final String[] values = new String[randomIntBetween(1, 5)];
                for (int j = 0; j < values.length; j++) {
                    values[j] = "\"" + WellKnownText.toWKT(getIndexGeometry()) + "\"";
                }
                addToIndexes(i, Arrays.toString(values), ALL_INDEXES);
            } else {
                final String value = WellKnownText.toWKT(getIndexGeometry());
                addToIndexes(i, "\"" + value + "\"", ALL_INDEXES);
            }
        }

        refresh(ALL_INDEXES);

        String smallRectangleCW = "POLYGON ((-10 -10, -10 10, 10 10, 10 -10, -10 -10))";
        assertFunction("ST_WITHIN", smallRectangleCW);
        String smallRectangleCCW = "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))";
        assertFunction("ST_WITHIN", smallRectangleCCW);
        for (int i = 0; i < 10; i++) {
            final Geometry geometry = getQueryGeometry();
            final String wkt = WellKnownText.toWKT(geometry);
            assertFunction("ST_INTERSECTS", wkt);
            assertFunction("ST_DISJOINT", wkt);
            assertFunction("ST_CONTAINS", wkt);
            // within and lines are not globally supported, so we avoid it here
            if (containsLine(geometry) == false) {
                assertFunction("ST_WITHIN", wkt);
            }
        }
    }

    protected List<String> getQueries(String query) {
        ArrayList<String> queries = new ArrayList<>();
        Arrays.stream(ALL_INDEXES).forEach(index -> queries.add(query.replaceAll("FROM (\\w+)\\s*\\|", "FROM " + index + " |")));
        queries.add(query.replaceAll("FROM (\\w+)\\s*\\|", "FROM " + String.join(",", ALL_INDEXES) + " |"));
        return queries;
    }

    protected void assertFunction(String spatialFunction, String wkt) {
        List<String> queries = getQueries(String.format(Locale.ROOT, """
            FROM index | WHERE %s(location, %s("%s")) | STATS COUNT(*)
            """, spatialFunction, castingFunction(), wkt));
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

    public void testQuantizedXY() {
        initIndexes();
        for (int i = 0; i < random().nextInt(50, 100); i++) {
            final String value = WellKnownText.toWKT(getIndexGeometry());
            addToIndexes(i, "\"" + value + "\"", "indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");
        }

        refresh("indexed", "not-indexed", "not-indexed-nor-doc-values", "no-doc-values");

        assertQuantizedXY();
    }

    protected abstract void assertQuantizedXY();

    protected abstract Point quantizePoint(Point point);

    protected abstract T quantize(T shape);

    protected T quantize(String wkt, Class<T> type) {
        try {
            return quantize(type.cast(WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Geometry parse(String wkt) {
        try {
            return WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Get responses as a list of type T (Point or Geometry)
    protected List<T> getResponsesAsType(TestQueryResponseCollection responses, int index, int column, Class<T> type) {
        return responses.getResponses(index, column).stream().map(o -> type.cast(parse(o.toString()))).toList();
    }

    // Get responses as a list of type T (Point or Geometry) with each value quantized
    protected List<T> getQuantizedResponsesAsType(TestQueryResponseCollection responses, int index, int column, Class<T> type) {
        return responses.getResponses(index, column).stream().map(o -> quantize(o.toString(), type)).toList();
    }

    protected static class TestQueryResponseCollection implements AutoCloseable {
        private final List<? extends EsqlQueryResponse> responses;

        public TestQueryResponseCollection(List<String> queries) {
            this.responses = queries.stream().map(query -> {
                try {
                    return client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).toList();
        }

        protected Object getResponse(int index, int column) {
            return responses.get(index).response().column(column).iterator().next();
        }

        protected List<Object> getResponses(int index, int column) {
            List<Object> results = new ArrayList<>();
            responses.get(index).response().column(column).iterator().forEachRemaining(results::add);
            return results;
        }

        @Override
        public void close() {
            for (EsqlQueryResponse response : responses) {
                response.close();
            }
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
