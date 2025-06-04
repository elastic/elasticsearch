/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class SpatialGridLicenseTestCase extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() throws Exception {
        assumeTrue("requires SPATIAL_GRID capability", EsqlCapabilities.Cap.SPATIAL_GRID.isEnabled());
        createAndPopulateIndexes(10);
    }

    protected List<Point> testData = new ArrayList<>();

    protected abstract String gridFunction();

    protected abstract Map<Long, Long> expectedValues();

    protected int precision() {
        return 1; // Default precision for grid function tests, can be overridden in subclasses
    }

    /**
     * This test will fail without a platinum license
     */
    public abstract void testGeoGridWithShapes();

    /**
     * This test should pass with and without platinum licenses.
     * This should only be overridden for ST_GEOHEX which also licenses geo_point use cases.
     */
    public void testGeoGridWithPoints() {
        assertGeoGridFromIndex("index_geo_point");
    }

    protected void assertGeoGridFromIndex(String index) {
        assumeTrue("geo_shape capability not yet implemented", index.equals("index_geo_point"));
        var query = String.format(Locale.ROOT, """
            FROM %s
            | EVAL gridId = %s(location, %s)
            | STATS count=COUNT() BY gridId
            | KEEP gridId, count
            """, index, gridFunction(), precision());
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("gridId", "count"));
            assertColumnTypes(resp.columns(), List.of("long", "long"));
            Map<Long, Long> values = getValuesMap(resp.values());
            Map<Long, Long> expected = expectedValues();
            assertThat(values.size(), equalTo(expected.size()));
            for (Long gridId : expected.keySet()) {
                assertThat("Missing grid-id: " + gridId, values.containsKey(gridId), equalTo(true));
                assertThat("Unexpected count for grid-id: " + gridId, values.get(gridId), equalTo(expected.get(gridId)));
            }
        }
    }

    protected void assertGeoGridFailsWith(String index) {
        assumeTrue("geo_shape capability not yet implemented", index.equals("index_geo_point"));
        var query = String.format(Locale.ROOT, """
            FROM %s
            | EVAL gridId = %s(location, %d)
            | STATS count=COUNT() BY gridId
            """, index, gridFunction(), precision());
        var expectedError = String.format(
            Locale.ROOT,
            "current license is non-compliant for [%s(location, %d)]",
            gridFunction(),
            precision()
        );
        ElasticsearchException e = expectThrows(VerificationException.class, () -> run(query));
        assertThat(e.getMessage(), containsString(expectedError));
    }

    public static Map<Long, Long> getValuesMap(Iterator<Iterator<Object>> values) {
        var valuesMap = new LinkedHashMap<Long, Long>();
        values.forEachRemaining(row -> { valuesMap.put(((Number) row.next()).longValue(), ((Number) row.next()).longValue()); });
        return valuesMap;
    }

    private void createAndPopulateIndexes(int count) throws Exception {
        initIndex("index_", "geo_point");
        initIndex("index_", "geo_shape");
        BulkRequestBuilder points = client().prepareBulk();
        BulkRequestBuilder shapes = client().prepareBulk();
        StringBuilder coords = new StringBuilder();
        for (int i = 0; i < count; i++) {
            double x = randomDoubleBetween(-10.0, 10.0, true);
            double y = randomDoubleBetween(-10.0, 10.0, true);
            Point point = new Point(x, y);
            testData.add(point);
            points.add(new IndexRequest("index_geo_point").id(x + ":" + y).source("location", point.toString()));
            if (coords.length() > 0) {
                coords.append(", ");
            }
            coords.append(x).append(" ").append(y);
        }
        if (coords.length() > 0) {
            String lineString = "LINESTRING (" + coords + ")";
            shapes.add(new IndexRequest("index_geo_shape").id("polygon").source("location", lineString));
        }
        points.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        shapes.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("index_geo_point");
        ensureYellow("index_geo_shape");
    }

    protected void initIndex(String prefix, String fieldType) {
        assertAcked(prepareCreate(prefix + fieldType).setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
                "location": { "type" : "%s" }
              }
            }
            """, fieldType)));
    }
}
