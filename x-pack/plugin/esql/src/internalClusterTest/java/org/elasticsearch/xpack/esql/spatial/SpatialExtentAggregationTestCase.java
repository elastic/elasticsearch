/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class SpatialExtentAggregationTestCase extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() throws Exception {
        assumeTrue("requires ST_EXTENT_AGG capability", EsqlCapabilities.Cap.ST_EXTENT_AGG.isEnabled());
        createAndPopulateIndexes(-10, 10, -10, 10);
    }

    /**
     * This test should pass only with an enterprise license
     */
    public abstract void testStExtentAggregationWithShapes();

    /**
     * This test should pass with and without enterprise licenses
     */
    public void testStExtentAggregationWithPoints() throws Exception {
        assertStExtentFromIndex("index_geo_point");
    }

    protected void assertStExtentFromIndex(String index) {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | STATS extent = ST_EXTENT_AGG(location)
            | EVAL minX = ROUND(ST_XMIN(extent))
            | EVAL maxX = ROUND(ST_XMAX(extent))
            | EVAL minY = ROUND(ST_YMIN(extent))
            | EVAL maxY = ROUND(ST_YMAX(extent))
            """, index);
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("extent", "minX", "maxX", "minY", "maxY"));
            assertColumnTypes(resp.columns(), List.of("geo_shape", "double", "double", "double", "double"));
            List<List<Object>> values = getValuesList(resp.values());
            assertThat(values.size(), equalTo(1));
            List<Object> row = values.getFirst();
            List<Object> expectedValues = List.of(-10.0, 10.0, -10.0, 10.0);
            assertThat(row.subList(1, row.size()), equalTo(expectedValues));
        }
    }

    protected void assertStExtentFailsWith(String index) {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | STATS extent = ST_EXTENT_AGG(location)
            | EVAL minX = ROUND(ST_XMIN(extent))
            | EVAL maxX = ROUND(ST_XMAX(extent))
            | EVAL minY = ROUND(ST_YMIN(extent))
            | EVAL maxY = ROUND(ST_YMAX(extent))
            """, index);
        ElasticsearchException e = expectThrows(VerificationException.class, () -> run(query));
        assertThat(e.getMessage(), containsString("current license is non-compliant for [ST_EXTENT_AGG(location)]"));
    }

    private void createAndPopulateIndexes(double minX, double maxX, double minY, double maxY) throws Exception {
        int numX = 21;
        int numY = 21;
        initIndex("index_", "geo_point");
        initIndex("index_", "geo_shape");
        BulkRequestBuilder points = client().prepareBulk();
        BulkRequestBuilder shapes = client().prepareBulk();
        for (int xi = 0; xi < numX; xi++) {
            for (int yi = 0; yi < numY; yi++) {
                double x = minX + xi * (maxX - minX) / (numX - 1);
                double y = minY + yi * (maxY - minY) / (numY - 1);
                String point = "POINT(" + x + " " + y + ")";
                points.add(new IndexRequest("index_geo_point").id(x + ":" + y).source("location", point));
                if (xi > 0 && yi > 0) {
                    double px = minX + (xi - 1) * (maxX - minX) / numX;
                    double py = minY + (yi - 1) * (maxY - minY) / numY;
                    String shape = "BBOX(" + px + ", " + x + ", " + y + ", " + py + ")";
                    shapes.add(new IndexRequest("index_geo_shape").id(x + ":" + y).source("location", shape));
                }
            }
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
