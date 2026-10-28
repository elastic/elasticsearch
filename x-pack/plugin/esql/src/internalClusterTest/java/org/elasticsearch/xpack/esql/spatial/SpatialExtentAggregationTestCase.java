/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class SpatialExtentAggregationTestCase extends AbstractSpatialAggregationTestCase {

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
}
