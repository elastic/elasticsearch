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
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class SpatialCentroidAggregationTestCase extends AbstractSpatialAggregationTestCase {

    @Before
    public void setupIndex() throws Exception {
        assumeTrue("requires ST_CENTROID_AGG_SHAPES capability", EsqlCapabilities.Cap.ST_CENTROID_AGG_SHAPES.isEnabled());
        createAndPopulateIndexes(-10, 10, -10, 10);
    }

    /**
     * This test should pass only with an enterprise license
     */
    public abstract void testStCentroidAggregationWithShapes();

    /**
     * This test should pass with and without enterprise licenses
     */
    public void testStCentroidAggregationWithPoints() throws Exception {
        // Points are evenly distributed in a grid from -10 to 10 in both dimensions
        // The centroid should be at the center (0, 0)
        assertStCentroidFromIndex("index_geo_point", 1e-7);
    }

    protected void assertStCentroidFromIndex(String index, double tolerance) {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | STATS centroid = ST_CENTROID_AGG(location)
            | EVAL x = ST_X(centroid)
            | EVAL y = ST_Y(centroid)
            """, index);
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("centroid", "x", "y"));
            assertColumnTypes(resp.columns(), List.of("geo_point", "double", "double"));
            List<List<Object>> values = getValuesList(resp.values());
            assertThat(values.size(), equalTo(1));
            List<Object> row = values.getFirst();
            assertThat((Double) row.get(1), closeTo(0.0, tolerance));
            assertThat((Double) row.get(2), closeTo(0.0, tolerance));
        }
    }

    protected void assertStCentroidFailsWith(String index) {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | STATS centroid = ST_CENTROID_AGG(location)
            | EVAL x = ST_X(centroid)
            | EVAL y = ST_Y(centroid)
            """, index);
        ElasticsearchException e = expectThrows(VerificationException.class, () -> run(query));
        assertThat(e.getMessage(), containsString("current license is non-compliant for [ST_CENTROID_AGG(location)]"));
    }
}
