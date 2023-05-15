/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.geohashGrid;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration Test for GeoCentroid metric aggregator
 */
@ESIntegTestCase.SuiteScopeTestCase
public class GeoCentroidIT extends CentroidAggregationTestBase {

    public void testSingleValueFieldAsSubAggToGeohashGrid() {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addAggregation(
                geohashGrid("geoGrid").field(SINGLE_VALUED_FIELD_NAME)
                    .subAggregation(centroidAgg(aggName()).field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();
        assertSearchResponse(response);

        GeoGrid grid = response.getAggregations().get("geoGrid");
        assertThat(grid, notNullValue());
        assertThat(grid.getName(), equalTo("geoGrid"));
        List<? extends GeoGrid.Bucket> buckets = grid.getBuckets();
        for (GeoGrid.Bucket cell : buckets) {
            String geohash = cell.getKeyAsString();
            SpatialPoint expectedCentroid = expectedCentroidsForGeoHash.get(geohash);
            GeoCentroid centroidAgg = cell.getAggregations().get(aggName());
            assertSameCentroid(centroidAgg.centroid(), expectedCentroid);
        }
    }

    @Override
    protected String aggName() {
        return "geoCentroid";
    }

    @Override
    public GeoCentroidAggregationBuilder centroidAgg(String name) {
        return new GeoCentroidAggregationBuilder(name);
    }

    /** Geo has different coordinate names than cartesian */
    @Override
    protected String coordinateName(String coordinate) {
        return switch (coordinate) {
            case "x" -> "lon";
            case "y" -> "lat";
            default -> throw new IllegalArgumentException("Unknown coordinate: " + coordinate);
        };
    }

    @Override
    protected String fieldTypeName() {
        return "geo_point";
    }

    @Override
    protected GeoPoint makePoint(double x, double y) {
        return new GeoPoint(y, x);
    }

    @Override
    protected GeoPoint randomPoint() {
        return RandomGeoGenerator.randomPoint(random());
    }

    @Override
    protected void resetX(SpatialPoint point, double x) {
        ((GeoPoint) point).resetLon(x);
    }

    @Override
    protected void resetY(SpatialPoint point, double y) {
        ((GeoPoint) point).resetLat(y);
    }

    @Override
    protected GeoPoint reset(SpatialPoint point, double x, double y) {
        return ((GeoPoint) point).reset(y, x);
    }
}
