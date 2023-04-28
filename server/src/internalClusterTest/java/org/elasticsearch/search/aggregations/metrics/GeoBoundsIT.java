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
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class GeoBoundsIT extends SpatialBoundsAggregationTestBase<GeoPoint> {

    public void testSingleValuedFieldNearDateLine() {
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
            .addAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertSearchResponse(response);

        GeoPoint geoValuesTopLeft = new GeoPoint(38, -179);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, 178);

        GeoBounds geoBounds = response.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(geoValuesTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(geoValuesTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(geoValuesBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(geoValuesBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedFieldNearDateLineWrapLongitude() {

        GeoPoint geoValuesTopLeft = new GeoPoint(38, 170);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, -175);
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
            .addAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME).wrapLongitude(true))
            .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(geoValuesTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(geoValuesTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(geoValuesBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(geoValuesBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    @Override
    protected String aggName() {
        return "geoBounds";
    }

    @Override
    public GeoBoundsAggregationBuilder boundsAgg(String aggName, String fieldName) {
        return new GeoBoundsAggregationBuilder(aggName).field(fieldName).wrapLongitude(false);
    }

    @Override
    protected void assertBoundsLimits(SpatialBounds<GeoPoint> geoBounds) {
        assertThat(geoBounds.topLeft().getY(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
        assertThat(geoBounds.topLeft().getX(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
        assertThat(geoBounds.bottomRight().getY(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
        assertThat(geoBounds.bottomRight().getX(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
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
