/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Integration Test for CartesianCentroid metric aggregator
 */
@ESIntegTestCase.SuiteScopeTestCase
public abstract class CentroidAggregationTestBase extends AbstractGeoTestCase {
    protected abstract String aggName();

    protected abstract ValuesSourceAggregationBuilder<?> centroidAgg(String name);

    public void testEmptyAggregation() {
        assertNoFailuresAndResponse(
            client().prepareSearch(EMPTY_IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(centroidAgg(aggName()).field(SINGLE_VALUED_FIELD_NAME)),
            response -> {
                CentroidAggregation geoCentroid = response.getAggregations().get(aggName());
                assertThat(response.getHits().getTotalHits().value(), equalTo(0L));
                assertThat(geoCentroid, notNullValue());
                assertThat(geoCentroid.getName(), equalTo(aggName()));
                assertThat(geoCentroid.centroid(), equalTo(null));
                assertEquals(0, geoCentroid.count());
            }
        );

    }

    public void testUnmapped() throws Exception {
        assertNoFailuresAndResponse(
            client().prepareSearch(UNMAPPED_IDX_NAME).addAggregation(centroidAgg(aggName()).field(SINGLE_VALUED_FIELD_NAME)),
            response -> {
                CentroidAggregation geoCentroid = response.getAggregations().get(aggName());
                assertThat(geoCentroid, notNullValue());
                assertThat(geoCentroid.getName(), equalTo(aggName()));
                assertThat(geoCentroid.centroid(), equalTo(null));
                assertEquals(0, geoCentroid.count());
            }
        );
    }

    public void testPartiallyUnmapped() {
        assertNoFailuresAndResponse(
            client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME).addAggregation(centroidAgg(aggName()).field(SINGLE_VALUED_FIELD_NAME)),
            response -> {
                CentroidAggregation geoCentroid = response.getAggregations().get(aggName());
                assertThat(geoCentroid, notNullValue());
                assertThat(geoCentroid.getName(), equalTo(aggName()));
                assertSameCentroid(geoCentroid.centroid(), singleCentroid);
                assertEquals(numDocs, geoCentroid.count());
            }
        );

    }

    public void testSingleValuedField() {
        assertNoFailuresAndResponse(
            client().prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(centroidAgg(aggName()).field(SINGLE_VALUED_FIELD_NAME)),
            response -> {
                CentroidAggregation geoCentroid = response.getAggregations().get(aggName());
                assertThat(geoCentroid, notNullValue());
                assertThat(geoCentroid.getName(), equalTo(aggName()));
                assertSameCentroid(geoCentroid.centroid(), singleCentroid);
                assertEquals(numDocs, geoCentroid.count());
            }
        );
    }

    public void testSingleValueFieldGetProperty() {
        assertNoFailuresAndResponse(
            client().prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(centroidAgg(aggName()).field(SINGLE_VALUED_FIELD_NAME))),
            response -> {
                Global global = response.getAggregations().get("global");
                assertThat(global, notNullValue());
                assertThat(global.getName(), equalTo("global"));
                assertThat(global.getDocCount(), equalTo((long) numDocs));
                assertThat(global.getAggregations(), notNullValue());
                assertThat(global.getAggregations().asList().size(), equalTo(1));

                CentroidAggregation geoCentroid = global.getAggregations().get(aggName());
                InternalAggregation agg = (InternalAggregation) global;
                assertThat(geoCentroid, notNullValue());
                assertThat(geoCentroid.getName(), equalTo(aggName()));
                assertThat((CentroidAggregation) agg.getProperty(aggName()), sameInstance(geoCentroid));
                assertSameCentroid(geoCentroid.centroid(), singleCentroid);
                assertSimilarValue(((SpatialPoint) agg.getProperty(aggName() + ".value")).getY(), singleCentroid.getY());
                assertSimilarValue(((SpatialPoint) agg.getProperty(aggName() + ".value")).getX(), singleCentroid.getX());
                assertSimilarValue((double) agg.getProperty(aggName() + "." + coordinateName("y")), singleCentroid.getY());
                assertSimilarValue((double) agg.getProperty(aggName() + "." + coordinateName("x")), singleCentroid.getX());
                assertEquals(numDocs, (long) ((InternalAggregation) global).getProperty(aggName() + ".count"));
            }
        );
    }

    public void testMultiValuedField() throws Exception {
        assertNoFailuresAndResponse(
            client().prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(centroidAgg(aggName()).field(MULTI_VALUED_FIELD_NAME)),
            response -> {
                CentroidAggregation geoCentroid = response.getAggregations().get(aggName());
                assertThat(geoCentroid, notNullValue());
                assertThat(geoCentroid.getName(), equalTo(aggName()));
                assertSameCentroid(geoCentroid.centroid(), multiCentroid);
                assertEquals(2 * numDocs, geoCentroid.count());
            }
        );
    }

    /** Override this if the spatial data uses different coordinate names (eg. Geo uses lon/at instead of x/y */
    protected String coordinateName(String coordinate) {
        return coordinate;
    }

    /** Override this if the spatial data needs custom tolerance calculations (eg. cartesian) */
    protected double tolerance(double a, double b) {
        return GEOHASH_TOLERANCE;
    }

    /** Override this if the spatial data needs custom normalization (eg. cartesian) */
    protected double normalize(double value) {
        return value;
    }

    protected void assertSimilarValue(double a, double b) {
        a = normalize(a);
        b = normalize(b);
        assertThat(a, closeTo(b, tolerance(a, b)));
    }

    protected void assertSameCentroid(SpatialPoint centroid, SpatialPoint expectedCentroid) {
        String[] names = centroid.getClass() == GeoPoint.class ? new String[] { "longitude", "latitude" } : new String[] { "x", "y" };
        double x = normalize(centroid.getX());
        double y = normalize(centroid.getY());
        double ex = normalize(expectedCentroid.getX());
        double ey = normalize(expectedCentroid.getY());
        assertThat("Mismatching value for '" + names[0] + "' field of centroid", x, closeTo(ex, tolerance(x, ex)));
        assertThat("Mismatching value for '" + names[1] + "' field of centroid", y, closeTo(ey, tolerance(y, ey)));
    }
}
