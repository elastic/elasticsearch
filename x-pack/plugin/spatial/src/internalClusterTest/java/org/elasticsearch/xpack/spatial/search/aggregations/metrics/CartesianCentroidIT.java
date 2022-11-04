/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.metrics.AbstractGeoTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Integration Test for CartesianCentroid metric aggregator
 */
@ESIntegTestCase.SuiteScopeTestCase
public class CartesianCentroidIT extends AbstractGeoTestCase {
    private static final String aggName = "cartesianCentroid";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse response = client().prepareSearch(EMPTY_IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        CartesianCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(response.getHits().getTotalHits().value, equalTo(0L));
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertThat(geoCentroid.centroid(), equalTo(null));
        assertEquals(0, geoCentroid.count());
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        CartesianCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertThat(geoCentroid.centroid(), equalTo(null));
        assertEquals(0, geoCentroid.count());
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        CartesianCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertSameCentroid(geoCentroid.centroid(), singleCentroid);
        assertEquals(numDocs, geoCentroid.count());
    }

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        CartesianCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertSameCentroid(geoCentroid.centroid(), singleCentroid);
        assertEquals(numDocs, geoCentroid.count());
    }

    public void testSingleValueFieldGetProperty() {
        SearchResponse response = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME)))
            .get();
        assertSearchResponse(response);

        Global global = response.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        CartesianCentroid geoCentroid = global.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertThat((CartesianCentroid) ((InternalAggregation) global).getProperty(aggName), sameInstance(geoCentroid));
        assertSameCentroid(geoCentroid.centroid(), singleCentroid);
        assertThat(
            ((CartesianPoint) ((InternalAggregation) global).getProperty(aggName + ".value")).getY(),
            closeTo(singleCentroid.getY(), GEOHASH_TOLERANCE)
        );
        assertThat(
            ((CartesianPoint) ((InternalAggregation) global).getProperty(aggName + ".value")).getX(),
            closeTo(singleCentroid.getX(), GEOHASH_TOLERANCE)
        );
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".y"), closeTo(singleCentroid.getY(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".x"), closeTo(singleCentroid.getX(), GEOHASH_TOLERANCE));
        assertEquals(numDocs, (long) ((InternalAggregation) global).getProperty(aggName + ".count"));
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(MULTI_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(searchResponse);

        CartesianCentroid geoCentroid = searchResponse.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertSameCentroid(geoCentroid.centroid(), multiCentroid);
        assertEquals(2 * numDocs, geoCentroid.count());
    }

    public static CartesianCentroidAggregationBuilder geoCentroid(String name) {
        return new CartesianCentroidAggregationBuilder(name);
    }

    @Override
    protected String fieldTypeName() {
        return "point";
    }

    @Override
    protected CartesianPoint makePoint(double x, double y) {
        return new CartesianPoint(x, y);
    }

    @Override
    protected CartesianPoint randomPoint() {
        Point point = ShapeTestUtils.randomPointNotExtreme(false);
        return new CartesianPoint(point.getX(), point.getY());
    }

    @Override
    protected void resetX(SpatialPoint point, double x) {
        ((CartesianPoint) point).resetX(x);
    }

    @Override
    protected void resetY(SpatialPoint point, double y) {
        ((CartesianPoint) point).resetY(y);
    }

    @Override
    protected CartesianPoint reset(SpatialPoint point, double x, double y) {
        return ((CartesianPoint) point).reset(x, y);
    }
}
