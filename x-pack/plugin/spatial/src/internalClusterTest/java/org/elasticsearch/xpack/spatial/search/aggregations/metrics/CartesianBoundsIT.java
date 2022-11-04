/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.AbstractGeoTestCase;
import org.elasticsearch.search.aggregations.metrics.SpatialBounds;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.SuiteScopeTestCase
public class CartesianBoundsIT extends AbstractGeoTestCase {
    private static final String aggName = "cartesianBounds";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME).addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)).get();

        assertSearchResponse(response);

        SpatialBounds<CartesianPoint> geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        CartesianPoint topLeft = geoBounds.topLeft();
        CartesianPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedField_getProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)))
            .get();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        SpatialBounds<CartesianPoint> geobounds = global.getAggregations().get(aggName);
        assertThat(geobounds, notNullValue());
        assertThat(geobounds.getName(), equalTo(aggName));
        assertThat((SpatialBounds<?>) ((InternalAggregation) global).getProperty(aggName), sameInstance(geobounds));
        CartesianPoint topLeft = geobounds.topLeft();
        CartesianPoint bottomRight = geobounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".top"), closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName + ".left"),
            closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE)
        );
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName + ".bottom"),
            closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE)
        );
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName + ".right"),
            closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE)
        );
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME).addAggregation(geoBounds(aggName).field(MULTI_VALUED_FIELD_NAME)).get();

        assertSearchResponse(response);

        SpatialBounds<CartesianPoint> geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        CartesianPoint topLeft = geoBounds.topLeft();
        CartesianPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(multiTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(multiTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(multiBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(multiBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();

        assertSearchResponse(response);

        SpatialBounds<CartesianPoint> geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        CartesianPoint topLeft = geoBounds.topLeft();
        CartesianPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();

        assertSearchResponse(response);

        SpatialBounds<CartesianPoint> geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        CartesianPoint topLeft = geoBounds.topLeft();
        CartesianPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(EMPTY_IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        SpatialBounds<CartesianPoint> geoBounds = searchResponse.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        CartesianPoint topLeft = geoBounds.topLeft();
        CartesianPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    /**
     * This test forces the {@link CartesianBoundsAggregator} to resize the {@link BigArray}s it uses to ensure they are resized correctly
     */
    public void testSingleValuedFieldAsSubAggToHighCardTermsAgg() {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addAggregation(terms("terms").field(NUMBER_FIELD_NAME).subAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat("InternalBucket " + bucket.getKey() + " has wrong number of documents", bucket.getDocCount(), equalTo(1L));
            SpatialBounds<CartesianPoint> geoBounds = bucket.getAggregations().get(aggName);
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(aggName));
            assertThat(geoBounds.topLeft().getY(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.topLeft().getX(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
            assertThat(geoBounds.bottomRight().getY(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.bottomRight().getX(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
        }
    }

    public void testSingleValuedFieldWithZeroLon() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_ZERO_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();

        assertSearchResponse(response);

        SpatialBounds<CartesianPoint> geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        CartesianPoint topLeft = geoBounds.topLeft();
        CartesianPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(0.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(0.0, GEOHASH_TOLERANCE));
    }

    public static CartesianBoundsAggregationBuilder geoBounds(String name) {
        return new CartesianBoundsAggregationBuilder(name);
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
