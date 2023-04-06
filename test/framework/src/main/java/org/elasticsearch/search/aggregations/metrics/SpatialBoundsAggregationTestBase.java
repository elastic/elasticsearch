/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.SuiteScopeTestCase
public abstract class SpatialBoundsAggregationTestBase<T extends SpatialPoint> extends AbstractGeoTestCase {

    protected abstract String aggName();

    protected abstract ValuesSourceAggregationBuilder<?> boundsAgg(String aggName, String fieldName);

    protected abstract void assertBoundsLimits(SpatialBounds<T> spatialBounds);

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME).addAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME)).get();

        assertSearchResponse(response);

        SpatialBounds<T> geoBounds = response.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        T topLeft = geoBounds.topLeft();
        T bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedField_getProperty() {
        SearchResponse searchResponse = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME)))
            .get();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        SpatialBounds<T> geobounds = global.getAggregations().get(aggName());
        assertThat(geobounds, notNullValue());
        assertThat(geobounds.getName(), equalTo(aggName()));
        assertThat((SpatialBounds<?>) ((InternalAggregation) global).getProperty(aggName()), sameInstance(geobounds));
        T topLeft = geobounds.topLeft();
        T bottomRight = geobounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE));
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName() + ".top"),
            closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE)
        );
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName() + ".left"),
            closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE)
        );
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName() + ".bottom"),
            closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE)
        );
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName() + ".right"),
            closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE)
        );
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME).addAggregation(boundsAgg(aggName(), MULTI_VALUED_FIELD_NAME)).get();

        assertSearchResponse(response);

        SpatialBounds<T> geoBounds = response.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        T topLeft = geoBounds.topLeft();
        T bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(multiTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(multiTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(multiBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(multiBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
            .addAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME))
            .get();

        assertSearchResponse(response);

        SpatialBounds<T> geoBounds = response.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        T topLeft = geoBounds.topLeft();
        T bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
            .addAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME))
            .get();

        assertSearchResponse(response);

        SpatialBounds<T> geoBounds = response.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        T topLeft = geoBounds.topLeft();
        T bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(singleTopLeft.getY(), GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(singleTopLeft.getX(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(singleBottomRight.getY(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(singleBottomRight.getX(), GEOHASH_TOLERANCE));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(EMPTY_IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        SpatialBounds<T> geoBounds = searchResponse.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        T topLeft = geoBounds.topLeft();
        T bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    /**
     * This test forces the bounds {@link MetricsAggregator} to resize the {@link BigArray}s it uses to ensure they are resized correctly
     */
    public void testSingleValuedFieldAsSubAggToHighCardTermsAgg() {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addAggregation(terms("terms").field(NUMBER_FIELD_NAME).subAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME)))
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
            SpatialBounds<T> geoBounds = bucket.getAggregations().get(aggName());
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(aggName()));
            assertBoundsLimits(geoBounds);
        }
    }

    public void testSingleValuedFieldWithZeroLon() {
        SearchResponse response = client().prepareSearch(IDX_ZERO_NAME)
            .addAggregation(boundsAgg(aggName(), SINGLE_VALUED_FIELD_NAME))
            .get();

        assertSearchResponse(response);

        SpatialBounds<T> geoBounds = response.getAggregations().get(aggName());
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName()));
        T topLeft = geoBounds.topLeft();
        T bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.getY(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(topLeft.getX(), closeTo(0.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.getY(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.getX(), closeTo(0.0, GEOHASH_TOLERANCE));
    }
}
