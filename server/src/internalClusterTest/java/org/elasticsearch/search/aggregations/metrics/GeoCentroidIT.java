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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoCentroid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geohashGrid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Integration Test for GeoCentroid metric aggregator
 */
@ESIntegTestCase.SuiteScopeTestCase
public class GeoCentroidIT extends AbstractGeoTestCase {
    private static final String aggName = "geoCentroid";

    public void testEmptyAggregation() throws Exception {
        SearchResponse response = client().prepareSearch(EMPTY_IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
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

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
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

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
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

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
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

        GeoCentroid geoCentroid = global.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertThat((GeoCentroid) ((InternalAggregation) global).getProperty(aggName), sameInstance(geoCentroid));
        assertSameCentroid(geoCentroid.centroid(), singleCentroid);
        assertThat(
            ((GeoPoint) ((InternalAggregation) global).getProperty(aggName + ".value")).lat(),
            closeTo(singleCentroid.lat(), GEOHASH_TOLERANCE)
        );
        assertThat(
            ((GeoPoint) ((InternalAggregation) global).getProperty(aggName + ".value")).lon(),
            closeTo(singleCentroid.lon(), GEOHASH_TOLERANCE)
        );
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".lat"), closeTo(singleCentroid.lat(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".lon"), closeTo(singleCentroid.lon(), GEOHASH_TOLERANCE));
        assertEquals(numDocs, (long) ((InternalAggregation) global).getProperty(aggName + ".count"));
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(MULTI_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(searchResponse);

        GeoCentroid geoCentroid = searchResponse.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertSameCentroid(geoCentroid.centroid(), multiCentroid);
        assertEquals(2 * numDocs, geoCentroid.count());
    }

    public void testSingleValueFieldAsSubAggToGeohashGrid() {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addAggregation(
                geohashGrid("geoGrid").field(SINGLE_VALUED_FIELD_NAME).subAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();
        assertSearchResponse(response);

        GeoGrid grid = response.getAggregations().get("geoGrid");
        assertThat(grid, notNullValue());
        assertThat(grid.getName(), equalTo("geoGrid"));
        List<? extends GeoGrid.Bucket> buckets = grid.getBuckets();
        for (GeoGrid.Bucket cell : buckets) {
            String geohash = cell.getKeyAsString();
            GeoPoint expectedCentroid = expectedCentroidsForGeoHash.get(geohash);
            GeoCentroid centroidAgg = cell.getAggregations().get(aggName);
            assertSameCentroid(centroidAgg.centroid(), expectedCentroid);
        }
    }
}
