/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroid;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoCentroid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geohashGrid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;

/**
 * Integration Test for GeoCentroid metric aggregator
 */
@ESIntegTestCase.SuiteScopeTestCase
public class GeoCentroidIT extends AbstractGeoTestCase {
    private static final String aggName = "geoCentroid";

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse response = client().prepareSearch(EMPTY_IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
                .execute().actionGet();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(response.getHits().getTotalHits(), equalTo(0l));
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(null));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
                .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
                .execute().actionGet();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(null));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
                .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
                .execute().actionGet();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(singleCentroid));
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
                .execute().actionGet();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(singleCentroid));
    }

    @Test
    public void singleValueField_getProperty() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME)))
                .execute().actionGet();
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
        assertThat((GeoCentroid) global.getProperty(aggName), sameInstance(geoCentroid));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(singleCentroid));
        assertThat((GeoPoint) global.getProperty(aggName + ".value"), equalTo(singleCentroid));
        assertThat((double) global.getProperty(aggName + ".lat"), closeTo(singleCentroid.lat(), 1e-5));
        assertThat((double) global.getProperty(aggName + ".lon"), closeTo(singleCentroid.lon(), 1e-5));
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(geoCentroid(aggName).field(MULTI_VALUED_FIELD_NAME))
                .execute().actionGet();
        assertSearchResponse(searchResponse);

        GeoCentroid geoCentroid = searchResponse.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(multiCentroid));
    }

    @Test
    public void singleValueFieldAsSubAggToGeohashGrid() throws Exception {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
                .addAggregation(geohashGrid("geoGrid").field(SINGLE_VALUED_FIELD_NAME)
                .subAggregation(geoCentroid(aggName)))
                .execute().actionGet();
        assertSearchResponse(response);

        GeoHashGrid grid = response.getAggregations().get("geoGrid");
        assertThat(grid, notNullValue());
        assertThat(grid.getName(), equalTo("geoGrid"));
        List<GeoHashGrid.Bucket> buckets = grid.getBuckets();
        for (int i=0; i < buckets.size(); ++i) {
            GeoHashGrid.Bucket cell = buckets.get(i);
            String geohash = cell.getKeyAsString();
            GeoPoint expectedCentroid = expectedCentroidsForGeoHash.get(geohash);
            GeoCentroid centroidAgg = cell.getAggregations().get(aggName);
            assertEquals("Geohash " + geohash + " has wrong centroid ", expectedCentroid, centroidAgg.centroid());
        }
    }
}
