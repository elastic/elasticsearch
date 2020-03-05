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

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoBounds;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.SuiteScopeTestCase
public class GeoBoundsIT extends AbstractGeoTestCase {
    private static final String geoPointAggName = "geoPointBounds";
    private static final String geoShapeAggName = "geoShapeBounds";

    public void test7xIndexOnly() {
        SearchPhaseExecutionException exception = expectThrows(SearchPhaseExecutionException.class,
            () -> client().prepareSearch(IDX_NAME_7x) .addAggregation(geoBounds(geoShapeAggName).field(SINGLE_VALUED_GEOSHAPE_FIELD_NAME))
            .get());
        assertNotNull(exception.getRootCause());
        assertThat(exception.getRootCause().getMessage(),
            equalTo("Can't load fielddata on [geoshape_value] because fielddata is unsupported on fields of type [geo_shape]." +
                " Use doc values instead."));
    }

    public void test7xIndexWith8Index() {
        SearchResponse response = client().prepareSearch(IDX_NAME_7x, IDX_NAME)
            .addAggregation(geoBounds(geoShapeAggName).field(SINGLE_VALUED_GEOSHAPE_FIELD_NAME).wrapLongitude(false)).get();
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.getSuccessfulShards(), lessThan(response.getTotalShards()));
        GeoBounds geoBounds = response.getAggregations().get(geoShapeAggName);
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
                .addAggregation(geoBounds(geoPointAggName).field(SINGLE_VALUED_GEOPOINT_FIELD_NAME)
                        .wrapLongitude(false))
                .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(geoPointAggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(geoPointAggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedField_getProperty() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(
                        global("global").subAggregation(geoBounds(geoPointAggName)
                            .field(SINGLE_VALUED_GEOPOINT_FIELD_NAME).wrapLongitude(false)))
                .get();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        GeoBounds geobounds = global.getAggregations().get(geoPointAggName);
        assertThat(geobounds, notNullValue());
        assertThat(geobounds.getName(), equalTo(geoPointAggName));
        assertThat((GeoBounds) ((InternalAggregation)global).getProperty(geoPointAggName), sameInstance(geobounds));
        GeoPoint topLeft = geobounds.topLeft();
        GeoPoint bottomRight = geobounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation)global).getProperty(geoPointAggName + ".top"),
            closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation)global).getProperty(geoPointAggName + ".left"),
            closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation)global).getProperty(geoPointAggName + ".bottom"),
                closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation)global).getProperty(geoPointAggName + ".right"),
                closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
                .addAggregation(geoBounds(geoPointAggName).field(MULTI_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .get();

        assertSearchResponse(response);


        GeoBounds geoBounds = response.getAggregations().get(geoPointAggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(geoPointAggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(multiTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(multiTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(multiBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(multiBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
                .addAggregation(geoBounds(geoPointAggName).field(SINGLE_VALUED_GEOPOINT_FIELD_NAME)
                        .wrapLongitude(false))
                .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(geoPointAggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(geoPointAggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
                .addAggregation(geoBounds(geoPointAggName).field(SINGLE_VALUED_GEOPOINT_FIELD_NAME)
                        .wrapLongitude(false))
                .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(geoPointAggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(geoPointAggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(EMPTY_IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(geoBounds(geoPointAggName).field(SINGLE_VALUED_GEOPOINT_FIELD_NAME)
                        .wrapLongitude(false))
                .addAggregation(geoBounds(geoShapeAggName).field(SINGLE_VALUED_GEOSHAPE_FIELD_NAME)
                        .wrapLongitude(false))
                .get();

        for (String aggName : List.of(geoPointAggName, geoShapeAggName)) {
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
            GeoBounds geoBounds = searchResponse.getAggregations().get(aggName);
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(aggName));
            GeoPoint topLeft = geoBounds.topLeft();
            GeoPoint bottomRight = geoBounds.bottomRight();
            assertThat(topLeft, equalTo(null));
            assertThat(bottomRight, equalTo(null));
        }
    }

    public void testSingleValuedFieldNearDateLine() throws Exception {
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
                .addAggregation(geoBounds(geoPointAggName).field(SINGLE_VALUED_GEOPOINT_FIELD_NAME)
                        .wrapLongitude(false))
                .get();

        assertSearchResponse(response);

        GeoPoint geoValuesTopLeft = new GeoPoint(38, -179);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, 178);

        GeoBounds geoBounds = response.getAggregations().get(geoPointAggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(geoPointAggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(geoValuesTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(geoValuesTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(geoValuesBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(geoValuesBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedFieldNearDateLineWrapLongitude() throws Exception {
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
                .addAggregation(geoBounds(geoPointAggName).field(SINGLE_VALUED_GEOPOINT_FIELD_NAME).wrapLongitude(true))
                .addAggregation(geoBounds(geoShapeAggName).field(SINGLE_VALUED_GEOSHAPE_FIELD_NAME).wrapLongitude(true))
                .get();

        assertSearchResponse(response);

        // test geo_point
        {
            GeoPoint geoValuesTopLeft = new GeoPoint(38, 170);
            GeoPoint geoValuesBottomRight = new GeoPoint(-24, -175);
            GeoBounds geoBounds = response.getAggregations().get(geoPointAggName);
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(geoPointAggName));
            GeoPoint topLeft = geoBounds.topLeft();
            GeoPoint bottomRight = geoBounds.bottomRight();
            assertThat(topLeft.lat(), closeTo(geoValuesTopLeft.lat(), GEOHASH_TOLERANCE));
            assertThat(topLeft.lon(), closeTo(geoValuesTopLeft.lon(), GEOHASH_TOLERANCE));
            assertThat(bottomRight.lat(), closeTo(geoValuesBottomRight.lat(), GEOHASH_TOLERANCE));
            assertThat(bottomRight.lon(), closeTo(geoValuesBottomRight.lon(), GEOHASH_TOLERANCE));
        }

        // test geo_shape, should not wrap dateline
        {
            GeoPoint geoValuesTopLeft = new GeoPoint(38, -179);
            GeoPoint geoValuesBottomRight = new GeoPoint(-24, 178);
            GeoBounds geoBounds = response.getAggregations().get(geoShapeAggName);
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(geoShapeAggName));
            GeoPoint topLeft = geoBounds.topLeft();
            GeoPoint bottomRight = geoBounds.bottomRight();
            assertThat(topLeft.lat(), closeTo(geoValuesTopLeft.lat(), GEOHASH_TOLERANCE));
            assertThat(topLeft.lon(), closeTo(geoValuesTopLeft.lon(), GEOHASH_TOLERANCE));
            assertThat(bottomRight.lat(), closeTo(geoValuesBottomRight.lat(), GEOHASH_TOLERANCE));
            assertThat(bottomRight.lon(), closeTo(geoValuesBottomRight.lon(), GEOHASH_TOLERANCE));
        }
    }

    /**
     * This test forces the {@link GeoBoundsAggregator} to resize the {@link BigArray}s it uses to ensure they are resized correctly
     */
    public void testSingleValuedFieldAsSubAggToHighCardTermsAgg() {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
                .addAggregation(terms("terms").field(NUMBER_FIELD_NAME).subAggregation(geoBounds(geoPointAggName)
                    .field(SINGLE_VALUED_GEOPOINT_FIELD_NAME).wrapLongitude(false)))
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
            GeoBounds geoBounds = bucket.getAggregations().get(geoPointAggName);
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(geoPointAggName));
            assertThat(geoBounds.topLeft().getLat(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.topLeft().getLon(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
            assertThat(geoBounds.bottomRight().getLat(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.bottomRight().getLon(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
        }
    }

    public void testSingleValuedFieldWithZeroLon() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_ZERO_NAME)
                .addAggregation(geoBounds(geoPointAggName).field(SINGLE_VALUED_GEOPOINT_FIELD_NAME).wrapLongitude(false)).get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(geoPointAggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(geoPointAggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(0.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(0.0, GEOHASH_TOLERANCE));
    }

    public void testIncorrectFieldType() {
        SearchRequestBuilder searchWithKeywordField = client().prepareSearch(DATELINE_IDX_NAME)
            .addAggregation(geoBounds("agg").field("tag"));
        assertFailures(searchWithKeywordField, RestStatus.BAD_REQUEST,
            containsString("Expected geo_point or geo_shape type on field [tag], but got [keyword]"));

        {
            SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
                .addAggregation(geoBounds("agg").missing(randomBoolean() ? "0,0" : "POINT (0 0)").field("non_existent")).get();
            assertSearchResponse(response);
            GeoBounds geoBounds = response.getAggregations().get("agg");
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo("agg"));
            GeoPoint topLeft = geoBounds.topLeft();
            GeoPoint bottomRight = geoBounds.bottomRight();
            assertThat(topLeft.lat(), closeTo(0.0, GEOHASH_TOLERANCE));
            assertThat(topLeft.lon(), closeTo(0.0, GEOHASH_TOLERANCE));
            assertThat(bottomRight.lat(), closeTo(0.0, GEOHASH_TOLERANCE));
            assertThat(bottomRight.lon(), closeTo(0.0, GEOHASH_TOLERANCE));
        }

        {
            SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
                .addAggregation(geoBounds("agg").missing("LINESTRING (30 10, 10 30, 40 40)").field("non_existent")).get();
            assertSearchResponse(response);
            GeoBounds geoBounds = response.getAggregations().get("agg");
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo("agg"));
            GeoPoint topLeft = geoBounds.topLeft();
            GeoPoint bottomRight = geoBounds.bottomRight();
            assertThat(topLeft.lat(), closeTo(40, GEOHASH_TOLERANCE));
            assertThat(topLeft.lon(), closeTo(10, GEOHASH_TOLERANCE));
            assertThat(bottomRight.lat(), closeTo(10, GEOHASH_TOLERANCE));
            assertThat(bottomRight.lon(), closeTo(40, GEOHASH_TOLERANCE));
        }
    }
}
