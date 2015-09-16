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
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregator;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoBounds;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 *
 */
@ESIntegTestCase.SuiteScopeTestCase
public class GeoBoundsIT extends AbstractGeoTestCase {

    private static final String aggName = "geoBounds";

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
                .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(singleTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(singleTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(singleBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(singleBottomRight.lon()));
    }

    @Test
    public void testSingleValuedField_getProperty() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch(IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(
                        global("global").subAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false)))
                .execute().actionGet();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        GeoBounds geobounds = global.getAggregations().get(aggName);
        assertThat(geobounds, notNullValue());
        assertThat(geobounds.getName(), equalTo(aggName));
        assertThat((GeoBounds) global.getProperty(aggName), sameInstance(geobounds));
        GeoPoint topLeft = geobounds.topLeft();
        GeoPoint bottomRight = geobounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(singleTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(singleTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(singleBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(singleBottomRight.lon()));
        assertThat((double) global.getProperty(aggName + ".top"), equalTo(singleTopLeft.lat()));
        assertThat((double) global.getProperty(aggName + ".left"), equalTo(singleTopLeft.lon()));
        assertThat((double) global.getProperty(aggName + ".bottom"), equalTo(singleBottomRight.lat()));
        assertThat((double) global.getProperty(aggName + ".right"), equalTo(singleBottomRight.lon()));
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
                .addAggregation(geoBounds(aggName).field(MULTI_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);


        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(multiTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(multiTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(multiBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(multiBottomRight.lon()));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
                .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
                .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(singleTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(singleTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(singleBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(singleBottomRight.lon()));
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(EMPTY_IDX_NAME)
                .setQuery(matchAllQuery())
                .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));
        GeoBounds geoBounds = searchResponse.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    @Test
    public void singleValuedFieldNearDateLine() throws Exception {        
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
                .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);

        GeoPoint geoValuesTopLeft = new GeoPoint(38, -179);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, 178);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(geoValuesTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(geoValuesTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(geoValuesBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(geoValuesBottomRight.lon()));
    }

    @Test
    public void singleValuedFieldNearDateLineWrapLongitude() throws Exception {

        GeoPoint geoValuesTopLeft = new GeoPoint(38, 170);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, -175);
        
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
                .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(true))
                .execute().actionGet();

        assertSearchResponse(response);
        
        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(geoValuesTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(geoValuesTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(geoValuesBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(geoValuesBottomRight.lon()));
    }

    /**
     * This test forces the {@link GeoBoundsAggregator} to resize the {@link BigArray}s it uses to ensure they are resized correctly
     */
    @Test
    public void singleValuedFieldAsSubAggToHighCardTermsAgg() {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
                .addAggregation(terms("terms").field(NUMBER_FIELD_NAME).subAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat("Bucket " + bucket.getKey() + " has wrong number of documents", bucket.getDocCount(), equalTo(1l));
            GeoBounds geoBounds = bucket.getAggregations().get(aggName);
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(aggName));
            assertThat(geoBounds.topLeft().getLat(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.topLeft().getLon(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
            assertThat(geoBounds.bottomRight().getLat(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.bottomRight().getLon(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
        }
    }

    @Test
    public void singleValuedFieldWithZeroLon() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_ZERO_NAME)
                .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false)).execute().actionGet();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(1.0));
        assertThat(topLeft.lon(), equalTo(0.0));
        assertThat(bottomRight.lat(), equalTo(1.0));
        assertThat(bottomRight.lon(), equalTo(0.0));
    }
}
