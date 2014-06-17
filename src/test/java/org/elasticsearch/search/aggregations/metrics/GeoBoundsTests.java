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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoBounds;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class GeoBoundsTests extends ElasticsearchIntegrationTest {

    private static final String SINGLE_VALUED_FIELD_NAME = "geo_value";
    private static final String MULTI_VALUED_FIELD_NAME = "geo_values";
    private static final String NUMBER_FIELD_NAME = "l_values";

    static int numDocs;
    static int numUniqueGeoPoints;
    static GeoPoint[] singleValues, multiValues;
    static GeoPoint singleTopLeft, singleBottomRight, multiTopLeft, multiBottomRight, unmappedTopLeft, unmappedBottomRight;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx")
                .addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=geo_point", MULTI_VALUED_FIELD_NAME, "type=geo_point", NUMBER_FIELD_NAME, "type=long", "tag", "type=string,index=not_analyzed"));
        createIndex("idx_unmapped");
        
        unmappedTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        unmappedBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        singleTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        singleBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        multiTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        multiBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        
        numDocs = randomIntBetween(6, 20);
        numUniqueGeoPoints = randomIntBetween(1, numDocs);

        singleValues = new GeoPoint[numUniqueGeoPoints];
        for (int i = 0 ; i < singleValues.length; i++)
        {
            singleValues[i] = randomGeoPoint();
            updateBoundsTopLeft(singleValues[i], singleTopLeft);
            updateBoundsBottomRight(singleValues[i], singleBottomRight);
        }
        
        multiValues = new GeoPoint[numUniqueGeoPoints];
        for (int i = 0 ; i < multiValues.length; i++)
        {
            multiValues[i] = randomGeoPoint();
            updateBoundsTopLeft(multiValues[i], multiTopLeft);
            updateBoundsBottomRight(multiValues[i], multiBottomRight);
        }
        
        List<IndexRequestBuilder> builders = new ArrayList<>();


        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .array(SINGLE_VALUED_FIELD_NAME, singleValues[i % numUniqueGeoPoints].lon(), singleValues[i % numUniqueGeoPoints].lat())
                    .startArray(MULTI_VALUED_FIELD_NAME)
                        .startArray().value(multiValues[i % numUniqueGeoPoints].lon()).value(multiValues[i % numUniqueGeoPoints].lat()).endArray()   
                        .startArray().value(multiValues[(i+1) % numUniqueGeoPoints].lon()).value(multiValues[(i+1) % numUniqueGeoPoints].lat()).endArray()
                     .endArray()
                    .field(NUMBER_FIELD_NAME, i)
                    .field("tag", "tag" + i)
                    .endObject()));
        }

        assertAcked(prepareCreate("empty_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=geo_point"));
        
        assertAcked(prepareCreate("idx_dateline")
                .addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=geo_point", MULTI_VALUED_FIELD_NAME, "type=geo_point", NUMBER_FIELD_NAME, "type=long", "tag", "type=string,index=not_analyzed"));

        GeoPoint[] geoValues = new GeoPoint[5];
        geoValues[0] = new GeoPoint(38, 178);
        geoValues[1] = new GeoPoint(12, -179);
        geoValues[2] = new GeoPoint(-24, 170);
        geoValues[3] = new GeoPoint(32, -175);
        geoValues[4] = new GeoPoint(-11, 178);
        
        for (int i = 0; i < 5; i++) {
            builders.add(client().prepareIndex("idx_dateline", "type").setSource(jsonBuilder()
                    .startObject()
                    .array(SINGLE_VALUED_FIELD_NAME, geoValues[i].lon(), geoValues[i].lat())
                    .field(NUMBER_FIELD_NAME, i)
                    .field("tag", "tag" + i)
                    .endObject()));
        }
        
        indexRandom(true, builders);
        ensureSearchable();
    }

    private void updateBoundsBottomRight(GeoPoint geoPoint, GeoPoint currentBound) {
        if (geoPoint.lat() < currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() > currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }

    private void updateBoundsTopLeft(GeoPoint geoPoint, GeoPoint currentBound) {
        if (geoPoint.lat() > currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() < currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }

    private GeoPoint randomGeoPoint() {
        return new GeoPoint((randomDouble() * 180) - 90, (randomDouble() * 360) - 180);
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(geoBounds("geoBounds").field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);


        GeoBounds geoBounds = response.getAggregations().get("geoBounds");
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo("geoBounds"));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(singleTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(singleTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(singleBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(singleBottomRight.lon()));
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(geoBounds("geoBounds").field(MULTI_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);


        GeoBounds geoBounds = response.getAggregations().get("geoBounds");
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo("geoBounds"));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(multiTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(multiTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(multiBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(multiBottomRight.lon()));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(geoBounds("geoBounds").field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);


        GeoBounds geoBounds = response.getAggregations().get("geoBounds");
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo("geoBounds"));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(geoBounds("geoBounds").field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);


        GeoBounds geoBounds = response.getAggregations().get("geoBounds");
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo("geoBounds"));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(singleTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(singleTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(singleBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(singleBottomRight.lon()));
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_idx")
                .setQuery(matchAllQuery())
                .addAggregation(geoBounds("geoBounds").field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));
        GeoBounds geoBounds = searchResponse.getAggregations().get("geoBounds");
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo("geoBounds"));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    @Test
    public void singleValuedFieldNearDateLine() throws Exception {        
        SearchResponse response = client().prepareSearch("idx_dateline")
                .addAggregation(geoBounds("geoBounds").field(SINGLE_VALUED_FIELD_NAME)
                        .wrapLongitude(false))
                .execute().actionGet();

        assertSearchResponse(response);

        GeoPoint geoValuesTopLeft = new GeoPoint(38, -179);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, 178);

        GeoBounds geoBounds = response.getAggregations().get("geoBounds");
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo("geoBounds"));
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
        
        SearchResponse response = client().prepareSearch("idx_dateline")
                .addAggregation(geoBounds("geoBounds").field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(true))
                .execute().actionGet();

        assertSearchResponse(response);
        
        GeoBounds geoBounds = response.getAggregations().get("geoBounds");
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo("geoBounds"));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), equalTo(geoValuesTopLeft.lat()));
        assertThat(topLeft.lon(), equalTo(geoValuesTopLeft.lon()));
        assertThat(bottomRight.lat(), equalTo(geoValuesBottomRight.lat()));
        assertThat(bottomRight.lon(), equalTo(geoValuesBottomRight.lon()));
    }

}
