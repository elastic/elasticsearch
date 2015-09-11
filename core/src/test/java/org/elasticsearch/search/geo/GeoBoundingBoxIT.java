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

package org.elasticsearch.search.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class GeoBoundingBoxIT extends ESIntegTestCase {

    @Test
    public void simpleBoundingBoxTest() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()).execute().actionGet();

        // to NY: 5.286 km
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject()
                .endObject()).execute().actionGet();

        // to NY: 0.4621 km
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.055 km
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .startObject("location").field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.258 km
        client().prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject()
                .endObject()).execute().actionGet();

        // to NY: 2.029 km
        client().prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .startObject("location").field("lat", 40.731033).field("lon", -73.9962255).endObject()
                .endObject()).execute().actionGet();

        // to NY: 8.572 km
        client().prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch() // from NY
                .setQuery(geoBoundingBoxQuery("location").topLeft(40.73, -74.1).bottomRight(40.717, -73.99))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoBoundingBoxQuery("location").topLeft(40.73, -74.1).bottomRight(40.717, -73.99).type("indexed"))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }
    }

    @Test
    public void limitsBoundingBoxTest() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 40).field("lon", -20).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 40).field("lon", -10).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 40).field("lon", 10).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 40).field("lon", 20).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 10).field("lon", -170).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 0).field("lon", -170).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", -10).field("lon", -170).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "8").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 10).field("lon", 170).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "9").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", 0).field("lon", 170).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "10").setSource(jsonBuilder().startObject()
                .startObject("location").field("lat", -10).field("lon", 170).endObject()
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(41, -11).bottomRight(40, 9))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(41, -11).bottomRight(40, 9).type("indexed"))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(41, -9).bottomRight(40, 11))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("3"));
        searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(41, -9).bottomRight(40, 11).type("indexed"))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("3"));

        searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(11, 171).bottomRight(1, -169))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("5"));
        searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(11, 171).bottomRight(1, -169).type("indexed"))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("5"));

        searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(9, 169).bottomRight(-1, -171))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("9"));
        searchResponse = client().prepareSearch()
                .setQuery(geoBoundingBoxQuery("location").topLeft(9, 169).bottomRight(-1, -171).type("indexed"))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("9"));
    }

    @Test
    public void limit2BoundingBoxTest() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("userid", 880)
                .field("title", "Place in Stockholm")
                .startObject("location").field("lat", 59.328355000000002).field("lon", 18.036842).endObject()
                .endObject())
                .setRefresh(true)
                .execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("userid", 534)
                .field("title", "Place in Montreal")
                .startObject("location").field("lat", 45.509526999999999).field("lon", -73.570986000000005).endObject()
                .endObject())
                .setRefresh(true)
                .execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 880)).filter(
                                geoBoundingBoxQuery("location").topLeft(74.579421999999994, 143.5).bottomRight(-66.668903999999998, 113.96875))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 880)).filter(
                                geoBoundingBoxQuery("location").topLeft(74.579421999999994, 143.5).bottomRight(-66.668903999999998, 113.96875).type("indexed"))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 534)).filter(
                                geoBoundingBoxQuery("location").topLeft(74.579421999999994, 143.5).bottomRight(-66.668903999999998, 113.96875))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 534)).filter(
                                geoBoundingBoxQuery("location").topLeft(74.579421999999994, 143.5).bottomRight(-66.668903999999998, 113.96875).type("indexed"))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void completeLonRangeTest() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("userid", 880)
                .field("title", "Place in Stockholm")
                .startObject("location").field("lat", 59.328355000000002).field("lon", 18.036842).endObject()
                .endObject())
                .setRefresh(true)
                .execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("userid", 534)
                .field("title", "Place in Montreal")
                .startObject("location").field("lat", 45.509526999999999).field("lon", -73.570986000000005).endObject()
                .endObject())
                .setRefresh(true)
                .execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(50, -180).bottomRight(-50, 180)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(50, -180).bottomRight(-50, 180).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(90, -180).bottomRight(-90, 180)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(90, -180).bottomRight(-90, 180).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(50, 0).bottomRight(-50, 360)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(50, 0).bottomRight(-50, 360).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(90, 0).bottomRight(-90, 360)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").coerce(true).topLeft(90, 0).bottomRight(-90, 360).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }
}

