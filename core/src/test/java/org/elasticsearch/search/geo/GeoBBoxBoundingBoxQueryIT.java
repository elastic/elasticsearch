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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test GeoBoundingBoxQuery with geo_bounding_box types
 */
public class GeoBBoxBoundingBoxQueryIT extends BaseGeoBoundingBoxQueryTestCase {

    @Override
    public void testSimpleBoundingBoxQuery() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location").field("type", "geo_bounding_box");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        // Not crossing
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
            .field("name", "BBox 1")
            .startObject("location").field("top_left", new GeoPoint(41.7143528, -74.0059731))
            .field("bottom_right", new GeoPoint(40.7143528 , -73.0059731)).endObject()
            .endObject()).execute().actionGet();

        // XDL
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
            .field("name", "BBox 2 - xdl")
            .startObject("location").field("top_left", new GeoPoint(40.0, 179.0))
            .field("bottom_right", new GeoPoint(39.0 , -179.0)).endObject()
            .endObject()).execute().actionGet();

        // Full lat range
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
            .field("name", "BBox 3 - Full Range")
            .startObject("location").field("top_left", new GeoPoint(10.0, -180.0))
            .field("bottom_right", new GeoPoint(-10.0 , 180.0)).endObject()
            .endObject()).execute().actionGet();

        // Full Map
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
            .field("name", "BBox 3 - Full Range")
            .startObject("location").field("top_left", new GeoPoint(90.0, -180.0))
            .field("bottom_right", new GeoPoint(-90.0 , 180.0)).endObject()
            .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        // INTERSECTS TEST
        SearchResponse searchResponse = client().prepareSearch() // from NY
            .setQuery(geoBoundingBoxQuery("location").setCorners(40.73, -74.1, 40.717, -73.99))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("4")));
        }

        // CONTAINS TEST
        searchResponse = client().prepareSearch() // from NY
            .setQuery(geoBoundingBoxQuery("location").setCorners(41.70, -73.9, 40.72, -73.1)
                .relation("CONTAINS"))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("4")));
        }

        // WITHIN TEST
        searchResponse = client().prepareSearch() // from NY
            .setQuery(geoBoundingBoxQuery("location").setCorners(42.70, -75.9, 39.72, -71.1)
                .relation("WITHIN"))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1")));
        }

        // DISJOINT TEST
        searchResponse = client().prepareSearch() // from NY
            .setQuery(geoBoundingBoxQuery("location").setCorners(32.70, -65.9, 32.62, -64.1)
                .relation("DISJOINT"))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("2"), equalTo("3")));
        }

        // CROSSES (Not yet supported)

        // --- CROSSING DATELINE ---
        // INTERSECTS TESTS
        searchResponse = client().prepareSearch() // test western bbox (test indexed box crossing dateline)
            .setQuery(geoBoundingBoxQuery("location").setCorners(39.9, -178, 39.1, -179.5))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("2"), equalTo("4")));
        }

        searchResponse = client().prepareSearch() // test eastern bbox
            .setQuery(geoBoundingBoxQuery("location").setCorners(39.9, 178, 39.1, 180))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("2"), equalTo("4")));
        }

        // ... crosses dateline
        searchResponse = client().prepareSearch() // test xdl query
            .setQuery(geoBoundingBoxQuery("location").setCorners(39.9, 179.5, 39.1, -179.5))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("2"), equalTo("4")));
        }

        // CONTAINS TEST
        // TODO in work
//        searchResponse = client().prepareSearch() // xdl
//            .setQuery(geoBoundingBoxQuery("location").setCorners(39.9, 179.5, 39.1, -179.5)
//                .relation("CONTAINS"))
//            .execute().actionGet();
//        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
//        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
//        for (SearchHit hit : searchResponse.getHits()) {
//            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("2"), equalTo("3")));
//        }

        // WITHIN TESTS
        searchResponse = client().prepareSearch() // xdl
            .setQuery(geoBoundingBoxQuery("location").setCorners(41.0, 178.0, 38.0, -178.0)
                .relation("WITHIN"))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("2"), equalTo("3")));
        }
    }

    @Override
    public void testLimit2BoundingBox() throws Exception {
        // todo
    }

    @Override
    public void testCompleteLonRange() throws Exception {
        // todo
    }
}
