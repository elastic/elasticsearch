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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoPolygonQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.SuiteScopeTestCase
public class GeoPolygonIT extends ESIntegTestCase {

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true)
                .startObject("fielddata").field("format", randomNumericFieldDataFormat()).endObject().endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .startObject("location").field("lat", 40.714).field("lon", -74.006).endObject()
                .endObject()), 
        // to NY: 5.286 km
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .startObject("location").field("lat", 40.759).field("lon", -73.984).endObject()
                .endObject()),
        // to NY: 0.4621 km
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .startObject("location").field("lat", 40.718).field("lon", -74.008).endObject()
                .endObject()),
        // to NY: 1.055 km
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .startObject("location").field("lat", 40.705).field("lon", -74.009).endObject()
                .endObject()),
        // to NY: 1.258 km
        client().prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .startObject("location").field("lat", 40.725).field("lon", -74).endObject()
                .endObject()),
        // to NY: 2.029 km
        client().prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .startObject("location").field("lat", 40.731).field("lon", -73.996).endObject()
                .endObject()),
        // to NY: 8.572 km
        client().prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()));
        ensureSearchable("test");
    }

    @Test
    public void simplePolygonTest() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("test") // from NY
                .setQuery(boolQuery().must(geoPolygonQuery("location")
                        .addPoint(40.7, -74.0)
                        .addPoint(40.7, -74.1)
                        .addPoint(40.8, -74.1)
                        .addPoint(40.8, -74.0)
                        .addPoint(40.7, -74.0)))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    @Test
    public void simpleUnclosedPolygon() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
                .setQuery(boolQuery().must(geoPolygonQuery("location")
                        .addPoint(40.7, -74.0)
                        .addPoint(40.7, -74.1)
                        .addPoint(40.8, -74.1)
                        .addPoint(40.8, -74.0)))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }
}
