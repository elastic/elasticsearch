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

import org.apache.lucene.util.XGeoHashUtils;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceRangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class GeoDistanceIT extends ESIntegTestCase {

    @Test
    public void simpleDistanceTests() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true)
                .startObject("fielddata").field("format", randomNumericFieldDataFormat()).endObject().endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()), 
        // to NY: 5.286 km
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject()
                .endObject()),
        // to NY: 0.4621 km
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endObject()),
        // to NY: 1.055 km
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .startObject("location").field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                .endObject()),
        // to NY: 1.258 km
        client().prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject()
                .endObject()),
        // to NY: 2.029 km
        client().prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .startObject("location").field("lat", 40.731033).field("lon", -73.9962255).endObject()
                .endObject()),
        // to NY: 8.572 km
        client().prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()));

        SearchResponse searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("3km").point(40.7143528, -74.0059731))
                .execute().actionGet();
        assertHitCount(searchResponse, 5);
        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("3km").point(40.7143528, -74.0059731).optimizeBbox("indexed"))
                .execute().actionGet();
        assertHitCount(searchResponse, 5);
        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }

        // now with a PLANE type
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("3km").geoDistance(GeoDistance.PLANE).point(40.7143528, -74.0059731))
                .execute().actionGet();
        assertHitCount(searchResponse, 5);
        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }

        // factor type is really too small for this resolution

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("2km").point(40.7143528, -74.0059731))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("2km").point(40.7143528, -74.0059731).optimizeBbox("indexed"))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("1.242mi").point(40.7143528, -74.0059731))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("1.242mi").point(40.7143528, -74.0059731).optimizeBbox("indexed"))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location").from("1.0km").to("2.0km").point(40.7143528, -74.0059731))
                .execute().actionGet();
        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("4"), equalTo("5")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location").from("1.0km").to("2.0km").point(40.7143528, -74.0059731).optimizeBbox("indexed"))
                .execute().actionGet();
        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("4"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location").to("2.0km").point(40.7143528, -74.0059731))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location").from("2.0km").point(40.7143528, -74.0059731))
                .execute().actionGet();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        // SORTING

        searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location").point(40.7143528, -74.0059731).order(SortOrder.ASC))
                .execute().actionGet();

        assertHitCount(searchResponse, 7);
        assertOrderedSearchHits(searchResponse, "1", "3", "4", "5", "6", "2", "7");

        searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location").point(40.7143528, -74.0059731).order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 7);
        assertOrderedSearchHits(searchResponse, "7", "2", "6", "5", "4", "3", "1");
    }

    @Test
    public void testDistanceSortingMVFields() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("locations").field("type", "geo_point").field("lat_lon", true)
                .field("ignore_malformed", true).field("coerce", true).startObject("fielddata")
                .field("format", randomNumericFieldDataFormat()).endObject().endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test")
                .addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("names", "New York")
                .startObject("locations").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("names", "New York 2")
                .startObject("locations").field("lat", 400.7143528).field("lon", 285.9990269).endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("names", "Times Square", "Tribeca")
                .startArray("locations")
                        // to NY: 5.286 km
                .startObject().field("lat", 40.759011).field("lon", -73.9844722).endObject()
                        // to NY: 0.4621 km
                .startObject().field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("names", "Wall Street", "Soho")
                .startArray("locations")
                        // to NY: 1.055 km
                .startObject().field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                        // to NY: 1.258 km
                .startObject().field("lat", 40.7247222).field("lon", -74).endObject()
                .endArray()
                .endObject()).execute().actionGet();


        client().prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("names", "Greenwich Village", "Brooklyn")
                .startArray("locations")
                        // to NY: 2.029 km
                .startObject().field("lat", 40.731033).field("lon", -73.9962255).endObject()
                        // to NY: 8.572 km
                .startObject().field("lat", 40.65).field("lon", -73.95).endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.ASC))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));

        // Order: Asc, Mode: max
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.ASC).sortMode("max"))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2",  "4", "3", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));

        // Order: Desc
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "3", "4", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        // Order: Desc, Mode: min
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.DESC).sortMode("min"))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "4", "3", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).sortMode("avg").order(SortOrder.ASC))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "4", "3", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1157d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(2874d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(5301d, 10d));

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).sortMode("avg").order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "3", "4", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        assertFailures(client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).sortMode("sum")),
                RestStatus.BAD_REQUEST,
                containsString("sort_mode [sum] isn't supported for sorting by geo distance"));
    }

    @Test
    // Regression bug: https://github.com/elasticsearch/elasticsearch/issues/2851
    public void testDistanceSortingWithMissingGeoPoint() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("locations").field("type", "geo_point").field("lat_lon", true)
                .startObject("fielddata").field("format", randomNumericFieldDataFormat()).endObject().endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("names", "Times Square", "Tribeca")
                .startArray("locations")
                        // to NY: 5.286 km
                .startObject().field("lat", 40.759011).field("lon", -73.9844722).endObject()
                        // to NY: 0.4621 km
                .startObject().field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("names", "Wall Street", "Soho")
                .endObject()).execute().actionGet();

        refresh();

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.ASC))
                .execute().actionGet();

        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "1", "2");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));

        // Order: Desc
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.DESC))
                .execute().actionGet();

        // Doc with missing geo point is first, is consistent with 0.20.x
        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "2", "1");        
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5286d, 10d));
    }

    @Test
    public void distanceScriptTests() throws Exception {
        double source_lat = 32.798;
        double source_long = -117.151;
        double target_lat = 32.81;
        double target_long = -117.21;

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "TestPosition")
                .startObject("location").field("lat", source_lat).field("lon", source_long).endObject()
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse1 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistance(" + target_lat + "," + target_long + ")")).execute()
                .actionGet();
        Double resultDistance1 = searchResponse1.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance1,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.DEFAULT), 0.0001d));

        SearchResponse searchResponse2 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].distance(" + target_lat + "," + target_long + ")")).execute()
                .actionGet();
        Double resultDistance2 = searchResponse2.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance2,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.DEFAULT), 0.0001d));

        SearchResponse searchResponse3 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInKm(" + target_lat + "," + target_long + ")"))
                .execute().actionGet();
        Double resultArcDistance3 = searchResponse3.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance3,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.0001d));

        SearchResponse searchResponse4 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].distanceInKm(" + target_lat + "," + target_long + ")")).execute()
                .actionGet();
        Double resultDistance4 = searchResponse4.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance4,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.0001d));

        SearchResponse searchResponse5 = client()
                .prepareSearch()
                .addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInKm(" + (target_lat) + "," + (target_long + 360) + ")"))
                .execute().actionGet();
        Double resultArcDistance5 = searchResponse5.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance5,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.0001d));

        SearchResponse searchResponse6 = client()
                .prepareSearch()
                .addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInKm(" + (target_lat + 360) + "," + (target_long) + ")"))
                .execute().actionGet();
        Double resultArcDistance6 = searchResponse6.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance6,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.0001d));

        SearchResponse searchResponse7 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInMiles(" + target_lat + "," + target_long + ")"))
                .execute().actionGet();
        Double resultDistance7 = searchResponse7.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance7,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES), 0.0001d));

        SearchResponse searchResponse8 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].distanceInMiles(" + target_lat + "," + target_long + ")"))
                .execute().actionGet();
        Double resultDistance8 = searchResponse8.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance8,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES), 0.0001d));
    }


    @Test
    public void testDistanceSortingNestedFields() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("company")
                .startObject("properties")
                .startObject("name").field("type", "string").endObject()
                .startObject("branches")
                .field("type", "nested")
                .startObject("properties")
                .startObject("name").field("type", "string").endObject()
                .startObject("location").field("type", "geo_point").field("lat_lon", true)
                .startObject("fielddata").field("format", randomNumericFieldDataFormat()).endObject().endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject().endObject();

        assertAcked(prepareCreate("companies").addMapping("company", xContentBuilder));
        ensureGreen();

        indexRandom(true, client().prepareIndex("companies", "company", "1").setSource(jsonBuilder().startObject()
                .field("name", "company 1")
                .startArray("branches")
                    .startObject()
                        .field("name", "New York")
                        .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                    .endObject()
                .endArray()
                .endObject()), 
        client().prepareIndex("companies", "company", "2").setSource(jsonBuilder().startObject()
                .field("name", "company 2")
                .startArray("branches")
                    .startObject()
                        .field("name", "Times Square")
                        .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject() // to NY: 5.286 km
                    .endObject()
                    .startObject()
                        .field("name", "Tribeca")
                        .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject() // to NY: 0.4621 km
                    .endObject()
                .endArray()
                .endObject()),
        client().prepareIndex("companies", "company", "3").setSource(jsonBuilder().startObject()
                .field("name", "company 3")
                .startArray("branches")
                    .startObject()
                        .field("name", "Wall Street")
                        .startObject("location").field("lat", 40.7051157).field("lon", -74.0088305).endObject() // to NY: 1.055 km
                    .endObject()
                    .startObject()
                        .field("name", "Soho")
                        .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject() // to NY: 1.258 km
                    .endObject()
                .endArray()
                .endObject()),
        client().prepareIndex("companies", "company", "4").setSource(jsonBuilder().startObject()
                .field("name", "company 4")
                .startArray("branches")
                    .startObject()
                        .field("name", "Greenwich Village")
                        .startObject("location").field("lat", 40.731033).field("lon", -73.9962255).endObject() // to NY: 2.029 km
                    .endObject()
                    .startObject()
                        .field("name", "Brooklyn")
                        .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject() // to NY: 8.572 km
                    .endObject()
                .endArray()
                .endObject()));

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("branches.location").point(40.7143528, -74.0059731).order(SortOrder.ASC).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));

        // Order: Asc, Mode: max
        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("branches.location").point(40.7143528, -74.0059731).order(SortOrder.ASC).sortMode("max").setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "3", "2", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));

        // Order: Desc
        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("branches.location").point(40.7143528, -74.0059731).order(SortOrder.DESC).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "2", "3", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        // Order: Desc, Mode: min
        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("branches.location").point(40.7143528, -74.0059731).order(SortOrder.DESC).sortMode("min").setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "3", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("branches.location").point(40.7143528, -74.0059731).sortMode("avg").order(SortOrder.ASC).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "3", "2", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));

        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location").setNestedPath("branches")
                            .point(40.7143528, -74.0059731).sortMode("avg").order(SortOrder.DESC).setNestedPath("branches")
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "2", "3", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.geoDistanceSort("branches.location").setNestedFilter(termQuery("branches.name", "brooklyn"))
                                .point(40.7143528, -74.0059731).sortMode("avg").order(SortOrder.ASC).setNestedPath("branches")
                )
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertFirstHit(searchResponse, hasId("4"));
        assertSearchHits(searchResponse, "1", "2", "3", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));

        assertFailures(client().prepareSearch("companies").setQuery(matchAllQuery())
                    .addSort(SortBuilders.geoDistanceSort("branches.location").point(40.7143528, -74.0059731).sortMode("sum").setNestedPath("branches")),
                RestStatus.BAD_REQUEST,
                containsString("sort_mode [sum] isn't supported for sorting by geo distance"));
    }
    
    /**
     * Issue 3073
     */
    @Test
    public void testGeoDistanceFilter() throws IOException {
        double lat = 40.720611;
        double lon = -73.998776;

        XContentBuilder mapping = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("location")
                        .startObject("properties")
                            .startObject("pin")
                                .field("type", "geo_point")
                                .field("geohash", true)
                                .field("geohash_precision", 24)
                                .field("lat_lon", true)
                                .startObject("fielddata")
                                    .field("format", randomNumericFieldDataFormat())
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        XContentBuilder source = JsonXContent.contentBuilder()
                .startObject()
                    .field("pin", XGeoHashUtils.stringEncode(lon, lat))
                .endObject();

        assertAcked(prepareCreate("locations").addMapping("location", mapping));
        client().prepareIndex("locations", "location", "1").setCreate(true).setSource(source).execute().actionGet();
        refresh();
        client().prepareGet("locations", "location", "1").execute().actionGet();

        SearchResponse result = client().prepareSearch("locations")
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(QueryBuilders.geoDistanceQuery("pin")
                        .geoDistance(GeoDistance.ARC)
                        .lat(lat).lon(lon)
                        .distance("1m"))
                .execute().actionGet();

        assertHitCount(result, 1);
    } 

    private double randomLon() {
        return randomDouble() * 360 - 180;
    }

    private double randomLat() {
        return randomDouble() * 180 - 90;
    }

    public void testDuelOptimizations() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "location", "type=geo_point,lat_lon=true"));
        final int numDocs = scaledRandomIntBetween(3000, 10000);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(client().prepareIndex("index", "type").setSource(jsonBuilder().startObject().startObject("location").field("lat", randomLat()).field("lon", randomLon()).endObject().endObject()));
        }
        indexRandom(true, docs);
        ensureSearchable();

        for (int i = 0; i < 10; ++i) {
            final double originLat = randomLat();
            final double originLon = randomLon();
            final String distance = DistanceUnit.KILOMETERS.toString(randomInt(10000));
            for (GeoDistance geoDistance : Arrays.asList(GeoDistance.ARC, GeoDistance.SLOPPY_ARC)) {
                logger.info("Now testing GeoDistance={}, distance={}, origin=({}, {})", geoDistance, distance, originLat, originLon);
                long matches = -1;
                for (String optimizeBbox : Arrays.asList("none", "memory", "indexed")) {
                    SearchResponse resp = client().prepareSearch("index").setSize(0).setQuery(QueryBuilders.constantScoreQuery(
                            QueryBuilders.geoDistanceQuery("location").point(originLat, originLon).distance(distance).geoDistance(geoDistance).optimizeBbox(optimizeBbox))).execute().actionGet();
                    assertSearchResponse(resp);
                    logger.info("{} -> {} hits", optimizeBbox, resp.getHits().totalHits());
                    if (matches < 0) {
                        matches = resp.getHits().totalHits();
                    } else {
                        assertEquals(matches, resp.getHits().totalHits());
                    }
                }
            }
        }
    }

}