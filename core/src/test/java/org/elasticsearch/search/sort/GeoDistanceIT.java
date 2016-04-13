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

package org.elasticsearch.search.sort;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceRangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;


public class GeoDistanceIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testSimpleDistance() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("location").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true);
        }
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", xContentBuilder));
        ensureGreen();

        indexRandom(true,
                client().prepareIndex("test", "type1", "1")
                        .setSource(jsonBuilder().startObject().field("name", "New York").startObject("location").field("lat", 40.7143528)
                                .field("lon", -74.0059731).endObject().endObject()),
                // to NY: 5.286 km
                client().prepareIndex("test", "type1", "2")
                        .setSource(jsonBuilder().startObject().field("name", "Times Square").startObject("location").field("lat", 40.759011)
                                .field("lon", -73.9844722).endObject().endObject()),
                // to NY: 0.4621 km
                client().prepareIndex("test", "type1", "3")
                        .setSource(jsonBuilder().startObject().field("name", "Tribeca").startObject("location").field("lat", 40.718266)
                                .field("lon", -74.007819).endObject().endObject()),
                // to NY: 1.055 km
                client().prepareIndex("test", "type1", "4")
                        .setSource(jsonBuilder().startObject().field("name", "Wall Street").startObject("location").field("lat", 40.7051157)
                                .field("lon", -74.0088305).endObject().endObject()),
                // to NY: 1.258 km
                client().prepareIndex("test", "type1", "5")
                        .setSource(jsonBuilder().startObject().field("name", "Soho").startObject("location").field("lat", 40.7247222)
                                .field("lon", -74).endObject().endObject()),
                // to NY: 2.029 km
                client().prepareIndex("test", "type1", "6")
                        .setSource(jsonBuilder().startObject().field("name", "Greenwich Village").startObject("location")
                                .field("lat", 40.731033).field("lon", -73.9962255).endObject().endObject()),
                // to NY: 8.572 km
                client().prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject().field("name", "Brooklyn")
                        .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject().endObject()));

        SearchResponse searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("3km").point(40.7143528, -74.0059731)).execute().actionGet();
        assertHitCount(searchResponse, 5);
        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("3km").point(40.7143528, -74.0059731).optimizeBbox("indexed")).execute()
                .actionGet();
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
                .setQuery(geoDistanceQuery("location").distance("2km").point(40.7143528, -74.0059731)).execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("2km").point(40.7143528, -74.0059731).optimizeBbox("indexed")).execute()
                .actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("1.242mi").point(40.7143528, -74.0059731)).execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceQuery("location").distance("1.242mi").point(40.7143528, -74.0059731).optimizeBbox("indexed")).execute()
                .actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location", 40.7143528, -74.0059731).from("1.0km").to("2.0km")).execute().actionGet();
        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("4"), equalTo("5")));
        }
        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location", 40.7143528, -74.0059731).from("1.0km").to("2.0km").optimizeBbox("indexed"))
                .execute().actionGet();
        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("4"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location", 40.7143528, -74.0059731).to("2.0km")).execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoDistanceRangeQuery("location", 40.7143528, -74.0059731).from("2.0km")).execute().actionGet();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        // SORTING

        searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location", 40.7143528, -74.0059731).order(SortOrder.ASC)).execute()
                .actionGet();

        assertHitCount(searchResponse, 7);
        assertOrderedSearchHits(searchResponse, "1", "3", "4", "5", "6", "2", "7");

        searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location", 40.7143528, -74.0059731).order(SortOrder.DESC)).execute()
                .actionGet();

        assertHitCount(searchResponse, 7);
        assertOrderedSearchHits(searchResponse, "7", "2", "6", "5", "4", "3", "1");
    }

    public void testDistanceSortingMVFields() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("locations").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true).field("coerce", true);
        }
        xContentBuilder.field("ignore_malformed", true).endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("names", "New York")
                .startObject("locations").field("lat", 40.7143528).field("lon", -74.0059731).endObject().endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("names", "New York 2")
                .startObject("locations").field("lat", 400.7143528).field("lon", 285.9990269).endObject().endObject()).execute()
                .actionGet();

        client().prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder().startObject().field("names", "Times Square", "Tribeca").startArray("locations")
                        // to NY: 5.286 km
                        .startObject().field("lat", 40.759011).field("lon", -73.9844722).endObject()
                        // to NY: 0.4621 km
                        .startObject().field("lat", 40.718266).field("lon", -74.007819).endObject().endArray().endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "4")
                .setSource(jsonBuilder().startObject().field("names", "Wall Street", "Soho").startArray("locations")
                        // to NY: 1.055 km
                        .startObject().field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                        // to NY: 1.258 km
                        .startObject().field("lat", 40.7247222).field("lon", -74).endObject().endArray().endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "5")
                .setSource(jsonBuilder().startObject().field("names", "Greenwich Village", "Brooklyn").startArray("locations")
                        // to NY: 2.029 km
                        .startObject().field("lat", 40.731033).field("lon", -73.9962255).endObject()
                        // to NY: 8.572 km
                        .startObject().field("lat", 40.65).field("lon", -73.95).endObject().endArray().endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC)).execute()
                .actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));

        // Order: Asc, Mode: max
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC).sortMode(SortMode.MAX))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "4", "3", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));

        // Order: Desc
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC)).execute()
                .actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "3", "4", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        // Order: Desc, Mode: min
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC).sortMode(SortMode.MIN))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "4", "3", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.AVG).order(SortOrder.ASC))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "4", "3", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1157d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(2874d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(5301d, 10d));

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.AVG).order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "3", "4", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        try {
                client().prepareSearch("test").setQuery(matchAllQuery())
                        .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.SUM));
                fail("sum should not be supported for sorting by geo distance");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Regression bug:
    // https://github.com/elastic/elasticsearch/issues/2851
    public void testDistanceSortingWithMissingGeoPoint() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("locations").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true);
        }
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("names", "Times Square", "Tribeca").startArray("locations")
                        // to NY: 5.286 km
                        .startObject().field("lat", 40.759011).field("lon", -73.9844722).endObject()
                        // to NY: 0.4621 km
                        .startObject().field("lat", 40.718266).field("lon", -74.007819).endObject().endArray().endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("names", "Wall Street", "Soho").endObject())
                .execute().actionGet();

        refresh();

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC)).execute()
                .actionGet();

        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "1", "2");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));

        // Order: Desc
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC)).execute()
                .actionGet();

        // Doc with missing geo point is first, is consistent with 0.20.x
        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5286d, 10d));
    }

    public void testDistanceSortingNestedFields() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("company").startObject("properties")
                .startObject("name").field("type", "text").endObject().startObject("branches").field("type", "nested")
                .startObject("properties").startObject("name").field("type", "text").endObject().startObject("location")
                .field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true);
        }
        xContentBuilder.endObject().endObject().endObject().endObject().endObject().endObject();

        assertAcked(prepareCreate("companies").setSettings(settings).addMapping("company", xContentBuilder));
        ensureGreen();

        indexRandom(true,
                client().prepareIndex("companies", "company", "1")
                        .setSource(
                                jsonBuilder().startObject().field("name", "company 1").startArray("branches").startObject()
                                        .field("name", "New York").startObject("location").field("lat", 40.7143528)
                                        .field("lon",
                                                -74.0059731)
                                        .endObject().endObject().endArray().endObject()),
                client().prepareIndex("companies", "company", "2")
                        .setSource(jsonBuilder().startObject().field("name", "company 2").startArray("branches").startObject()
                                .field("name", "Times Square").startObject("location").field("lat", 40.759011).field("lon", -73.9844722)
                                .endObject() // to NY: 5.286 km
                                .endObject().startObject().field("name", "Tribeca").startObject("location").field("lat", 40.718266)
                                .field("lon", -74.007819).endObject() // to NY:
                                                                      // 0.4621
                                                                      // km
                                .endObject().endArray().endObject()),
                client().prepareIndex("companies", "company", "3")
                        .setSource(jsonBuilder().startObject().field("name", "company 3").startArray("branches").startObject()
                                .field("name", "Wall Street").startObject("location").field("lat", 40.7051157).field("lon", -74.0088305)
                                .endObject() // to NY: 1.055 km
                                .endObject().startObject().field("name", "Soho").startObject("location").field("lat", 40.7247222)
                                .field("lon", -74).endObject() // to NY: 1.258
                                                               // km
                                .endObject().endArray().endObject()),
                client().prepareIndex("companies", "company", "4")
                        .setSource(jsonBuilder().startObject().field("name", "company 4").startArray("branches").startObject()
                                .field("name", "Greenwich Village").startObject("location").field("lat", 40.731033)
                                .field("lon", -73.9962255).endObject() // to NY:
                                                                       // 2.029
                                                                       // km
                                .endObject().startObject().field("name", "Brooklyn").startObject("location").field("lat", 40.65)
                                .field("lon", -73.95).endObject() // to NY:
                                                                  // 8.572 km
                                .endObject().endArray().endObject()));

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery()).addSort(SortBuilders
                .geoDistanceSort("branches.location", 40.7143528, -74.0059731).order(SortOrder.ASC).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));

        // Order: Asc, Mode: max
        searchResponse = client()
                .prepareSearch("companies").setQuery(matchAllQuery()).addSort(SortBuilders.geoDistanceSort("branches.location",
                        40.7143528, -74.0059731).order(SortOrder.ASC).sortMode(SortMode.MAX).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "3", "2", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));

        // Order: Desc
        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery()).addSort(SortBuilders
                .geoDistanceSort("branches.location", 40.7143528, -74.0059731).order(SortOrder.DESC).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "2", "3", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        // Order: Desc, Mode: min
        searchResponse = client()
                .prepareSearch("companies").setQuery(matchAllQuery()).addSort(SortBuilders.geoDistanceSort("branches.location",
                        40.7143528, -74.0059731).order(SortOrder.DESC).sortMode(SortMode.MIN).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "3", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client()
                .prepareSearch("companies").setQuery(matchAllQuery()).addSort(SortBuilders.geoDistanceSort("branches.location",
                        40.7143528, -74.0059731).sortMode(SortMode.AVG).order(SortOrder.ASC).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "3", "2", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));

        searchResponse = client().prepareSearch("companies")
                .setQuery(matchAllQuery()).addSort(SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .setNestedPath("branches").sortMode(SortMode.AVG).order(SortOrder.DESC).setNestedPath("branches"))
                .execute().actionGet();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "2", "3", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .setNestedFilter(termQuery("branches.name", "brooklyn"))
                        .sortMode(SortMode.AVG).order(SortOrder.ASC).setNestedPath("branches"))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertFirstHit(searchResponse, hasId("4"));
        assertSearchHits(searchResponse, "1", "2", "3", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));

        try {
                client().prepareSearch("companies").setQuery(matchAllQuery())
                        .addSort(SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731).sortMode(SortMode.SUM)
                                .setNestedPath("branches"));
                fail("Sum should not be allowed as sort mode");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    /**
     * Issue 3073
     */
    public void testGeoDistanceFilter() throws IOException {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        double lat = 40.720611;
        double lon = -73.998776;

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject().startObject("location").startObject("properties")
                .startObject("pin").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            mapping.field("lat_lon", true);
        }
        mapping.endObject().endObject().endObject().endObject();

        XContentBuilder source = JsonXContent.contentBuilder().startObject().field("pin", GeoHashUtils.stringEncode(lon, lat)).endObject();

        assertAcked(prepareCreate("locations").setSettings(settings).addMapping("location", mapping));
        client().prepareIndex("locations", "location", "1").setCreate(true).setSource(source).execute().actionGet();
        refresh();
        client().prepareGet("locations", "location", "1").execute().actionGet();

        SearchResponse result = client().prepareSearch("locations").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(QueryBuilders.geoDistanceQuery("pin").geoDistance(GeoDistance.ARC).point(lat, lon).distance("1m")).execute()
                .actionGet();

        assertHitCount(result, 1);
    }

    private static double randomLon() {
        return randomDouble() * 360 - 180;
    }

    private static double randomLat() {
        return randomDouble() * 180 - 90;
    }

    public void testDuelOptimizations() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        if (version.before(Version.V_2_2_0)) {
            assertAcked(prepareCreate("index").setSettings(settings).addMapping("type", "location", "type=geo_point,lat_lon=true"));
        } else {
            assertAcked(prepareCreate("index").setSettings(settings).addMapping("type", "location", "type=geo_point"));
        }
        final int numDocs = scaledRandomIntBetween(3000, 10000);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(client().prepareIndex("index", "type").setSource(jsonBuilder().startObject().startObject("location")
                    .field("lat", randomLat()).field("lon", randomLon()).endObject().endObject()));
        }
        indexRandom(true, docs);
        ensureSearchable();

        for (int i = 0; i < 10; ++i) {
            final double originLat = randomLat();
            final double originLon = randomLon();
            final String distance = DistanceUnit.KILOMETERS.toString(randomIntBetween(1, 10000));
            for (GeoDistance geoDistance : Arrays.asList(GeoDistance.ARC, GeoDistance.SLOPPY_ARC)) {
                logger.info("Now testing GeoDistance={}, distance={}, origin=({}, {})", geoDistance, distance, originLat, originLon);
                GeoDistanceQueryBuilder qb = QueryBuilders.geoDistanceQuery("location").point(originLat, originLon).distance(distance)
                        .geoDistance(geoDistance);
                long matches;
                if (version.before(Version.V_2_2_0)) {
                    for (String optimizeBbox : Arrays.asList("none", "memory", "indexed")) {
                        qb.optimizeBbox(optimizeBbox);
                        SearchResponse resp = client().prepareSearch("index").setSize(0).setQuery(QueryBuilders.constantScoreQuery(qb))
                                .execute().actionGet();
                        matches = assertDuelOptimization(resp);
                        logger.info("{} -> {} hits", optimizeBbox, matches);
                    }
                } else {
                    SearchResponse resp = client().prepareSearch("index").setSize(0).setQuery(QueryBuilders.constantScoreQuery(qb))
                            .execute().actionGet();
                    matches = assertDuelOptimization(resp);
                    logger.info("{} hits", matches);
                }
            }
        }
    }

    private static long assertDuelOptimization(SearchResponse resp) {
        long matches = -1;
        assertSearchResponse(resp);
        if (matches < 0) {
            matches = resp.getHits().totalHits();
        } else {
            assertEquals(matches, matches = resp.getHits().totalHits());
        }
        return matches;
    }
}
