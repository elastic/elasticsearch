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

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class GeoBoundingBoxIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(InternalSettingsPlugin.class); // uses index.version.created
    }

    public void testSimpleBoundingBoxTest() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true);
        }
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", xContentBuilder));
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
                .setQuery(geoBoundingBoxQuery("location").setCorners(40.73, -74.1, 40.717, -73.99))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoBoundingBoxQuery("location").setCorners(40.73, -74.1, 40.717, -73.99).type("indexed"))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }
    }

    public void testLimit2BoundingBox() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true);
        }
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("userid", 880)
                .field("title", "Place in Stockholm")
                .startObject("location").field("lat", 59.328355000000002).field("lon", 18.036842).endObject()
                .endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("userid", 534)
                .field("title", "Place in Montreal")
                .startObject("location").field("lat", 45.509526999999999).field("lon", -73.570986000000005).endObject()
                .endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 880)).filter(
                                geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 880)).filter(
                                geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875).type("indexed"))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));

        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 534)).filter(
                                geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 534)).filter(
                                geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875).type("indexed"))
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
    }

    public void testCompleteLonRange() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true);
        }
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("userid", 880)
                .field("title", "Place in Stockholm")
                .startObject("location").field("lat", 59.328355000000002).field("lon", 18.036842).endObject()
                .endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("userid", 534)
                .field("title", "Place in Montreal")
                .startObject("location").field("lat", 45.509526999999999).field("lon", -73.570986000000005).endObject()
                .endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, -180, -50, 180)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, -180, -50, 180).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, -180, -90, 180)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, -180, -90, 180).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2L));

        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, 0, -50, 360)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, 0, -50, 360).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, 0, -90, 360)
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, 0, -90, 360).type("indexed")
                ).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2L));
    }
}

