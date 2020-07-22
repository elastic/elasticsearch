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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class GeoBoundingBoxQueryIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testSimpleBoundingBoxTest() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("location").field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()).get();

        // to NY: 5.286 km
        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject()
                .endObject()).get();

        // to NY: 0.4621 km
        client().prepareIndex("test").setId("3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endObject()).get();

        // to NY: 1.055 km
        client().prepareIndex("test").setId("4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .startObject("location").field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                .endObject()).get();

        // to NY: 1.258 km
        client().prepareIndex("test").setId("5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject()
                .endObject()).get();

        // to NY: 2.029 km
        client().prepareIndex("test").setId("6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .startObject("location").field("lat", 40.731033).field("lon", -73.9962255).endObject()
                .endObject()).get();

        // to NY: 8.572 km
        client().prepareIndex("test").setId("7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()).get();

        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse = client().prepareSearch() // from NY
                .setQuery(geoBoundingBoxQuery("location").setCorners(40.73, -74.1, 40.717, -73.99))
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
                .setQuery(geoBoundingBoxQuery("location").setCorners(40.73, -74.1, 40.717, -73.99).type("indexed"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }
    }

    public void testLimit2BoundingBox() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("location").field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject()
                .field("userid", 880)
                .field("title", "Place in Stockholm")
                .startObject("location").field("lat", 59.328355000000002).field("lon", 18.036842).endObject()
                .endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject()
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
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 880)).filter(
                                geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875)
                                        .type("indexed"))
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 534)).filter(
                                geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875))
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        boolQuery().must(termQuery("userid", 534)).filter(
                                geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875)
                                        .type("indexed"))
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testCompleteLonRange() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("location").field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject()
                .field("userid", 880)
                .field("title", "Place in Stockholm")
                .startObject("location").field("lat", 59.328355000000002).field("lon", 18.036842).endObject()
                .endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject()
                .field("userid", 534)
                .field("title", "Place in Montreal")
                .startObject("location").field("lat", 45.509526999999999).field("lon", -73.570986000000005).endObject()
                .endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, -180, -50, 180)
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, -180, -50, 180)
                            .type("indexed")
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, -180, -90, 180)
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, -180, -90, 180)
                            .type("indexed")
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));

        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, 0, -50, 360)
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, 0, -50, 360)
                                .type("indexed")
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, 0, -90, 360)
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        searchResponse = client().prepareSearch()
                .setQuery(
                        geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, 0, -90, 360)
                                .type("indexed")
                ).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
    }
}

