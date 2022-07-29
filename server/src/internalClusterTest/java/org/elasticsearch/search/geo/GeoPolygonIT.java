/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoPolygonQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.SuiteScopeTestCase
public class GeoPolygonIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();

        assertAcked(
            prepareCreate("test").setSettings(settings).setMapping("location", "type=geo_point", "alias", "type=alias,path=location")
        );
        ensureGreen();

        indexRandom(
            true,
            client().prepareIndex("test")
                .setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "New York")
                        .startObject("location")
                        .field("lat", 40.714)
                        .field("lon", -74.006)
                        .endObject()
                        .endObject()
                ),
            // to NY: 5.286 km
            client().prepareIndex("test")
                .setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Times Square")
                        .startObject("location")
                        .field("lat", 40.759)
                        .field("lon", -73.984)
                        .endObject()
                        .endObject()
                ),
            // to NY: 0.4621 km
            client().prepareIndex("test")
                .setId("3")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Tribeca")
                        .startObject("location")
                        .field("lat", 40.718)
                        .field("lon", -74.008)
                        .endObject()
                        .endObject()
                ),
            // to NY: 1.055 km
            client().prepareIndex("test")
                .setId("4")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Wall Street")
                        .startObject("location")
                        .field("lat", 40.705)
                        .field("lon", -74.009)
                        .endObject()
                        .endObject()
                ),
            // to NY: 1.258 km
            client().prepareIndex("test")
                .setId("5")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Soho")
                        .startObject("location")
                        .field("lat", 40.725)
                        .field("lon", -74)
                        .endObject()
                        .endObject()
                ),
            // to NY: 2.029 km
            client().prepareIndex("test")
                .setId("6")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Greenwich Village")
                        .startObject("location")
                        .field("lat", 40.731)
                        .field("lon", -73.996)
                        .endObject()
                        .endObject()
                ),
            // to NY: 8.572 km
            client().prepareIndex("test")
                .setId("7")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Brooklyn")
                        .startObject("location")
                        .field("lat", 40.65)
                        .field("lon", -73.95)
                        .endObject()
                        .endObject()
                )
        );
        ensureSearchable("test");
    }

    public void testSimplePolygon() throws Exception {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        points.add(new GeoPoint(40.7, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
            .setQuery(boolQuery().must(geoPolygonQuery("location", points)))
            .get();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    public void testSimpleUnclosedPolygon() throws Exception {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
            .setQuery(boolQuery().must(geoPolygonQuery("location", points)))
            .get();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    public void testFieldAlias() {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        points.add(new GeoPoint(40.7, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
            .setQuery(boolQuery().must(geoPolygonQuery("alias", points)))
            .get();
        assertHitCount(searchResponse, 4);
    }
}
