/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.legacygeo.test.TestLegacyGeoShapeFieldMapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoShapeQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class LegacyGeoShapeQueryTests extends GeoShapeQueryTestCase {

    private static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.PrefixTrees.QUADTREE };

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TestLegacyGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected void createMapping(String indexName, String fieldName, Settings settings) throws Exception {
        final XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "geo_shape")
            .field("tree", randomFrom(PREFIX_TREES))
            .endObject()
            .endObject()
            .endObject();
        client().admin().indices().prepareCreate(indexName).setMapping(xcb).setSettings(settings).get();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testPointsOnlyExplicit() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultFieldName)
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .field("tree_levels", "6")
                .field("distance_error_pct", "0.01")
                .field("points_only", true)
                .endObject()
                .endObject()
                .endObject()
        );

        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get();
        ensureGreen();

        // MULTIPOINT
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex("geo_points_only")
            .setId("1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultFieldName), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // POINT
        Point point = GeometryTestUtils.randomPoint(false);
        client().prepareIndex("geo_points_only")
            .setId("2")
            .setSource(GeoJson.toXContent(point, jsonBuilder().startObject().field(defaultFieldName), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only").setQuery(matchAllQuery()).get();

        assertEquals(2, response.getHits().getTotalHits().value);
    }

    public void testPointsOnly() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultFieldName)
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .field("tree_levels", "6")
                .field("distance_error_pct", "0.01")
                .field("points_only", true)
                .endObject()
                .endObject()
                .endObject()
        );

        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get();
        ensureGreen();

        Geometry geometry = GeometryTestUtils.randomGeometry(false);
        try {
            client().prepareIndex("geo_points_only")
                .setId("1")
                .setSource(GeoJson.toXContent(geometry, jsonBuilder().startObject().field(defaultFieldName), null).endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();
        } catch (Exception e) {
            // Random geometry generator created something other than a POINT type, verify the correct exception is thrown
            assertThat(e.getMessage(), containsString("is configured for points only"));
            return;
        }

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only")
            .setQuery(geoIntersectionQuery(defaultFieldName, geometry))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testFieldAlias() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultFieldName)
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .endObject()
                .startObject("alias")
                .field("type", "alias")
                .field("path", defaultFieldName)
                .endObject()
                .endObject()
                .endObject()
        );

        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultFieldName), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQuery("alias", multiPoint)).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    /**
     * It turns out that LegacyGeoShape has an issue indexing points with longitude=180,
     * but since this is legacy, rather than fix that, we just skip it from tests.
     * See https://github.com/elastic/elasticsearch/issues/86118
     */
    @Override
    protected boolean ignoreLons(double[] lons) {
        return Arrays.stream(lons).anyMatch(v -> v == 180);
    }
}
