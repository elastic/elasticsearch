/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoShapeQueryTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class LegacyGeoShapeWithDocValuesQueryTests extends GeoShapeQueryTestCase {

    @SuppressWarnings("deprecation")
    private static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.PrefixTrees.QUADTREE };

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
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

        final Settings finalSetting;
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().prepareCreate(indexName).setMapping(xcb).setSettings(settings).get()
        );
        assertThat(
            ex.getMessage(),
            containsString("using deprecated parameters [tree] in mapper [" + fieldName + "] of type [geo_shape] is no longer allowed")
        );
        Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        finalSetting = settings(version).put(settings).build();
        client().admin().indices().prepareCreate(indexName).setMapping(xcb).setSettings(finalSetting).get();
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
                .startObject(defaultGeoFieldName)
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .field("tree_levels", "6")
                .field("distance_error_pct", "0.01")
                .field("points_only", true)
                .endObject()
                .endObject()
                .endObject()
        );

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get()
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "using deprecated parameters [points_only, tree, distance_error_pct, tree_levels] "
                    + "in mapper [geo] of type [geo_shape] is no longer allowed"
            )
        );

        Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        Settings settings = settings(version).build();
        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).setSettings(settings).get();
        ensureGreen();

        // MULTIPOINT
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex("geo_points_only")
            .setId("1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // POINT
        Point point = GeometryTestUtils.randomPoint(false);
        client().prepareIndex("geo_points_only")
            .setId("2")
            .setSource(GeoJson.toXContent(point, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject())
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
                .startObject(defaultGeoFieldName)
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .field("tree_levels", "6")
                .field("distance_error_pct", "0.01")
                .field("points_only", true)
                .endObject()
                .endObject()
                .endObject()
        );

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get()
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "using deprecated parameters [points_only, tree, distance_error_pct, tree_levels] "
                    + "in mapper [geo] of type [geo_shape] is no longer allowed"
            )
        );

        Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        Settings settings = settings(version).build();
        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).setSettings(settings).get();
        ensureGreen();

        Geometry geometry = GeometryTestUtils.randomGeometry(false);
        try {
            client().prepareIndex("geo_points_only")
                .setId("1")
                .setSource(GeoJson.toXContent(geometry, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();
        } catch (MapperParsingException e) {
            // Random geometry generator created something other than a POINT type, verify the correct exception is thrown
            assertThat(e.getMessage(), containsString("is configured for points only"));
            return;
        }

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only")
            .setQuery(geoIntersectionQuery(defaultGeoFieldName, geometry))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testFieldAlias() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultGeoFieldName)
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .endObject()
                .startObject("alias")
                .field("type", "alias")
                .field("path", defaultGeoFieldName)
                .endObject()
                .endObject()
                .endObject()
        );

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get()
        );
        assertThat(
            ex.getMessage(),
            containsString("using deprecated parameters [tree] in mapper [geo] of type [geo_shape] is no longer allowed")
        );

        Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        Settings settings = settings(version).build();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).setSettings(settings).get();
        ensureGreen();

        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQuery("alias", multiPoint)).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }
}
