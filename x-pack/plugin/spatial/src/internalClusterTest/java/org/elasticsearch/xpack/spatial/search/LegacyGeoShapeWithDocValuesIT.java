/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoShapeIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;

public class LegacyGeoShapeWithDocValuesIT extends GeoShapeIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    @Override
    protected void getGeoShapeMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
        b.field("strategy", "recursive");
    }

    @Override
    protected IndexVersion randomSupportedVersion() {
        // legacy shapes can only be created in version lower than 8.x
        return IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0);
    }

    @Override
    protected boolean allowExpensiveQueries() {
        return false;
    }

    public void testMappingUpdate() {
        // create index
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(settings(randomSupportedVersion()).build())
                .setMapping("shape", "type=geo_shape,strategy=recursive")
        );
        ensureGreen();

        String update = """
            {
              "properties": {
                "shape": {
                  "type": "geo_shape"
                }
              }
            }""";

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().preparePutMapping("test").setSource(update, XContentType.JSON).get()
        );
        assertThat(e.getMessage(), containsString("mapper [shape] of type [geo_shape] cannot change strategy from [recursive] to [BKD]"));
    }

    /**
     * Test that the circle is still supported for the legacy shapes
     */
    public void testLegacyCircle() throws Exception {
        // create index
        assertAcked(
            prepareCreate("test").setSettings(settings(randomSupportedVersion()).build())
                .setMapping("shape", "type=geo_shape,strategy=recursive,tree=geohash")
        );
        ensureGreen();

        indexRandom(true, prepareIndex("test").setId("0").setSource("shape", (ToXContent) (builder, params) -> {
            builder.startObject()
                .field("type", "circle")
                .startArray("coordinates")
                .value(30)
                .value(50)
                .endArray()
                .field("radius", "77km")
                .endObject();
            return builder;
        }));

        // test self crossing of circles
        assertHitCount(client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Circle(30, 50, 77000))), 1L);
    }
}
