/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoShapeIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class GeoShapeWithDocValuesIT extends GeoShapeIntegTestCase {

    @Override
    protected boolean addMockGeoShapeFieldMapper() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    @Override
    protected void getGeoShapeMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
    }

    @Override
    protected Version randomSupportedVersion() {
        return VersionUtils.randomIndexCompatibleVersion(random());
    }

    @Override
    protected boolean allowExpensiveQueries() {
        return true;
    }

    public void testMappingUpdate() {
        // create index
        Version version = randomSupportedVersion();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(settings(version).build())
                .setMapping("shape", "type=geo_shape")
                .get()
        );
        ensureGreen();

        String update = """
            {
              "properties": {
                "shape": {
                  "type": "geo_shape",
                  "strategy": "recursive"
                }
              }
            }""";

        if (version.before(Version.V_8_0_0)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().preparePutMapping("test").setSource(update, XContentType.JSON).get()
            );
            assertThat(
                e.getMessage(),
                containsString("mapper [shape] of type [geo_shape] cannot change strategy from [BKD] to [recursive]")
            );
        } else {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> client().admin().indices().preparePutMapping("test").setSource(update, XContentType.JSON).get()
            );
            assertThat(
                e.getMessage(),
                containsString("using deprecated parameters [strategy] in mapper [shape] of type [geo_shape] is no longer allowed")
            );
        }
    }
}
