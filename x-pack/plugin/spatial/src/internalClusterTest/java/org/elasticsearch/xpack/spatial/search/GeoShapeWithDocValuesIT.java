/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoShapeIntegTestCase;
import org.elasticsearch.test.VersionUtils;
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
    protected Collection<Class<? extends Plugin>>  nodePlugins() {
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/77755")
    public void testMappingUpdate() {
        // create index
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(settings(randomSupportedVersion()).build())
            .setMapping("shape", "type=geo_shape").get());
        ensureGreen();

        String update ="{\n" +
            "  \"properties\": {\n" +
            "    \"shape\": {\n" +
            "      \"type\": \"geo_shape\"," +
            "      \"strategy\": \"recursive\"" +
            "    }\n" +
            "  }\n" +
            "}";

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().indices()
            .preparePutMapping("test")
            .setSource(update, XContentType.JSON).get());
        assertThat(e.getMessage(), containsString("mapper [shape] of type [geo_shape] cannot change strategy from [BKD] to [recursive]"));
    }
}
