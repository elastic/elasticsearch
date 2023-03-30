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
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class CartesianShapeIT extends CartesianShapeIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    @Override
    protected void getGeoShapeMapping(XContentBuilder b) throws IOException {
        b.field("type", "shape");
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
            client().admin().indices().prepareCreate("test").setSettings(settings(version).build()).setMapping("shape", "type=shape").get()
        );
        ensureGreen();

        String update = """
            {
              "properties": {
                "shape": {
                  "type": "shape",
                  "strategy": "recursive"
                }
              }
            }""";

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().preparePutMapping("test").setSource(update, XContentType.JSON).get()
        );
        assertThat(e.getMessage(), containsString("unknown parameter [strategy] on mapper [shape] of type [shape]"));
    }
}
