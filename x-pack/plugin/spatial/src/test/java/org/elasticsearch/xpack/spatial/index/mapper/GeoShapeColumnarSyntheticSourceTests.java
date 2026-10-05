/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that in columnar index modes a {@code geo_shape} field rebuilds {@code _source} natively from its field-owned
 * geometry doc value (instead of being rejected for having no native synthetic source).
 */
public class GeoShapeColumnarSyntheticSourceTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testColumnarSingleValue() throws IOException {
        DocumentMapper mapper = columnarMapper();
        String source = syntheticSource(mapper, b -> b.field("location", "POINT (-71.34 41.12)"));
        assertThat(source, equalTo("{\"location\":\"POINT (-71.34 41.12)\"}"));
    }

    public void testColumnarMultiValuePreservesOrder() throws IOException {
        DocumentMapper mapper = columnarMapper();
        String source = syntheticSource(mapper, b -> b.array("location", "LINESTRING (1 1, 2 2)", "POINT (-71.34 41.12)"));
        assertThat(source, equalTo("{\"location\":[\"LINESTRING (1.0 1.0, 2.0 2.0)\",\"POINT (-71.34 41.12)\"]}"));
    }

    private DocumentMapper columnarMapper() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        return createMapperService(settings, mapping(b -> b.startObject("location").field("type", "geo_shape").endObject()))
            .documentMapper();
    }
}
