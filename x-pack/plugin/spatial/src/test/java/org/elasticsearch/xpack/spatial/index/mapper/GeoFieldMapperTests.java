/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Base class for testing geo field mappers */
public abstract class GeoFieldMapperTests extends MapperTestCase {

    static final String FIELD_NAME = "field";

    @Override
    protected Collection<Plugin> getPlugins() {
        var plugin = new LocalStateSpatialPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return List.of();
            }
        });

        return Collections.singletonList(plugin);
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {}

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", getFieldName());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "POINT (14.0 15.0)";
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return Map.of("x", 14.0, "y", 15.0);
    }

    protected abstract String getFieldName();
}
