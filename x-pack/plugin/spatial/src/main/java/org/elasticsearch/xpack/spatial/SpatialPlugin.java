/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class SpatialPlugin extends Plugin implements MapperPlugin {

    public SpatialPlugin(Settings settings) {
    }

    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(b -> {
            XPackPlugin.bindFeatureSet(b, SpatialFeatureSet.class);
        });
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(ShapeFieldMapper.CONTENT_TYPE, new ShapeFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }
}
