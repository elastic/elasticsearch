/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Some tests depend on the {@link org.elasticsearch.index.mapper.GeoShapeFieldMapper}.
 * This mapper is registered in the spatial-extras module, but used in many integration
 * tests in server code. The goal is to migrate all of the spatial/geo pieces to the spatial-extras
 * module such that no tests in server depend on this test plugin
 */
@Deprecated
public class TestGeoShapeFieldMapperPlugin extends Plugin implements MapperPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(GeoShapeFieldMapper.CONTENT_TYPE, GeoShapeFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }
}
