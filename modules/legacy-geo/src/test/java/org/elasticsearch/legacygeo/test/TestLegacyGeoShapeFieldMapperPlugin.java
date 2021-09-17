/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.legacygeo.test;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.legacygeo.LegacyGeoPlugin;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test plugin to test functionality of this mapper.
 */
@Deprecated
public class TestLegacyGeoShapeFieldMapperPlugin extends LegacyGeoPlugin implements MapperPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(LegacyGeoShapeFieldMapper.CONTENT_TYPE, LegacyGeoShapeFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }
}
