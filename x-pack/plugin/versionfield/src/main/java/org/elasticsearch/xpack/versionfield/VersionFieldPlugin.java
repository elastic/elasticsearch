/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.VersionRangeType;
import org.elasticsearch.index.mapper.VersionStringFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class VersionFieldPlugin extends Plugin implements MapperPlugin {

    public VersionFieldPlugin(Settings settings) {}

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(VersionRangeType.VERSION_RANGE_NAME, new RangeFieldMapper.TypeParser(VersionRangeType.INSTANCE));
        mappers.put(VersionStringFieldMapper.CONTENT_TYPE, new VersionStringFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }
}
