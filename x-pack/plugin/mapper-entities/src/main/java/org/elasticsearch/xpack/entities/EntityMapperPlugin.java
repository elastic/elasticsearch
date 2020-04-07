/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.entities;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.entities.mapper.EntityKeywordFieldMapper;

import java.util.Collections;
import java.util.Map;

public class EntityMapperPlugin extends Plugin implements MapperPlugin {

    public EntityMapperPlugin(Settings settings) {}

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(EntityKeywordFieldMapper.CONTENT_TYPE, new EntityKeywordFieldMapper.TypeParser());
    }
}
