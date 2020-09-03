/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper;

import java.util.Map;

import static java.util.Collections.singletonMap;

public class FlattenedMapperPlugin extends Plugin implements MapperPlugin {

    public FlattenedMapperPlugin() { }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(FlatObjectFieldMapper.CONTENT_TYPE, new FlatObjectFieldMapper.TypeParser());
    }
}
