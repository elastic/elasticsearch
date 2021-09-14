/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.mapper;

import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.size.SizeFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.Map;

public class MapperSizePlugin extends Plugin implements MapperPlugin {

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.singletonMap(SizeFieldMapper.NAME, SizeFieldMapper.PARSER);
    }
}
