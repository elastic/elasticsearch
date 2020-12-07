/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.matchonlytext;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.matchonlytext.mapper.MatchOnlyTextFieldMapper;

import java.util.Map;

import static java.util.Collections.singletonMap;

public class MatchOnlyTextMapperPlugin extends Plugin implements MapperPlugin {
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(MatchOnlyTextFieldMapper.CONTENT_TYPE, MatchOnlyTextFieldMapper.PARSER);
    }

}
