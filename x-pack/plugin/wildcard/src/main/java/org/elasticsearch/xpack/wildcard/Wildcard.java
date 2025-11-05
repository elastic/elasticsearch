/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Plugin for wildcard field mapping in Elasticsearch.
 * <p>
 * This plugin provides the {@code wildcard} field type, which is optimized for fields
 * that will be queried using wildcard and regexp patterns. Unlike the {@code keyword}
 * field type, which is optimized for exact matches, the wildcard field uses an n-gram
 * index structure that provides efficient wildcard searches even on large text values.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * PUT /my-index
 * {
 *   "mappings": {
 *     "properties": {
 *       "file_path": {
 *         "type": "wildcard"
 *       }
 *     }
 *   }
 * }
 *
 * GET /my-index/_search
 * {
 *   "query": {
 *     "wildcard": {
 *       "file_path": "**/config/*.yaml"
 *     }
 *   }
 * }
 * }</pre>
 */
public class Wildcard extends Plugin implements MapperPlugin {

    /**
     * Returns the field mappers provided by this plugin.
     *
     * @return a map containing the wildcard field type parser
     */
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(WildcardFieldMapper.CONTENT_TYPE, WildcardFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }
}
