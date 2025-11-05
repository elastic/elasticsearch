/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper;

import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Plugin for the constant keyword field mapper in Elasticsearch.
 * <p>
 * This plugin provides the {@code constant_keyword} field type, which is optimized for
 * fields that have the same value across all documents in an index. This field type uses
 * minimal storage and provides efficient query performance by storing the value once in
 * metadata rather than for each document.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * PUT /my-index
 * {
 *   "mappings": {
 *     "properties": {
 *       "environment": {
 *         "type": "constant_keyword",
 *         "value": "production"
 *       }
 *     }
 *   }
 * }
 * }</pre>
 */
public class ConstantKeywordMapperPlugin extends Plugin implements MapperPlugin {
    /**
     * Returns the field mappers provided by this plugin.
     *
     * @return a map containing the constant keyword field type parser
     */
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(ConstantKeywordFieldMapper.CONTENT_TYPE, ConstantKeywordFieldMapper.PARSER);
    }

}
