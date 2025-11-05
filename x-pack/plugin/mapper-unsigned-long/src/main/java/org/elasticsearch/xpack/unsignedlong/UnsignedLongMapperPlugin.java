/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Plugin for unsigned long field mapping in Elasticsearch.
 * <p>
 * This plugin provides the {@code unsigned_long} field type, which supports storing
 * and querying unsigned 64-bit integers (range: 0 to 2^64-1). This is useful for
 * fields that store values exceeding the signed long range (2^63-1), such as large
 * counters, timestamps in nanoseconds, or unsigned identifiers.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * PUT /my-index
 * {
 *   "mappings": {
 *     "properties": {
 *       "counter": {
 *         "type": "unsigned_long"
 *       }
 *     }
 *   }
 * }
 *
 * POST /my-index/_doc
 * {
 *   "counter": 18446744073709551615
 * }
 * }</pre>
 */
public class UnsignedLongMapperPlugin extends Plugin implements MapperPlugin {

    /**
     * Returns the field mappers provided by this plugin.
     *
     * @return a map containing the unsigned long field type parser
     */
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(UnsignedLongFieldMapper.CONTENT_TYPE, UnsignedLongFieldMapper.PARSER);
    }

}
