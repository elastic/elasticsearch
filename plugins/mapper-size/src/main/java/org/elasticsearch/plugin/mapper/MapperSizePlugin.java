/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.mapper;

import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.size.SizeFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.Map;

/**
 * Elasticsearch plugin that provides the _size metadata field mapper.
 * The _size field stores the size in bytes of the original _source field,
 * which can be useful for monitoring and managing document sizes.
 */
public class MapperSizePlugin extends Plugin implements MapperPlugin {

    /**
     * Provides the _size metadata field mapper for tracking document source sizes.
     *
     * @return a map containing the _size field mapper parser
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * PUT my-index
     * {
     *   "mappings": {
     *     "_size": {
     *       "enabled": true
     *     }
     *   }
     * }
     *
     * // Query documents by size
     * GET my-index/_search
     * {
     *   "query": {
     *     "range": {
     *       "_size": {
     *         "gte": 1000
     *       }
     *     }
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.singletonMap(SizeFieldMapper.NAME, SizeFieldMapper.PARSER);
    }
}
