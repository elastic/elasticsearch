/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Plugin adding support for exponential histogram field types.
 */
public class ExponentialHistogramMapperPlugin extends Plugin implements MapperPlugin {
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        if (ExponentialHistogramFieldMapper.EXPONENTIAL_HISTOGRAM_FEATURE.isEnabled()) {
            mappers.put(ExponentialHistogramFieldMapper.CONTENT_TYPE, ExponentialHistogramFieldMapper.PARSER);
        }
        return Collections.unmodifiableMap(mappers);
    }
}
