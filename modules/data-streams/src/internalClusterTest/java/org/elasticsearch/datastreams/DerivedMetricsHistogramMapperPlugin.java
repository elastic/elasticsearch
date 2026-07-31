/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramFieldMapper;

import java.util.Map;

/**
 * Registers just the {@code exponential_histogram} mapper, which the derived metrics destination template maps {@code metric.histogram}
 * as.
 *
 * <p>In a real cluster the mapper comes from {@code x-pack-analytics}, which is always bundled in the default distribution. Loading that
 * whole plugin here would drag in {@code x-pack-core} for stats actions and licensing that have nothing to do with what is under test, so
 * this registers the one mapper the destination needs and nothing else.
 */
public class DerivedMetricsHistogramMapperPlugin extends Plugin implements MapperPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(ExponentialHistogramFieldMapper.CONTENT_TYPE, ExponentialHistogramFieldMapper.PARSER);
    }
}
