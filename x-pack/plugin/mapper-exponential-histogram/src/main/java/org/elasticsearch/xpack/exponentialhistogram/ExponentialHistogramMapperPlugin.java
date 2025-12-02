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
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.ExponentialHistogramAggregatorsRegistrar;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Plugin adding support for exponential histogram field types.
 */
public class ExponentialHistogramMapperPlugin extends Plugin implements MapperPlugin, SearchPlugin {
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(ExponentialHistogramFieldMapper.CONTENT_TYPE, ExponentialHistogramFieldMapper.PARSER);
    }

    @Override
    public List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return List.of(
            ExponentialHistogramAggregatorsRegistrar::registerValueCountAggregator,
            ExponentialHistogramAggregatorsRegistrar::registerSumAggregator,
            ExponentialHistogramAggregatorsRegistrar::registerAvgAggregator,
            ExponentialHistogramAggregatorsRegistrar::registerHistogramAggregator
        );
    }
}
