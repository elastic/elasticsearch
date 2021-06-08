/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.aggregatemetric.aggregations.metrics.AggregateMetricsAggregatorsRegistrar;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;

public class AggregateMetricMapperPlugin extends Plugin implements MapperPlugin, ActionPlugin, SearchPlugin {

    private final boolean transportClientMode;

    public AggregateMetricMapperPlugin(Settings settings) {
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, AggregateMetricFeatureSet.class));
        return modules;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(AggregateDoubleMetricFieldMapper.CONTENT_TYPE, AggregateDoubleMetricFieldMapper.PARSER);
    }

    @Override
    public java.util.List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return org.elasticsearch.core.List.of(
            AggregateMetricsAggregatorsRegistrar::registerSumAggregator,
            AggregateMetricsAggregatorsRegistrar::registerAvgAggregator,
            AggregateMetricsAggregatorsRegistrar::registerMinAggregator,
            AggregateMetricsAggregatorsRegistrar::registerMaxAggregator,
            AggregateMetricsAggregatorsRegistrar::registerValueCountAggregator
        );
    }
}
