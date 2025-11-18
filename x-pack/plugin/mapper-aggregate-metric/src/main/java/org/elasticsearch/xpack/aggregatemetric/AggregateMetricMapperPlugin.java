/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.aggregatemetric.aggregations.metrics.AggregateMetricsAggregatorsRegistrar;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;

public class AggregateMetricMapperPlugin extends Plugin implements MapperPlugin, ActionPlugin, SearchPlugin, ExtensiblePlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(AggregateMetricDoubleFieldMapper.CONTENT_TYPE, AggregateMetricDoubleFieldMapper.PARSER);
    }

    @Override
    public List<ActionHandler> getActions() {
        return Arrays.asList(
            new ActionHandler(XPackUsageFeatureAction.AGGREGATE_METRIC, AggregateMetricUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.AGGREGATE_METRIC, AggregateMetricInfoTransportAction.class)
        );
    }

    @Override
    public List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return List.of(
            AggregateMetricsAggregatorsRegistrar::registerSumAggregator,
            AggregateMetricsAggregatorsRegistrar::registerAvgAggregator,
            AggregateMetricsAggregatorsRegistrar::registerMinAggregator,
            AggregateMetricsAggregatorsRegistrar::registerMaxAggregator,
            AggregateMetricsAggregatorsRegistrar::registerValueCountAggregator
        );
    }
}
