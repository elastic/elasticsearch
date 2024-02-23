/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder.VALUE_FIELD;

class WeightedAvgAggregatorFactory extends MultiValuesSourceAggregatorFactory {

    WeightedAvgAggregatorFactory(
        String name,
        Map<String, ValuesSourceConfig> configs,
        DocValueFormat format,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, configs, format, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalWeightedAvg empty = InternalWeightedAvg.empty(name, format, metadata);
        return new NonCollectingSingleMetricAggregator(name, context, parent, empty, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        Map<String, ValuesSourceConfig> configs,
        DocValueFormat format,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        MultiValuesSource.NumericMultiValuesSource numericMultiVS = new MultiValuesSource.NumericMultiValuesSource(configs);
        if (numericMultiVS.areValuesSourcesEmpty()) {
            return createUnmapped(parent, metadata);
        }
        return new WeightedAvgAggregator(name, numericMultiVS, format, context, parent, metadata);
    }

    @Override
    public String getStatsSubtype() {
        return configs.get(VALUE_FIELD.getPreferredName()).valueSourceType().typeName();
    }
}
