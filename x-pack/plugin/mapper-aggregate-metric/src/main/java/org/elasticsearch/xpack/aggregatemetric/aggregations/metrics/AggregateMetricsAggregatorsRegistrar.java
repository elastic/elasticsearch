/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSourceType;

/**
 * Utility class providing static methods to register aggregators for the aggregate_metric values source
 */
public class AggregateMetricsAggregatorsRegistrar {

    public static void registerSumAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            SumAggregationBuilder.REGISTRY_KEY,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            AggregateMetricBackedSumAggregator::new,
            true
        );
    }

    public static void registerAvgAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            AvgAggregationBuilder.REGISTRY_KEY,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            AggregateMetricBackedAvgAggregator::new,
            true
        );
    }

    public static void registerMinAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MinAggregationBuilder.REGISTRY_KEY,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            AggregateMetricBackedMinAggregator::new,
            true
        );
    }

    public static void registerMaxAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MaxAggregationBuilder.REGISTRY_KEY,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            AggregateMetricBackedMaxAggregator::new,
            true
        );
    }

    public static void registerValueCountAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            ValueCountAggregationBuilder.REGISTRY_KEY,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            AggregateMetricBackedValueCountAggregator::new,
            true
        );
    }
}
