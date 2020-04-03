/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinMaxAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSource;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSourceType;

/**
 * Utility class providing static methods to register aggregators for the aggregate_metric values source
 */
public class AggregateMetricsAggregatorsRegistrar {

    /**
     * Register the Sum aggregator
     */
    public static void registerSumAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(
            SumAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) (
                name,
                valuesSource,
                formatter,
                context,
                parent,
                pipelineAggregators,
                metaData) -> new AggregateMetricBackedSumAggregator(
                    name,
                    (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    formatter,
                    context,
                    parent,
                    pipelineAggregators,
                    metaData
                )
        );
    }

    /**
     * Register the Average aggregator
     */
    public static void registerAvgAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(
            AvgAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) (
                name,
                valuesSource,
                formatter,
                context,
                parent,
                pipelineAggregators,
                metaData) -> new AggregateMetricBackedAvgAggregator(
                    name,
                    (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    formatter,
                    context,
                    parent,
                    pipelineAggregators,
                    metaData
                )
        );
    }

    /**
     * Register the Min aggregator
     */
    public static void registerMinAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(
            MinAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MinMaxAggregatorSupplier) (
                name,
                valuesSourceConfig,
                valuesSource,
                context,
                parent,
                pipelineAggregators,
                metaData) -> new AggregateMetricBackedMinAggregator(
                    name,
                    valuesSourceConfig,
                    (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    context,
                    parent,
                    pipelineAggregators,
                    metaData
                )
        );
    }

    /**
     * Register the Max aggregator
     */
    public static void registerMaxAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(
            MaxAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MinMaxAggregatorSupplier) (
                name,
                config,
                valuesSource,
                context,
                parent,
                pipelineAggregators,
                metaData) -> new AggregateMetricBackedMaxAggregator(
                    name,
                    config,
                    (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    context,
                    parent,
                    pipelineAggregators,
                    metaData
                )
        );
    }

    /**
     * Register the ValueCount aggregator
     */
    public static void registerValueCountAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(
            ValueCountAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) (
                name,
                valuesSource,
                formatter,
                context,
                parent,
                pipelineAggregators,
                metaData) -> new AggregateMetricBackedValueCountAggregator(
                    name,
                    (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    context,
                    parent,
                    pipelineAggregators,
                    metaData
                )
        );
    }
}
