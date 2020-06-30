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
            SumAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) AggregateMetricBackedSumAggregator::new
        );
    }

    public static void registerAvgAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            AvgAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) AggregateMetricBackedAvgAggregator::new
        );
    }

    public static void registerMinAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MinAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) AggregateMetricBackedMinAggregator::new
        );
    }

    public static void registerMaxAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MaxAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) AggregateMetricBackedMaxAggregator::new
        );
    }

    public static void registerValueCountAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            ValueCountAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) AggregateMetricBackedValueCountAggregator::new
        );
    }
}
