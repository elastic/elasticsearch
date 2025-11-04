/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations;

import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.bucket.histogram.ExponentialHistogramBackedHistogramAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics.ExponentialHistogramAvgAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics.ExponentialHistogramSumAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics.ExponentialHistogramValueCountAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.support.ExponentialHistogramValuesSourceType;

/**
 * Utility class providing static methods to register aggregators for the aggregate_metric values source
 */
public class ExponentialHistogramAggregatorsRegistrar {

    public static void registerValueCountAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            ValueCountAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM,
            ExponentialHistogramValueCountAggregator::new,
            true
        );
    }

    public static void registerSumAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            SumAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM,
            ExponentialHistogramSumAggregator::new,
            true
        );
    }

    public static void registerAvgAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            AvgAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM,
            ExponentialHistogramAvgAggregator::new,
            true
        );
    }

    public static void registerHistogramAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            HistogramAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM,
            ExponentialHistogramBackedHistogramAggregator::new,
            true
        );
    }
}
