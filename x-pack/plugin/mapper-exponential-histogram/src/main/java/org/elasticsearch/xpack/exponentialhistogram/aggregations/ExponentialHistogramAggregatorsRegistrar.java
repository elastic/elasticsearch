/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations;

import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.bucket.histogram.ExponentialHistogramBackedHistogramAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics.ExponentialHistogramAvgAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics.ExponentialHistogramMaxAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics.ExponentialHistogramMinAggregator;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics.ExponentialHistogramPercentilesAggregator;
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

    public static void registerMinAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MinAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM,
            ExponentialHistogramMinAggregator::new,
            true
        );
    }

    public static void registerMaxAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MaxAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM,
            ExponentialHistogramMaxAggregator::new,
            true
        );
    }

    public static void registerPercentilesAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            PercentilesAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM,
            (name, config, context, parent, percents, percentilesConfig, keyed, formatter, metadata) -> {
                if (percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
                    // We make sure to preserve the t-digest settings, as we might be querying mixed T-Digest / exponential histogram data
                    double compression = ((PercentilesConfig.TDigest) percentilesConfig).getCompression();
                    TDigestExecutionHint executionHint = ((PercentilesConfig.TDigest) percentilesConfig).getExecutionHint(context);
                    return new ExponentialHistogramPercentilesAggregator(
                        name,
                        config,
                        context,
                        parent,
                        percents,
                        compression,
                        executionHint,
                        keyed,
                        formatter,
                        metadata
                    );
                }
                throw new IllegalArgumentException(
                    "Percentiles algorithm "
                        + percentilesConfig.getMethod().toString()
                        + " must not be used with exponential_histogram field"
                );
            },
            true
        );
    }
}
