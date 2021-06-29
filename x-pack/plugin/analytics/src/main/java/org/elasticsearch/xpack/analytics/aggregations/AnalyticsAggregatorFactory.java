/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations;

import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.analytics.aggregations.bucket.histogram.HistoBackedHistogramAggregator;
import org.elasticsearch.xpack.analytics.aggregations.bucket.range.HistoBackedRangeAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedAvgAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedHDRPercentileRanksAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedHDRPercentilesAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedMaxAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedMinAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedSumAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedTDigestPercentileRanksAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedTDigestPercentilesAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedValueCountAggregator;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

public class AnalyticsAggregatorFactory {

    public static void registerPercentilesAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(PercentilesAggregationBuilder.REGISTRY_KEY,
            AnalyticsValuesSourceType.HISTOGRAM,
            (name, valuesSource, context, parent, percents, percentilesConfig, keyed, formatter, metadata) -> {
                if (percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
                    double compression = ((PercentilesConfig.TDigest)percentilesConfig).getCompression();
                    return new HistoBackedTDigestPercentilesAggregator(name, valuesSource, context, parent,
                        percents, compression, keyed, formatter, metadata);

                } else if (percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
                    int numSigFig = ((PercentilesConfig.Hdr)percentilesConfig).getNumberOfSignificantValueDigits();
                    return new HistoBackedHDRPercentilesAggregator(name, valuesSource, context, parent,
                        percents, numSigFig, keyed, formatter, metadata);
                }

                throw new IllegalArgumentException("Percentiles algorithm: [" + percentilesConfig.getMethod().toString() + "] " +
                    "is not compatible with Histogram field");
            }, true);
    }

    public static void registerPercentileRanksAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(PercentileRanksAggregationBuilder.REGISTRY_KEY,
            AnalyticsValuesSourceType.HISTOGRAM,
            (name, valuesSource, context, parent, percents, percentilesConfig, keyed, formatter, metadata) -> {
                if (percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
                    double compression = ((PercentilesConfig.TDigest)percentilesConfig).getCompression();
                    return new HistoBackedTDigestPercentileRanksAggregator(name, valuesSource, context, parent,
                        percents, compression, keyed, formatter, metadata);

                } else if (percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
                    int numSigFig = ((PercentilesConfig.Hdr)percentilesConfig).getNumberOfSignificantValueDigits();
                    return new HistoBackedHDRPercentileRanksAggregator(name, valuesSource, context, parent,
                        percents, numSigFig, keyed, formatter, metadata);
                }

                throw new IllegalArgumentException("Percentiles algorithm: [" + percentilesConfig.getMethod().toString() + "] " +
                    "is not compatible with Histogram field");
            }, true);
    }

    public static void registerHistoBackedSumAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(SumAggregationBuilder.REGISTRY_KEY, AnalyticsValuesSourceType.HISTOGRAM, HistoBackedSumAggregator::new, true);
    }

    public static void registerHistoBackedValueCountAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(ValueCountAggregationBuilder.REGISTRY_KEY,
            AnalyticsValuesSourceType.HISTOGRAM,
            HistoBackedValueCountAggregator::new,
                true);
    }

    public static void registerHistoBackedAverageAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(AvgAggregationBuilder.REGISTRY_KEY, AnalyticsValuesSourceType.HISTOGRAM, HistoBackedAvgAggregator::new, true);
    }

    public static void registerHistoBackedHistogramAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(HistogramAggregationBuilder.REGISTRY_KEY,
            AnalyticsValuesSourceType.HISTOGRAM,
            HistoBackedHistogramAggregator::new,
                true);
    }

    public static void registerHistoBackedMinggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(MinAggregationBuilder.REGISTRY_KEY, AnalyticsValuesSourceType.HISTOGRAM, HistoBackedMinAggregator::new, true);
    }

    public static void registerHistoBackedMaxggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(MaxAggregationBuilder.REGISTRY_KEY, AnalyticsValuesSourceType.HISTOGRAM, HistoBackedMaxAggregator::new, true);
    }

    public static void registerHistoBackedRangeAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            RangeAggregationBuilder.REGISTRY_KEY,
            AnalyticsValuesSourceType.HISTOGRAM,
            HistoBackedRangeAggregator::build,
            true
        );
    }

}
