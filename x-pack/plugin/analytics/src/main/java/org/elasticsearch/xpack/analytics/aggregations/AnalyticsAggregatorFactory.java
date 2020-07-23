/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.aggregations;

import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.analytics.aggregations.bucket.histogram.HistoBackedHistogramAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HllBackedCardinalityAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedAvgAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedHDRPercentileRanksAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedHDRPercentilesAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedSumAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedTDigestPercentileRanksAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedTDigestPercentilesAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HistoBackedValueCountAggregator;
import org.elasticsearch.xpack.analytics.aggregations.metrics.HyperLogLog;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.HllFieldMapper;

public class AnalyticsAggregatorFactory {

    public static void registerPercentilesAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(PercentilesAggregationBuilder.NAME,
            AnalyticsValuesSourceType.HISTOGRAM,
            (PercentilesAggregatorSupplier) (name, valuesSource, context, parent, percents, percentilesConfig, keyed,
                                             formatter, metadata) -> {

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
            });
    }

    public static void registerPercentileRanksAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(PercentileRanksAggregationBuilder.NAME,
            AnalyticsValuesSourceType.HISTOGRAM,
            (PercentilesAggregatorSupplier) (name, valuesSource, context, parent, percents, percentilesConfig, keyed,
                                                      formatter, metadata) -> {

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
            });
    }

    public static void registerHistoBackedSumAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(SumAggregationBuilder.NAME,
            AnalyticsValuesSourceType.HISTOGRAM,
            (MetricAggregatorSupplier) HistoBackedSumAggregator::new
        );
    }

    public static void registerHistoBackedValueCountAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(ValueCountAggregationBuilder.NAME,
            AnalyticsValuesSourceType.HISTOGRAM,
            (MetricAggregatorSupplier) HistoBackedValueCountAggregator::new
        );
    }

    public static void registerHistoBackedAverageAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(AvgAggregationBuilder.NAME,
            AnalyticsValuesSourceType.HISTOGRAM,
            (MetricAggregatorSupplier) HistoBackedAvgAggregator::new
        );
    }

    public static void registerHistoBackedHistogramAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(HistogramAggregationBuilder.NAME,
            AnalyticsValuesSourceType.HISTOGRAM,
            (HistogramAggregatorSupplier) HistoBackedHistogramAggregator::new
        );
    }

    public static void registerCardinalityBackedCardinalityAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(CardinalityAggregationBuilder.NAME,
            AnalyticsValuesSourceType.CARDINALITY,
            (CardinalityAggregatorSupplier) (name, valuesSource, precision, context, parent, metadata) -> {
                HllFieldMapper.HllFieldType fieldType = (HllFieldMapper.HllFieldType) valuesSource.fieldType();
                if (fieldType.precision() >= precision) {
                    return new HllBackedCardinalityAggregator(name, valuesSource, precision,
                        fieldType.precision(), context, parent, metadata);
                }
                throw new IllegalArgumentException("Cardinality aggregation precision ["  + precision + "] " +
                    "is not compatible with field precision [" + fieldType.precision() + "]. Precision threshold must " +
                    "be lower or equal than [" + HyperLogLog.thresholdFromPrecision(fieldType.precision()) + "]");
            });
    }
}
