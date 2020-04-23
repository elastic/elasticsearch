/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

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
            (MetricAggregatorSupplier) HistoBackedSumAggregator::new);
    }
}
