/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.elasticsearch.plugins.SearchPlugin.AggregationExtension;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.util.List;

public class AnalyticsPercentilesAggregatorFactory {
    private static final PercentilesAggregatorSupplier PERCENTILES =
        (name, valuesSource, context, parent, percents, percentilesConfig, keyed, formatter, pipelineAggregators, metadata) -> {
            if (percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
                double compression = ((PercentilesConfig.TDigest)percentilesConfig).getCompression();
                return new HistoBackedTDigestPercentilesAggregator(name, valuesSource, context, parent,
                    percents, compression, keyed, formatter, pipelineAggregators, metadata);
            }
            if (percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
                int numSigFig = ((PercentilesConfig.Hdr)percentilesConfig).getNumberOfSignificantValueDigits();
                return new HistoBackedHDRPercentilesAggregator(name, valuesSource, context, parent,
                    percents, numSigFig, keyed, formatter, pipelineAggregators, metadata);
            }

            throw new IllegalArgumentException("Percentiles algorithm: [" + percentilesConfig.getMethod().toString() + "] " +
                "is not compatible with Histogram field");
        };
    private static final PercentilesAggregatorSupplier PERCENTILE_RANKS =
        (name, valuesSource, context, parent, percents, percentilesConfig, keyed, formatter, pipelineAggregators, metadata) -> {
            if (percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
                double compression = ((PercentilesConfig.TDigest)percentilesConfig).getCompression();
                return new HistoBackedTDigestPercentileRanksAggregator(name, valuesSource, context, parent,
                    percents, compression, keyed, formatter, pipelineAggregators, metadata);
            }
            if (percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
                int numSigFig = ((PercentilesConfig.Hdr)percentilesConfig).getNumberOfSignificantValueDigits();
                return new HistoBackedHDRPercentileRanksAggregator(name, valuesSource, context, parent,
                    percents, numSigFig, keyed, formatter, pipelineAggregators, metadata);
            }

            throw new IllegalArgumentException("Percentiles algorithm: [" + percentilesConfig.getMethod().toString() + "] " +
                "is not compatible with Histogram field");
        };

    public static final List<AggregationExtension> EXTENSIONS = List.of(
        new AggregationExtension(PercentilesAggregationBuilder.NAME,
            spec -> spec.implementFor(PERCENTILES, AnalyticsValuesSourceType.HISTOGRAM)),
        new AggregationExtension(PercentileRanksAggregationBuilder.NAME,
            spec -> spec.implementFor(PERCENTILE_RANKS, AnalyticsValuesSourceType.HISTOGRAM))
    );
}
