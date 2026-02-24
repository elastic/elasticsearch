/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.support.ExponentialHistogramValuesSource;

import java.io.IOException;
import java.util.Map;

public final class ExponentialHistogramMaxAggregator extends NumericMetricsAggregator.SingleValue {

    private final ExponentialHistogramValuesSource.ExponentialHistogram valuesSource;
    private final DocValueFormat format;

    private DoubleArray maxs;

    public ExponentialHistogramMaxAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert config.hasValues();
        this.valuesSource = (ExponentialHistogramValuesSource.ExponentialHistogram) config.getValuesSource();
        maxs = bigArrays().newDoubleArray(1, false);
        maxs.fill(0, maxs.size(), Double.NEGATIVE_INFINITY);
        this.format = config.format();
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final ExponentialHistogramValuesReader values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= maxs.size()) {
                    long from = maxs.size();
                    maxs = bigArrays().grow(maxs, bucket + 1);
                    maxs.fill(from, maxs.size(), Double.NEGATIVE_INFINITY);
                }

                if (values.advanceExact(doc)) {
                    double max = Math.max(maxs.get(bucket), values.maxValue());
                    maxs.set(bucket, max);
                }

            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= maxs.size()) {
            return Double.NEGATIVE_INFINITY;
        }
        return maxs.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= maxs.size()) {
            return buildEmptyAggregation();
        }
        return new Max(name, maxs.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return Max.createEmptyMax(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(maxs);
    }

}
