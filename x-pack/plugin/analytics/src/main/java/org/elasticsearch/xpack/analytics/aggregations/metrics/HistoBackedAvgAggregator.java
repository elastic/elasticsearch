/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Average aggregator operating over histogram datatypes {@link HistogramValuesSource}
 * The aggregation computes weighted average by taking counts into consideration for each value
 */
public class HistoBackedAvgAggregator extends NumericMetricsAggregator.SingleValue {

    private final HistogramValuesSource.Histogram valuesSource;

    LongArray counts;
    DoubleArray sums;
    DoubleArray compensations;
    DocValueFormat format;

    public HistoBackedAvgAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: Stop depending on nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? (HistogramValuesSource.Histogram) valuesSourceConfig.getValuesSource() : null;
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            counts = bigArrays().newLongArray(1, true);
            sums = bigArrays().newDoubleArray(1, true);
            compensations = bigArrays().newDoubleArray(1, true);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final HistogramValues values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays().grow(counts, bucket + 1);
                sums = bigArrays().grow(sums, bucket + 1);
                compensations = bigArrays().grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();

                    // Compute the sum of double values with Kahan summation algorithm which is more accurate than naive summation
                    final double sum = sums.get(bucket);
                    final double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);
                    while (sketch.next()) {
                        double d = sketch.value() * sketch.count();
                        kahanSummation.add(d);
                        counts.increment(bucket, sketch.count());
                    }

                    sums.set(bucket, kahanSummation.value());
                    compensations.set(bucket, kahanSummation.delta());
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return Double.NaN;
        }
        return sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalAvg(name, sums.get(bucket), counts.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalAvg(name, 0.0, 0L, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts, sums, compensations);
    }

}
