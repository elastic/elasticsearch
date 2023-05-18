/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.DoubleArray;
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
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Sum aggregator operating over histogram datatypes {@link HistogramValuesSource}
 *
 * The aggregator sums each histogram value multiplied by its count.
 * Eg for a histogram of response times, this is an approximate "total time spent".
 */
public class HistoBackedSumAggregator extends NumericMetricsAggregator.SingleValue {

    private final HistogramValuesSource.Histogram valuesSource;
    private final DocValueFormat format;

    private DoubleArray sums;
    private DoubleArray compensations;

    public HistoBackedSumAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (HistogramValuesSource.Histogram) valuesSourceConfig.getValuesSource();
        this.format = valuesSourceConfig.format();
        sums = bigArrays().newDoubleArray(1, true);
        compensations = bigArrays().newDoubleArray(1, true);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final HistogramValues values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays().grow(sums, bucket + 1);
                compensations = bigArrays().grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();
                    final double sum = sums.get(bucket);
                    final double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);
                    while (sketch.next()) {
                        double d = sketch.value() * sketch.count();
                        kahanSummation.add(d);
                    }

                    compensations.set(bucket, kahanSummation.delta());
                    sums.set(bucket, kahanSummation.value());
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new Sum(name, sums.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return Sum.empty(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }
}
