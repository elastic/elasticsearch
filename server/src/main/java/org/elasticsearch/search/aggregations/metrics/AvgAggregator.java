/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

class AvgAggregator extends NumericMetricsAggregator.SingleDoubleValue {

    LongArray counts;
    DoubleArray sums;
    DoubleArray compensations;
    DocValueFormat format;

    AvgAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.format = valuesSourceConfig.format();
        final BigArrays bigArrays = context.bigArrays();
        counts = bigArrays.newLongArray(1, true);
        sums = bigArrays.newDoubleArray(1, true);
        compensations = bigArrays.newDoubleArray(1, true);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, final LeafBucketCollector sub) {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    final int valueCount = values.docValueCount();
                    counts.increment(bucket, valueCount);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    kahanSummation.reset(sums.get(bucket), compensations.get(bucket));
                    for (int i = 0; i < valueCount; i++) {
                        kahanSummation.add(values.nextValue());
                    }
                    sums.set(bucket, kahanSummation.value());
                    compensations.set(bucket, kahanSummation.delta());
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, final LeafBucketCollector sub) {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    counts.increment(bucket, 1L);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    kahanSummation.reset(sums.get(bucket), compensations.get(bucket));
                    kahanSummation.add(values.doubleValue());
                    sums.set(bucket, kahanSummation.value());
                    compensations.set(bucket, kahanSummation.delta());
                }
            }
        };
    }

    private void maybeGrow(long bucket) {
        if (bucket >= counts.size()) {
            counts = bigArrays().grow(counts, bucket + 1);
            sums = bigArrays().grow(sums, bucket + 1);
            compensations = bigArrays().grow(compensations, bucket + 1);
        }
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= sums.size()) {
            return Double.NaN;
        }
        return sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalAvg(name, sums.get(bucket), counts.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalAvg.empty(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts, sums, compensations);
    }

}
