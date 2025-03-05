/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
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

public class SumAggregator extends NumericMetricsAggregator.SingleDoubleValue {

    protected final DocValueFormat format;
    protected DoubleArray sums;
    protected DoubleArray compensations;

    SumAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.format = valuesSourceConfig.format();
        var bigArrays = context.bigArrays();
        sums = bigArrays.newDoubleArray(1, true);
        compensations = bigArrays.newDoubleArray(1, true);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    sumSortedDoubles(bucket, values, sums, compensations);
                }
            }
        };
    }

    // returns number of values added
    static int sumSortedDoubles(long bucket, SortedNumericDoubleValues values, DoubleArray sums, DoubleArray compensations)
        throws IOException {
        final int valueCount = values.docValueCount();
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        double value = sums.get(bucket);
        double delta = compensations.get(bucket);
        for (int i = 0; i < valueCount; i++) {
            double added = values.nextValue();
            value = addIfNonOrInf(added, value);
            if (Double.isFinite(value)) {
                double correctedSum = added + delta;
                double updatedValue = value + correctedSum;
                delta = correctedSum - (updatedValue - value);
                value = updatedValue;
            }
        }
        compensations.set(bucket, delta);
        sums.set(bucket, value);
        return valueCount;
    }

    private static double addIfNonOrInf(double added, double value) {
        // If the value is Inf or NaN, just add it to the running tally to "convert" to
        // Inf/NaN. This keeps the behavior bwc from before kahan summing
        if (Double.isFinite(added)) {
            return value;
        }
        return added + value;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    computeSum(bucket, values.doubleValue(), sums, compensations);
                }
            }
        };
    }

    static void computeSum(long bucket, double added, DoubleArray sums, DoubleArray compensations) {
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        double value = addIfNonOrInf(added, sums.get(bucket));
        if (Double.isFinite(value)) {
            double delta = compensations.get(bucket);
            double correctedSum = added + delta;
            double updatedValue = value + correctedSum;
            delta = correctedSum - (updatedValue - value);
            value = updatedValue;
            compensations.set(bucket, delta);
        }

        sums.set(bucket, value);
    }

    protected final void maybeGrow(long bucket) {
        if (bucket >= sums.size()) {
            doGrow(bucket, bigArrays());
        }
    }

    protected void doGrow(long bucket, BigArrays bigArrays) {
        sums = bigArrays.grow(sums, bucket + 1);
        compensations = bigArrays.grow(compensations, bucket + 1);
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
