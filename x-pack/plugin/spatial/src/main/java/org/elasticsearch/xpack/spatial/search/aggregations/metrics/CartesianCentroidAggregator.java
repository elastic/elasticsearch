/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * A metric aggregator that computes a cartesian-centroid from a {@code point} type field
 */
public final class CartesianCentroidAggregator extends MetricsAggregator {
    private final CartesianPointValuesSource valuesSource;
    private DoubleArray xSum, xCompensations, ySum, yCompensations;
    private LongArray counts;

    public CartesianCentroidAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (CartesianPointValuesSource) valuesSourceConfig.getValuesSource();
        xSum = bigArrays().newDoubleArray(1, true);
        xCompensations = bigArrays().newDoubleArray(1, true);
        ySum = bigArrays().newDoubleArray(1, true);
        yCompensations = bigArrays().newDoubleArray(1, true);
        counts = bigArrays().newLongArray(1, true);

    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        final CartesianPointValuesSource.MultiCartesianPointValues values = valuesSource.pointValues(aggCtx.getLeafReaderContext());
        final CompensatedSum compensatedSumX = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumY = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                xSum = bigArrays().grow(xSum, bucket + 1);
                ySum = bigArrays().grow(ySum, bucket + 1);
                xCompensations = bigArrays().grow(xCompensations, bucket + 1);
                yCompensations = bigArrays().grow(yCompensations, bucket + 1);
                counts = bigArrays().grow(counts, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    // increment by the number of points for this document
                    counts.increment(bucket, valueCount);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sumX = xSum.get(bucket);
                    double compensationX = xCompensations.get(bucket);
                    double sumY = ySum.get(bucket);
                    double compensationY = yCompensations.get(bucket);

                    compensatedSumX.reset(sumX, compensationX);
                    compensatedSumY.reset(sumY, compensationY);

                    // update the sum
                    for (int i = 0; i < valueCount; ++i) {
                        CartesianPoint value = values.nextValue();
                        // x / longitude
                        compensatedSumX.add(value.getX());
                        // y / latitude
                        compensatedSumY.add(value.getY());
                    }
                    xSum.set(bucket, compensatedSumX.value());
                    xCompensations.set(bucket, compensatedSumX.delta());
                    ySum.set(bucket, compensatedSumY.value());
                    yCompensations.set(bucket, compensatedSumY.delta());
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        final long bucketCount = counts.get(bucket);
        final CartesianPoint bucketCentroid = (bucketCount > 0)
            ? new CartesianPoint(xSum.get(bucket) / bucketCount, ySum.get(bucket) / bucketCount)
            : null;
        return new InternalCartesianCentroid(name, bucketCentroid, bucketCount, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalCartesianCentroid.empty(name, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(xSum, xCompensations, ySum, yCompensations, counts);
    }
}
