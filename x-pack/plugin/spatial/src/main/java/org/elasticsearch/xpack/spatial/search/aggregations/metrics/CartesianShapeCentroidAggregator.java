/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * A cartesian metric aggregator that computes a cartesian-centroid from a {@code shape} type field
 * TODO: This can be generalized with the nearly identical class GeoShapeCentroidAggregator
 */
public final class CartesianShapeCentroidAggregator extends MetricsAggregator {
    private final CartesianShapeValuesSource valuesSource;
    private DoubleArray lonSum, lonCompensations, latSum, latCompensations, weightSum, weightCompensations;
    private LongArray counts;
    private ByteArray dimensionalShapeTypes;

    public CartesianShapeCentroidAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (CartesianShapeValuesSource) valuesSourceConfig.getValuesSource();
        lonSum = bigArrays().newDoubleArray(1, true);
        lonCompensations = bigArrays().newDoubleArray(1, true);
        latSum = bigArrays().newDoubleArray(1, true);
        latCompensations = bigArrays().newDoubleArray(1, true);
        weightSum = bigArrays().newDoubleArray(1, true);
        weightCompensations = bigArrays().newDoubleArray(1, true);
        counts = bigArrays().newLongArray(1, true);
        dimensionalShapeTypes = bigArrays().newByteArray(1, true);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        final CartesianShapeValues values = valuesSource.shapeValues(aggCtx.getLeafReaderContext());
        final CompensatedSum compensatedSumLat = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumLon = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumWeight = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeResize(bucket);
                    // increment by the number of points for this document
                    counts.increment(bucket, 1);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    final DimensionalShapeType shapeType = DimensionalShapeType.fromOrdinalByte(dimensionalShapeTypes.get(bucket));
                    final CartesianShapeValues.CartesianShapeValue value = values.value();
                    final int compares = shapeType.compareTo(value.dimensionalShapeType());
                    // update the sum
                    if (compares < 0) {
                        // shape with higher dimensional value
                        final double coordinateWeight = value.weight();
                        compensatedSumLat.reset(coordinateWeight * value.getY(), 0.0);
                        compensatedSumLon.reset(coordinateWeight * value.getX(), 0.0);
                        compensatedSumWeight.reset(coordinateWeight, 0.0);
                        dimensionalShapeTypes.set(bucket, (byte) value.dimensionalShapeType().ordinal());
                    } else if (compares == 0) {
                        // shape with the same dimensional value
                        compensatedSumLat.reset(latSum.get(bucket), latCompensations.get(bucket));
                        compensatedSumLon.reset(lonSum.get(bucket), lonCompensations.get(bucket));
                        compensatedSumWeight.reset(weightSum.get(bucket), weightCompensations.get(bucket));
                        final double coordinateWeight = value.weight();
                        compensatedSumLat.add(coordinateWeight * value.getY());
                        compensatedSumLon.add(coordinateWeight * value.getX());
                        compensatedSumWeight.add(coordinateWeight);
                    } else {
                        // do not modify centroid calculation since shape is of lower dimension than the running dimension
                        return;
                    }
                    lonSum.set(bucket, compensatedSumLon.value());
                    lonCompensations.set(bucket, compensatedSumLon.delta());
                    latSum.set(bucket, compensatedSumLat.value());
                    latCompensations.set(bucket, compensatedSumLat.delta());
                    weightSum.set(bucket, compensatedSumWeight.value());
                    weightCompensations.set(bucket, compensatedSumWeight.delta());
                }
            }

            private void maybeResize(long bucket) {
                latSum = bigArrays().grow(latSum, bucket + 1);
                lonSum = bigArrays().grow(lonSum, bucket + 1);
                weightSum = bigArrays().grow(weightSum, bucket + 1);
                lonCompensations = bigArrays().grow(lonCompensations, bucket + 1);
                latCompensations = bigArrays().grow(latCompensations, bucket + 1);
                weightCompensations = bigArrays().grow(weightCompensations, bucket + 1);
                counts = bigArrays().grow(counts, bucket + 1);
                dimensionalShapeTypes = bigArrays().grow(dimensionalShapeTypes, bucket + 1);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        final long bucketCount = counts.get(bucket);
        final double bucketWeight = weightSum.get(bucket);
        final CartesianPoint bucketCentroid = (bucketWeight > 0)
            ? new CartesianPoint(lonSum.get(bucket) / bucketWeight, latSum.get(bucket) / bucketWeight)
            : null;
        return new InternalCartesianCentroid(name, bucketCentroid, bucketCount, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalGeoCentroid.empty(name, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(
            latSum,
            latCompensations,
            lonSum,
            lonCompensations,
            counts,
            weightSum,
            weightCompensations,
            dimensionalShapeTypes
        );
    }
}
