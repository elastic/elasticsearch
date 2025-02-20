/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/**
 * A geo metric aggregator that computes a geo-centroid from a {@code geo_point} type field
 */
final class GeoCentroidAggregator extends MetricsAggregator {
    private final ValuesSource.GeoPoint valuesSource;
    private DoubleArray lonSum, lonCompensations, latSum, latCompensations;
    private LongArray counts;

    GeoCentroidAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (ValuesSource.GeoPoint) valuesSourceConfig.getValuesSource();
        lonSum = bigArrays().newDoubleArray(1, true);
        lonCompensations = bigArrays().newDoubleArray(1, true);
        latSum = bigArrays().newDoubleArray(1, true);
        latCompensations = bigArrays().newDoubleArray(1, true);
        counts = bigArrays().newLongArray(1, true);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        final MultiGeoPointValues values = valuesSource.geoPointValues(aggCtx.getLeafReaderContext());
        final GeoPointValues singleton = FieldData.unwrapSingleton(values);
        return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub);
    }

    private LeafBucketCollector getLeafCollector(MultiGeoPointValues values, LeafBucketCollector sub) {
        final CompensatedSum compensatedSumLat = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumLon = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    growBucket(bucket);
                    final int valueCount = values.docValueCount();
                    // increment by the number of points for this document
                    counts.increment(bucket, valueCount);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    compensatedSumLat.reset(latSum.get(bucket), latCompensations.get(bucket));
                    compensatedSumLon.reset(lonSum.get(bucket), lonCompensations.get(bucket));
                    // update the sum
                    for (int i = 0; i < valueCount; ++i) {
                        final GeoPoint value = values.nextValue();
                        // latitude
                        compensatedSumLat.add(value.getLat());
                        // longitude
                        compensatedSumLon.add(value.getLon());
                    }
                    lonSum.set(bucket, compensatedSumLon.value());
                    lonCompensations.set(bucket, compensatedSumLon.delta());
                    latSum.set(bucket, compensatedSumLat.value());
                    latCompensations.set(bucket, compensatedSumLat.delta());
                }
            }
        };
    }

    private LeafBucketCollector getLeafCollector(GeoPointValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    growBucket(bucket);
                    // increment by the number of points for this document
                    counts.increment(bucket, 1);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    final GeoPoint value = values.pointValue();
                    SumAggregator.computeSum(bucket, value.getLat(), latSum, latCompensations);
                    SumAggregator.computeSum(bucket, value.getLon(), lonSum, lonCompensations);
                }
            }
        };
    }

    private void growBucket(long bucket) {
        if (bucket >= latSum.size()) {
            final long newSize = bucket + 1;
            latSum = bigArrays().grow(latSum, newSize);
            lonSum = bigArrays().grow(lonSum, newSize);
            lonCompensations = bigArrays().grow(lonCompensations, newSize);
            latCompensations = bigArrays().grow(latCompensations, newSize);
            counts = bigArrays().grow(counts, newSize);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        final long bucketCount = counts.get(bucket);
        final GeoPoint bucketCentroid = (bucketCount > 0)
            ? new GeoPoint(latSum.get(bucket) / bucketCount, lonSum.get(bucket) / bucketCount)
            : null;
        return new InternalGeoCentroid(name, bucketCentroid, bucketCount, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalGeoCentroid.empty(name, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(latSum, latCompensations, lonSum, lonCompensations, counts);
    }
}
