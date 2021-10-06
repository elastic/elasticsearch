/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
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
        // TODO: Stop expecting nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.GeoPoint) valuesSourceConfig.getValuesSource() : null;
        if (valuesSource != null) {
            lonSum = bigArrays().newDoubleArray(1, true);
            lonCompensations = bigArrays().newDoubleArray(1, true);
            latSum = bigArrays().newDoubleArray(1, true);
            latCompensations = bigArrays().newDoubleArray(1, true);
            counts = bigArrays().newLongArray(1, true);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        final CompensatedSum compensatedSumLat = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumLon = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                latSum = bigArrays().grow(latSum, bucket + 1);
                lonSum = bigArrays().grow(lonSum, bucket + 1);
                lonCompensations = bigArrays().grow(lonCompensations, bucket + 1);
                latCompensations = bigArrays().grow(latCompensations, bucket + 1);
                counts = bigArrays().grow(counts, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    // increment by the number of points for this document
                    counts.increment(bucket, valueCount);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sumLat = latSum.get(bucket);
                    double compensationLat = latCompensations.get(bucket);
                    double sumLon = lonSum.get(bucket);
                    double compensationLon = lonCompensations.get(bucket);

                    compensatedSumLat.reset(sumLat, compensationLat);
                    compensatedSumLon.reset(sumLon, compensationLon);

                    // update the sum
                    for (int i = 0; i < valueCount; ++i) {
                        GeoPoint value = values.nextValue();
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

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= counts.size()) {
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
        return new InternalGeoCentroid(name, null, 0L, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(latSum, latCompensations, lonSum, lonCompensations, counts);
    }
}
