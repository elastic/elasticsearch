/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.DimensionalShapeType;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A geo metric aggregator that computes a geo-centroid from a {@code geo_point} type field
 */
final class GeoCentroidAggregator extends MetricsAggregator {
    private final ValuesSource.Geo valuesSource;
    private DoubleArray lonSum, lonCompensations, latSum, latCompensations, weightSum, weightCompensations;
    private LongArray counts;
    private ByteArray dimensionalShapeTypes;

    GeoCentroidAggregator(String name, SearchContext context, Aggregator parent,
                          ValuesSource.Geo valuesSource, List<PipelineAggregator> pipelineAggregators,
                          Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            lonSum = bigArrays.newDoubleArray(1, true);
            lonCompensations = bigArrays.newDoubleArray(1, true);
            latSum = bigArrays.newDoubleArray(1, true);
            latCompensations = bigArrays.newDoubleArray(1, true);
            weightSum = bigArrays.newDoubleArray(1, true);
            weightCompensations = bigArrays.newDoubleArray(1, true);
            counts = bigArrays.newLongArray(1, true);
            dimensionalShapeTypes = bigArrays.newByteArray(1, true);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final MultiGeoValues values = valuesSource.geoValues(ctx);
        final CompensatedSum compensatedSumLat = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumLon = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumWeight = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                latSum = bigArrays.grow(latSum, bucket + 1);
                lonSum = bigArrays.grow(lonSum, bucket + 1);
                weightSum = bigArrays.grow(weightSum, bucket + 1);
                lonCompensations = bigArrays.grow(lonCompensations, bucket + 1);
                latCompensations = bigArrays.grow(latCompensations, bucket + 1);
                weightCompensations = bigArrays.grow(weightCompensations, bucket + 1);
                counts = bigArrays.grow(counts, bucket + 1);
                dimensionalShapeTypes = bigArrays.grow(dimensionalShapeTypes, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    // increment by the number of points for this document
                    counts.increment(bucket, valueCount);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    DimensionalShapeType shapeType = DimensionalShapeType.fromOrdinalByte(dimensionalShapeTypes.get(bucket));
                    double sumLat = latSum.get(bucket);
                    double compensationLat = latCompensations.get(bucket);
                    double sumLon = lonSum.get(bucket);
                    double compensationLon = lonCompensations.get(bucket);
                    double sumWeight = weightSum.get(bucket);
                    double compensatedWeight = weightCompensations.get(bucket);

                    compensatedSumLat.reset(sumLat, compensationLat);
                    compensatedSumLon.reset(sumLon, compensationLon);
                    compensatedSumWeight.reset(sumWeight, compensatedWeight);

                    // update the sum
                    for (int i = 0; i < valueCount; ++i) {
                        MultiGeoValues.GeoValue value = values.nextValue();
                        int compares = shapeType.compareTo(value.dimensionalShapeType());
                        if (compares < 0) {
                            double coordinateWeight = value.weight();
                            compensatedSumLat.reset(coordinateWeight * value.lat(), 0.0);
                            compensatedSumLon.reset(coordinateWeight * value.lon(), 0.0);
                            compensatedSumWeight.reset(coordinateWeight, 0.0);
                            dimensionalShapeTypes.set(bucket, (byte) value.dimensionalShapeType().ordinal());
                        } else if (compares == 0) {
                            double coordinateWeight = value.weight();
                            // weighted latitude
                            compensatedSumLat.add(coordinateWeight * value.lat());
                            // weighted longitude
                            compensatedSumLon.add(coordinateWeight * value.lon());
                            // weight
                            compensatedSumWeight.add(coordinateWeight);
                        }
                        // else (compares > 0)
                        //   do not modify centroid calculation since shape is of lower dimension than the running dimension

                    }
                    lonSum.set(bucket, compensatedSumLon.value());
                    lonCompensations.set(bucket, compensatedSumLon.delta());
                    latSum.set(bucket, compensatedSumLat.value());
                    latCompensations.set(bucket, compensatedSumLat.delta());
                    weightSum.set(bucket, compensatedSumWeight.value());
                    weightCompensations.set(bucket, compensatedSumWeight.delta());
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
        final double bucketWeight = weightSum.get(bucket);
        final GeoPoint bucketCentroid = (bucketWeight > 0)
            ? new GeoPoint(latSum.get(bucket) / bucketWeight, lonSum.get(bucket) / bucketWeight)
            : null;
        return new InternalGeoCentroid(name, bucketCentroid , bucketCount, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoCentroid(name, null, 0L, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(latSum, latCompensations, lonSum, lonCompensations, counts, weightSum, weightCompensations,
            dimensionalShapeTypes);
    }
}
