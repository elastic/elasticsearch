/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroid;

import java.io.IOException;
import java.util.Map;

/**
 * Serialization and merge logic for {@link GeoShapeCentroidAggregator}.
 *
 * <p>Unlike {@link InternalGeoCentroid} (which uses document count for cross-shard weighting,
 * correct only for {@code geo_point}), this class carries the raw area-weighted coordinate sums
 * so that cross-shard reduction divides by total area weight rather than total document count.
 *
 * <p>It also tracks the {@link DimensionalShapeType} so that cross-shard reduction respects
 * dimension priority: when mixing POINT, LINE, and POLYGON results from different shards,
 * only the highest-dimensional type contributes to the final centroid.
 */
public class InternalGeoShapeCentroid extends InternalGeoCentroid {

    public static final String NAME = "geo_centroid_shape";

    private final double latWeightedSum;
    private final double lonWeightedSum;
    private final double totalWeight;
    private final DimensionalShapeType shapeType;

    /**
     * Creates a new result, computing the centroid as {@code (latWeightedSum/totalWeight, lonWeightedSum/totalWeight)}.
     *
     * @param latWeightedSum sum of {@code (lat_i * weight_i)} across all shapes in this shard/bucket
     * @param lonWeightedSum sum of {@code (lon_i * weight_i)} across all shapes in this shard/bucket
     * @param totalWeight    sum of {@code weight_i} (e.g. area for polygons, length for lines, 1 for points)
     * @param docCount       number of documents contributing to this bucket
     * @param shapeType      the highest dimensional shape type seen in this shard/bucket
     */
    public InternalGeoShapeCentroid(
        String name,
        double latWeightedSum,
        double lonWeightedSum,
        double totalWeight,
        long docCount,
        DimensionalShapeType shapeType,
        Map<String, Object> metadata
    ) {
        super(
            name,
            docCount > 0 && totalWeight > 0 ? new GeoPoint(latWeightedSum / totalWeight, lonWeightedSum / totalWeight) : null,
            docCount,
            metadata
        );
        this.latWeightedSum = latWeightedSum;
        this.lonWeightedSum = lonWeightedSum;
        this.totalWeight = totalWeight;
        this.shapeType = shapeType;
    }

    public InternalGeoShapeCentroid(StreamInput in) throws IOException {
        super(in);
        this.latWeightedSum = in.readDouble();
        this.lonWeightedSum = in.readDouble();
        this.totalWeight = in.readDouble();
        this.shapeType = DimensionalShapeType.fromOrdinalByte(in.readByte());
    }

    public static InternalGeoShapeCentroid empty(String name, Map<String, Object> metadata) {
        return new InternalGeoShapeCentroid(name, Double.NaN, Double.NaN, 0, 0, DimensionalShapeType.POINT, metadata);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeDouble(latWeightedSum);
        out.writeDouble(lonWeightedSum);
        out.writeDouble(totalWeight);
        out.writeByte((byte) shapeType.ordinal());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {

            double combinedLatWeightedSum = 0;
            double combinedLonWeightedSum = 0;
            double combinedWeight = 0;
            long totalCount = 0;
            DimensionalShapeType combinedShapeType = DimensionalShapeType.POINT;
            boolean hasValues = false;

            @Override
            public void accept(InternalAggregation aggregation) {
                if (aggregation instanceof InternalGeoShapeCentroid shapeAgg) {
                    if (shapeAgg.count > 0 && shapeAgg.totalWeight > 0) {
                        int cmp = shapeAgg.shapeType.compareTo(combinedShapeType);
                        if (hasValues == false || cmp > 0) {
                            // First value or higher dimension — reset
                            combinedLatWeightedSum = shapeAgg.latWeightedSum;
                            combinedLonWeightedSum = shapeAgg.lonWeightedSum;
                            combinedWeight = shapeAgg.totalWeight;
                            totalCount = shapeAgg.count;
                            combinedShapeType = shapeAgg.shapeType;
                            hasValues = true;
                        } else if (cmp == 0) {
                            // Same dimension — accumulate
                            combinedLatWeightedSum += shapeAgg.latWeightedSum;
                            combinedLonWeightedSum += shapeAgg.lonWeightedSum;
                            combinedWeight += shapeAgg.totalWeight;
                            totalCount += shapeAgg.count;
                        }
                        // cmp < 0: lower dimension — ignore
                    }
                } else {
                    // BWC: old node sent InternalGeoCentroid without raw weighted sums or shape type.
                    // Fall back to count-weighted combination — no dimensional type info available.
                    InternalGeoCentroid centroidAgg = (InternalGeoCentroid) aggregation;
                    if (centroidAgg.count() > 0 && centroidAgg.centroid() != null) {
                        final double w = centroidAgg.count();
                        if (hasValues == false) {
                            combinedLatWeightedSum = w * centroidAgg.centroid().getY();
                            combinedLonWeightedSum = w * centroidAgg.centroid().getX();
                            combinedWeight = w;
                            totalCount = centroidAgg.count();
                            hasValues = true;
                        } else {
                            combinedLatWeightedSum += w * centroidAgg.centroid().getY();
                            combinedLonWeightedSum += w * centroidAgg.centroid().getX();
                            combinedWeight += w;
                            totalCount += centroidAgg.count();
                        }
                    }
                }
            }

            @Override
            public InternalAggregation get() {
                if (hasValues == false) {
                    return InternalGeoShapeCentroid.empty(name, getMetadata());
                }
                return new InternalGeoShapeCentroid(
                    name,
                    combinedLatWeightedSum,
                    combinedLonWeightedSum,
                    combinedWeight,
                    totalCount,
                    combinedShapeType,
                    getMetadata()
                );
            }
        };
    }

    @Override
    protected InternalGeoShapeCentroid copyWith(SpatialPoint result, long count) {
        if (result == null || count == 0) {
            return InternalGeoShapeCentroid.empty(name, getMetadata());
        }
        final double scaleFactor = this.count > 0 ? (double) count / this.count : 1.0;
        return new InternalGeoShapeCentroid(
            name,
            latWeightedSum * scaleFactor,
            lonWeightedSum * scaleFactor,
            totalWeight * scaleFactor,
            count,
            shapeType,
            getMetadata()
        );
    }

    @Override
    protected InternalGeoShapeCentroid copyWith(double firstSum, double secondSum, long totalCount) {
        // Called only by the parent's getLeaderReducer, which we override; defensive implementation.
        if (Double.isNaN(firstSum) || totalCount == 0) {
            return InternalGeoShapeCentroid.empty(name, getMetadata());
        }
        return new InternalGeoShapeCentroid(name, firstSum, secondSum, totalCount, totalCount, shapeType, getMetadata());
    }
}
