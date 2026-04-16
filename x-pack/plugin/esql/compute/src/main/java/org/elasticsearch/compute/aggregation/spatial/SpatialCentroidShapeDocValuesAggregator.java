/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;

/**
 * This aggregator calculates the centroid of a set of shapes (geo_shape or cartesian_shape).
 * It is assumed that the input is a multi-valued double field containing 4 values per document:
 * [x, y, weight, shapeTypeOrdinal] as extracted from the doc-values centroid header.
 * This requires that the planner has planned that shapes are loaded from doc-values with
 * EXTRACT_SPATIAL_CENTROID preference.
 */
@Aggregator(
    {
        @IntermediateState(name = "xVal", type = "DOUBLE"),
        @IntermediateState(name = "xDel", type = "DOUBLE"),
        @IntermediateState(name = "yVal", type = "DOUBLE"),
        @IntermediateState(name = "yDel", type = "DOUBLE"),
        @IntermediateState(name = "weight", type = "DOUBLE"),
        @IntermediateState(name = "shapeType", type = "INT") }
)
@GroupingAggregator
class SpatialCentroidShapeDocValuesAggregator extends CentroidShapeAggregator {
    /**
     * Combines a multi-valued double block containing [x, y, weight, shapeTypeOrdinal] into the state.
     * The centroid coordinates are weighted before being added to the state.
     */
    public static void combine(ShapeCentroidState current, @Position int p, DoubleBlock values) {
        if (values.getValueCount(p) == 0) {
            return;
        }
        int start = values.getFirstValueIndex(p);
        // We expect exactly 4 values: [x, y, weight, shapeTypeOrdinal]
        double x = values.getDouble(start);
        double y = values.getDouble(start + 1);
        double weight = values.getDouble(start + 2);
        int shapeTypeOrdinal = (int) values.getDouble(start + 3);

        if (weight > 0) {
            DimensionalShapeType shapeType = DimensionalShapeType.fromOrdinalByte((byte) shapeTypeOrdinal);
            // The centroid values from doc-values are the actual centroid coordinates.
            // We need to multiply by weight to get weighted sums for aggregation.
            current.add(x * weight, 0d, y * weight, 0d, weight, shapeType);
        }
    }

    /**
     * Combines a multi-valued double block containing [x, y, weight, shapeTypeOrdinal] into the grouping state.
     */
    public static void combine(GroupingShapeCentroidState current, int groupId, @Position int p, DoubleBlock values) {
        if (values.isNull(p)) {
            return;
        }
        int start = values.getFirstValueIndex(p);
        // We expect exactly 4 values: [x, y, weight, shapeTypeOrdinal]
        double x = values.getDouble(start);
        double y = values.getDouble(start + 1);
        double weight = values.getDouble(start + 2);
        int shapeTypeOrdinal = (int) values.getDouble(start + 3);

        if (weight > 0) {
            DimensionalShapeType shapeType = DimensionalShapeType.fromOrdinalByte((byte) shapeTypeOrdinal);
            // The centroid values from doc-values are the actual centroid coordinates.
            // We need to multiply by weight to get weighted sums for aggregation.
            current.add(x * weight, 0d, y * weight, 0d, weight, shapeType, groupId);
        }
    }
}
