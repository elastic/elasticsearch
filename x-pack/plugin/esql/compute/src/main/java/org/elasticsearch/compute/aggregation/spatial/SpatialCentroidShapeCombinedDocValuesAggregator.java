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
 * This aggregator calculates the centroid of a set of shapes (geo_shape or cartesian_shape)
 * from the combined bounds-and-centroid format.
 * It reads the last 4 values from a multi-valued double field: [x, y, weight, shapeTypeOrdinal]
 * which is the centroid portion of the combined format that also includes bounds.
 * This requires that the planner has planned that shapes are loaded from doc-values with
 * EXTRACT_SPATIAL_BOUNDS_AND_CENTROID preference.
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
class SpatialCentroidShapeCombinedDocValuesAggregator extends CentroidShapeAggregator {
    /**
     * Combines the centroid portion from a combined bounds+centroid block into the state.
     * The centroid data is the last 4 values: [x, y, weight, shapeTypeOrdinal].
     */
    public static void combine(ShapeCentroidState current, @Position int p, DoubleBlock values) {
        int valueCount = values.getValueCount(p);
        if (valueCount == 0) {
            return;
        }
        int start = values.getFirstValueIndex(p);
        // Centroid is the last 4 values of the block
        int centroidStart = start + valueCount - 4;
        double x = values.getDouble(centroidStart);
        double y = values.getDouble(centroidStart + 1);
        double weight = values.getDouble(centroidStart + 2);
        int shapeTypeOrdinal = (int) values.getDouble(centroidStart + 3);

        if (weight > 0) {
            DimensionalShapeType shapeType = DimensionalShapeType.fromOrdinalByte((byte) shapeTypeOrdinal);
            current.add(x * weight, 0d, y * weight, 0d, weight, shapeType);
        }
    }

    /**
     * Combines the centroid portion from a combined bounds+centroid block into the grouping state.
     */
    public static void combine(GroupingShapeCentroidState current, int groupId, @Position int p, DoubleBlock values) {
        if (values.isNull(p)) {
            return;
        }
        int valueCount = values.getValueCount(p);
        int start = values.getFirstValueIndex(p);
        // Centroid is the last 4 values of the block
        int centroidStart = start + valueCount - 4;
        double x = values.getDouble(centroidStart);
        double y = values.getDouble(centroidStart + 1);
        double weight = values.getDouble(centroidStart + 2);
        int shapeTypeOrdinal = (int) values.getDouble(centroidStart + 3);

        if (weight > 0) {
            DimensionalShapeType shapeType = DimensionalShapeType.fromOrdinalByte((byte) shapeTypeOrdinal);
            current.add(x * weight, 0d, y * weight, 0d, weight, shapeType, groupId);
        }
    }
}
