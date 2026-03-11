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

/**
 * Computes the extent of a set of cartesian shapes from the combined bounds-and-centroid format.
 * The bounds are the first 4 values as doubles (representing encoded integers):
 * [minX, maxX, maxY, minY], the same order as found in the constructor of the Rectangle class.
 * This requires that the planner has planned that shapes are loaded from doc-values with
 * EXTRACT_SPATIAL_BOUNDS_AND_CENTROID preference.
 */
@Aggregator(
    {
        @IntermediateState(name = "minX", type = "INT"),
        @IntermediateState(name = "maxX", type = "INT"),
        @IntermediateState(name = "maxY", type = "INT"),
        @IntermediateState(name = "minY", type = "INT") }
)
@GroupingAggregator
class SpatialExtentCartesianShapeCombinedDocValuesAggregator extends SpatialExtentAggregator {
    public static SpatialExtentState initSingle() {
        return new SpatialExtentState(PointType.CARTESIAN);
    }

    public static SpatialExtentGroupingState initGrouping() {
        return new SpatialExtentGroupingState(PointType.CARTESIAN);
    }

    public static void combine(SpatialExtentState current, @Position int p, DoubleBlock values) {
        if (values.getValueCount(p) == 0) {
            return;
        }
        int start = values.getFirstValueIndex(p);
        // Read the first 4 values as the bounds (stored as doubles but representing encoded ints)
        // Order is: minX, maxX, maxY (top), minY (bottom) as stored in Rectangle
        int minX = (int) values.getDouble(start);
        int maxX = (int) values.getDouble(start + 1);
        int maxY = (int) values.getDouble(start + 2);  // top
        int minY = (int) values.getDouble(start + 3);  // bottom
        current.add(minX, maxX, maxY, minY);
    }

    public static void combine(SpatialExtentGroupingState current, int groupId, @Position int p, DoubleBlock values) {
        if (values.isNull(p)) {
            return;
        }
        int start = values.getFirstValueIndex(p);
        // Read the first 4 values as the bounds (stored as doubles but representing encoded ints)
        // Order is: minX, maxX, maxY (top), minY (bottom) as stored in Rectangle
        int minX = (int) values.getDouble(start);
        int maxX = (int) values.getDouble(start + 1);
        int maxY = (int) values.getDouble(start + 2);  // top
        int minY = (int) values.getDouble(start + 3);  // bottom
        current.add(groupId, minX, maxX, maxY, minY);
    }
}
