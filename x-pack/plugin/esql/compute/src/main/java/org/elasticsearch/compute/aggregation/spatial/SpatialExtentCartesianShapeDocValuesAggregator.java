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

/**
 * Computes the extent of a set of cartesian shapes read from doc-values, which means they are encoded as an array of integers.
 * This requires that the planner has planned that the shape extent is loaded from the index as doc-values.
 * The intermediate state is the extent of the shapes, encoded as four integers: minX, maxX, maxY, minY.
 * The order of the integers is the same as defined in the constructor of the Rectangle class.
 * Note that this is very different from the six values used for the intermediate state of geo_shape geometries.
 */
@Aggregator(
    {
        @IntermediateState(name = "minX", type = "INT"),
        @IntermediateState(name = "maxX", type = "INT"),
        @IntermediateState(name = "maxY", type = "INT"),
        @IntermediateState(name = "minY", type = "INT") }
)
@GroupingAggregator
class SpatialExtentCartesianShapeDocValuesAggregator extends SpatialExtentAggregator {
    public static SpatialExtentState initSingle() {
        return new SpatialExtentState(PointType.CARTESIAN);
    }

    public static SpatialExtentGroupingState initGrouping() {
        return new SpatialExtentGroupingState(PointType.CARTESIAN);
    }

    public static void combine(SpatialExtentState current, int[] values) {
        current.add(values);
    }

    public static void combine(SpatialExtentGroupingState current, int groupId, int[] values) {
        current.add(groupId, values);
    }
}
