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
 * Computes the extent of a set of cartesian points. It is assumed the points are encoded as longs.
 * This requires that the planner has planned that points are loaded from the index as doc-values.
 */
@Aggregator(
    {
        @IntermediateState(name = "minX", type = "INT"),
        @IntermediateState(name = "maxX", type = "INT"),
        @IntermediateState(name = "maxY", type = "INT"),
        @IntermediateState(name = "minY", type = "INT") }
)
@GroupingAggregator
class SpatialExtentCartesianPointDocValuesAggregator extends SpatialExtentAggregator {
    public static SpatialExtentState initSingle() {
        return new SpatialExtentState(PointType.CARTESIAN);
    }

    public static SpatialExtentGroupingState initGrouping() {
        return new SpatialExtentGroupingState(PointType.CARTESIAN);
    }

    public static void combine(SpatialExtentState current, long v) {
        current.add(v);
    }

    public static void combine(SpatialExtentGroupingState current, int groupId, long v) {
        current.add(groupId, v);
    }
}
