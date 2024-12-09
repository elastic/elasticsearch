/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;

/**
 * Computes the extent of a set of cartesian shapes. It is assumed that the cartesian points are encoded as WKB BytesRef.
 * We do not currently support reading shape values or extents from doc values.
 * This is also used for final aggregations and aggregations in the coordinator node,
 * even if the local node partial aggregation is done with {@link SpatialStExtentCartesianPointSourceValuesAggregator}.
 */
@Aggregator(
    {
        @IntermediateState(name = "minX", type = "INT"),
        @IntermediateState(name = "maxX", type = "INT"),
        @IntermediateState(name = "maxY", type = "INT"),
        @IntermediateState(name = "minY", type = "INT") }
)
@GroupingAggregator
class SpatialStExtentCartesianShapeAggregator extends StExtentAggregator {
    public static StExtentState initSingle() {
        return new StExtentState(PointType.CARTESIAN);
    }

    public static StExtentGroupingState initGrouping() {
        return new StExtentGroupingState(PointType.CARTESIAN);
    }

    public static void combine(StExtentState current, BytesRef bytes) {
        current.add(SpatialAggregationUtils.decode(bytes));
    }

    public static void combine(StExtentGroupingState current, int groupId, BytesRef bytes) {
        current.add(groupId, SpatialAggregationUtils.decode(bytes));
    }
}
