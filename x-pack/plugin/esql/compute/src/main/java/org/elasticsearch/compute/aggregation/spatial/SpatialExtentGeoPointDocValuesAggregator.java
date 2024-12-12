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
 * Computes the extent of a set of geo points. It is assumed the points are encoded as longs.
 * This requires that the planner has planned that points are loaded from the index as doc-values.
 */
@Aggregator(
    {
        @IntermediateState(name = "minNegX", type = "INT"),
        @IntermediateState(name = "minPosX", type = "INT"),
        @IntermediateState(name = "maxNegX", type = "INT"),
        @IntermediateState(name = "maxPosX", type = "INT"),
        @IntermediateState(name = "maxY", type = "INT"),
        @IntermediateState(name = "minY", type = "INT") }
)
@GroupingAggregator
class SpatialExtentGeoPointDocValuesAggregator extends SpatialExtentLongitudeWrappingAggregator {
    // TODO support non-longitude wrapped geo shapes.
    public static SpatialExtentStateWrappedLongitudeState initSingle() {
        return new SpatialExtentStateWrappedLongitudeState();
    }

    public static SpatialExtentGroupingStateWrappedLongitudeState initGrouping() {
        return new SpatialExtentGroupingStateWrappedLongitudeState();
    }

    public static void combine(SpatialExtentStateWrappedLongitudeState current, long encoded) {
        current.add(encoded);
    }

    public static void combine(SpatialExtentGroupingStateWrappedLongitudeState current, int groupId, long encoded) {
        current.add(groupId, encoded);
    }
}
