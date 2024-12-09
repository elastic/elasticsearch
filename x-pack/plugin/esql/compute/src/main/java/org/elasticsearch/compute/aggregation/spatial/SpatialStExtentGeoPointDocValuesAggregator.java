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
class SpatialStExtentGeoPointDocValuesAggregator extends StExtentLongitudeWrappingAggregator {
    // TODO support non-longitude wrapped geo shapes.
    public static StExtentStateWrappedLongitudeState initSingle() {
        return new StExtentStateWrappedLongitudeState();
    }

    public static StExtentGroupingStateWrappedLongitudeState initGrouping() {
        return new StExtentGroupingStateWrappedLongitudeState();
    }

    public static void combine(StExtentStateWrappedLongitudeState current, long encoded) {
        current.add(encoded);
    }

    public static void combine(StExtentGroupingStateWrappedLongitudeState current, int groupId, long encoded) {
        current.add(groupId, encoded);
    }
}
