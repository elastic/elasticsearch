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
 * Computes the extent of a set of geo shapes read from doc-values, which means they are encoded as an array of integers.
 * This requires that the planner has planned that the shape extent is loaded from the index as doc-values.
 * The intermediate state is the extent of the shapes, encoded as six integers: top, bottom, negLeft, negRight, posLeft, posRight.
 * The order of the integers is the same as defined in the constructor of the Extent class,
 * as that is the order in which the values are stored in shape doc-values.
 * Note that this is very different from the four values used for the intermediate state of cartesian_shape geometries.
 */
@Aggregator(
    {
        @IntermediateState(name = "top", type = "INT"),
        @IntermediateState(name = "bottom", type = "INT"),
        @IntermediateState(name = "negLeft", type = "INT"),
        @IntermediateState(name = "negRight", type = "INT"),
        @IntermediateState(name = "posLeft", type = "INT"),
        @IntermediateState(name = "posRight", type = "INT") }
)
@GroupingAggregator
class SpatialExtentGeoShapeDocValuesAggregator extends SpatialExtentLongitudeWrappingAggregator {
    public static SpatialExtentStateWrappedLongitudeState initSingle() {
        return new SpatialExtentStateWrappedLongitudeState();
    }

    public static SpatialExtentGroupingStateWrappedLongitudeState initGrouping() {
        return new SpatialExtentGroupingStateWrappedLongitudeState();
    }

    public static void combine(SpatialExtentStateWrappedLongitudeState current, int[] values) {
        current.add(values);
    }

    public static void combine(SpatialExtentGroupingStateWrappedLongitudeState current, int groupId, int[] values) {
        current.add(groupId, values);
    }
}
