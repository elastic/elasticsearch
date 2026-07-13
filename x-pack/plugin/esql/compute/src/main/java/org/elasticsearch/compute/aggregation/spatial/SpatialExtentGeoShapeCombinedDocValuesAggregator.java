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
 * Computes the extent of a set of geo shapes from the combined bounds-and-centroid format.
 * The bounds are the first 6 values as doubles (representing encoded integers):
 * [top, bottom, negLeft, negRight, posLeft, posRight].
 * The order of the values is the same as defined in the constructor of the Extent class,
 * as that is the order in which the values are stored in shape doc-values.
 * This requires that the planner has planned that shapes are loaded from doc-values with
 * EXTRACT_SPATIAL_BOUNDS_AND_CENTROID preference.
 * The reason the integers are stored as doubles in the DoubleBlock is that they are loaded
 * from doc-values together with the centroid values, which themselves are stored as doubles.
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
class SpatialExtentGeoShapeCombinedDocValuesAggregator extends SpatialExtentLongitudeWrappingAggregator {
    public static SpatialExtentStateWrappedLongitudeState initSingle() {
        return new SpatialExtentStateWrappedLongitudeState();
    }

    public static SpatialExtentGroupingStateWrappedLongitudeState initGrouping() {
        return new SpatialExtentGroupingStateWrappedLongitudeState();
    }

    public static void combine(SpatialExtentStateWrappedLongitudeState current, @Position int p, DoubleBlock values) {
        if (values.getValueCount(p) == 0) {
            return;
        }
        int start = values.getFirstValueIndex(p);
        // Read the first 6 values as the bounds (stored as doubles but representing encoded ints)
        int top = (int) values.getDouble(start);
        int bottom = (int) values.getDouble(start + 1);
        int negLeft = (int) values.getDouble(start + 2);
        int negRight = (int) values.getDouble(start + 3);
        int posLeft = (int) values.getDouble(start + 4);
        int posRight = (int) values.getDouble(start + 5);
        current.add(top, bottom, negLeft, negRight, posLeft, posRight);
    }

    public static void combine(SpatialExtentGroupingStateWrappedLongitudeState current, int groupId, @Position int p, DoubleBlock values) {
        if (values.isNull(p)) {
            return;
        }
        int start = values.getFirstValueIndex(p);
        // Read the first 6 values as the bounds (stored as doubles but representing encoded ints)
        int top = (int) values.getDouble(start);
        int bottom = (int) values.getDouble(start + 1);
        int negLeft = (int) values.getDouble(start + 2);
        int negRight = (int) values.getDouble(start + 3);
        int posLeft = (int) values.getDouble(start + 4);
        int posRight = (int) values.getDouble(start + 5);
        current.add(groupId, top, bottom, negLeft, negRight, posLeft, posRight);
    }
}
