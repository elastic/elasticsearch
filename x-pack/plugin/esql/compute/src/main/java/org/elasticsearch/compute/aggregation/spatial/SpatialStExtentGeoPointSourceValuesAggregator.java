/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;

/**
 * This aggregator calculates the centroid of a set of geo points.
 * It is assumes that the geo points are encoded as WKB BytesRef.
 * This requires that the planner has NOT planned that points are loaded from the index as doc-values, but from source instead.
 * This is also used for final aggregations and aggregations in the coordinator node,
 * even if the local node partial aggregation is done with {@link SpatialStExtentGeoPointDocValuesAggregator}.
 */
@Aggregator(
    {
        @IntermediateState(name = "xVal", type = "DOUBLE"),
        @IntermediateState(name = "xDel", type = "DOUBLE"),
        @IntermediateState(name = "yVal", type = "DOUBLE"),
        @IntermediateState(name = "yDel", type = "DOUBLE") }
)
@GroupingAggregator
class SpatialStExtentGeoPointSourceValuesAggregator extends StExtentAggregator {
    public static StExtentState initSingle() {
        return new StExtentState();
    }

    public static GroupingStExtentState initGrouping(BigArrays bigArrays) {
        return new GroupingStExtentState(bigArrays);
    }

    public static void combine(StExtentState current, BytesRef wkb) {
        current.add(decode(wkb));
    }

    public static void combine(GroupingStExtentState current, int groupId, BytesRef wkb) {
        current.add(groupId, decode(wkb));
    }

    private static Geometry decode(BytesRef wkb) {
        return WellKnownBinary.fromWKB(GeometryValidator.NOOP, false /* coerce */, wkb.bytes, wkb.offset, wkb.length);
    }
}
