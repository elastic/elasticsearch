/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.geometry.Point;

/**
 * This aggregator calculates the centroid of a set of geo points. It is assumes that the geo points are encoded as longs.
 * This requires that the planner has planned that points are loaded from the index as doc-values.
 */
@Aggregator(
    {
        @IntermediateState(name = "xVal", type = "DOUBLE"),
        @IntermediateState(name = "xDel", type = "DOUBLE"),
        @IntermediateState(name = "yVal", type = "DOUBLE"),
        @IntermediateState(name = "yDel", type = "DOUBLE") }
)
@GroupingAggregator
class SpatialStExtentGeoPointDocValuesAggregator extends StExtentAggregator {
    public static StExtentState initSingle() {
        return new StExtentState();
    }

    public static GroupingStExtentState initGrouping(BigArrays bigArrays) {
        return new GroupingStExtentState(bigArrays);
    }

    public static void combine(StExtentState current, long v) {
        current.add(new Point(decodeX(v), decodeY(v)));
    }

    public static void combine(GroupingStExtentState current, int groupId, long encoded) {
        throw new AssertionError("TODO(gal)");
    }

    private static double decodeX(long encoded) {
        return GeoEncodingUtils.decodeLongitude((int) (encoded & 0xFFFFFFFFL));
    }

    private static double decodeY(long encoded) {
        return GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32));
    }
}
