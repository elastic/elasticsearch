/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;

/**
 * This aggregator calculates the centroid of a set of cartesian points.
 * It is assumes that the cartesian points are encoded as longs.
 * This requires that the planner has planned that points are loaded from the index as doc-values.
 */
@Aggregator(
    {
        @IntermediateState(name = "xVal", type = "DOUBLE"),
        @IntermediateState(name = "xDel", type = "DOUBLE"),
        @IntermediateState(name = "yVal", type = "DOUBLE"),
        @IntermediateState(name = "yDel", type = "DOUBLE"),
        @IntermediateState(name = "count", type = "LONG") }
)
@GroupingAggregator
class SpatialCentroidCartesianPointDocValuesAggregator extends CentroidPointAggregator {

    public static CentroidState initSingle() {
        return new CentroidState();
    }

    public static GroupingCentroidState initGrouping(BigArrays bigArrays) {
        return new GroupingCentroidState(bigArrays);
    }

    public static void combine(CentroidState current, long v) {
        current.add(decodeX(v), decodeY(v));
    }

    public static void combine(GroupingCentroidState current, int groupId, long encoded) {
        current.add(decodeX(encoded), 0d, decodeY(encoded), 0d, 1, groupId);
    }

    private static double decodeX(long encoded) {
        return XYEncodingUtils.decode((int) (encoded >>> 32));
    }

    private static double decodeY(long encoded) {
        return XYEncodingUtils.decode((int) (encoded & 0xFFFFFFFFL));
    }
}
