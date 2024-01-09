/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.geometry.Point;

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
class SpatialCentroidCartesianPointAggregator extends CentroidPointAggregator {
    private static final Encoder CARTESIAN_ENCODER = new Encoder((x, y) -> {
        final long xi = XYEncodingUtils.encode(x.floatValue());
        final long yi = XYEncodingUtils.encode(y.floatValue());
        return (yi & 0xFFFFFFFFL) | xi << 32;
    }, (encoded) -> {
        final double x = XYEncodingUtils.decode((int) (encoded >>> 32));
        final double y = XYEncodingUtils.decode((int) (encoded & 0xFFFFFFFFL));
        return new Point(x, y);
    });

    public static CentroidState initSingle() {
        return new CentroidState(CARTESIAN_ENCODER);
    }

    public static GroupingCentroidState initGrouping(BigArrays bigArrays) {
        return new GroupingCentroidState(bigArrays, CARTESIAN_ENCODER);
    }
}
