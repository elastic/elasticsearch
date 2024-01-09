/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

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
        @IntermediateState(name = "yDel", type = "DOUBLE"),
        @IntermediateState(name = "count", type = "LONG") }
)
@GroupingAggregator
class SpatialCentroidGeoPointAggregator extends CentroidPointAggregator {
    private static final CentroidPointAggregator.Encoder GEO_ENCODER = new CentroidPointAggregator.Encoder(
        (x, y) -> (long) GeoEncodingUtils.encodeLatitude(y) << 32 | (long) GeoEncodingUtils.encodeLongitude(x),
        (encoded) -> {
            double y = GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32));
            double x = GeoEncodingUtils.decodeLongitude((int) (encoded & 0xFFFFFFFFL));
            return new Point(x, y);
        }
    );

    public static CentroidState initSingle() {
        return new CentroidState(GEO_ENCODER);
    }

    public static GroupingCentroidState initGrouping(BigArrays bigArrays) {
        return new GroupingCentroidState(bigArrays, GEO_ENCODER);
    }
}
