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
import org.elasticsearch.geometry.Point;

/**
 * This aggregator calculates the centroid of a set of geo points.
 * It is assumes that the geo points are encoded as WKB BytesRef.
 * This requires that the planner has NOT planned that points are loaded from the index as doc-values, but from source instead.
 * This is also used for final aggregations and aggregations in the coordinator node,
 * even if the local node partial aggregation is done with {@link SpatialCentroidGeoPointDocValuesAggregator}.
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
class SpatialCentroidGeoPointSourceValuesAggregator extends CentroidPointAggregator {
    public static void combine(CentroidState current, BytesRef wkb) {
        Point point = SpatialAggregationUtils.decodePoint(wkb);
        current.add(point.getX(), point.getY());
    }

    public static void combine(GroupingCentroidState current, int groupId, BytesRef wkb) {
        Point point = SpatialAggregationUtils.decodePoint(wkb);
        current.add(point.getX(), 0d, point.getY(), 0d, 1, groupId);
    }
}
