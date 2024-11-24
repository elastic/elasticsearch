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

/**
 * This aggregator calculates the centroid of a set of cartesian points.
 * It is assumes that the cartesian points are encoded as WKB BytesRef.
 * This requires that the planner has NOT planned that points are loaded from the index as doc-values, but from source instead.
 * This is also used for final aggregations and aggregations in the coordinator node,
 * even if the local node partial aggregation is done with {@link SpatialStExtentCartesianShapeAggregator}.
 */
@Aggregator({ @IntermediateState(name = "extent", type = "BYTES_REF") })
@GroupingAggregator
class SpatialStExtentCartesianShapeAggregator extends StExtentAggregator {
    public static void combine(StExtentState current, BytesRef wkb) {
        current.add(SpatialAggregationUtils.decode(wkb));
    }

    public static void combine(GroupingStExtentState current, int groupId, BytesRef wkb) {
        current.add(groupId, SpatialAggregationUtils.decode(wkb));
    }
}
