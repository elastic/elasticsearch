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
import org.elasticsearch.geometry.Point;

import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.decodeLatitude;
import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.decodeLongitude;
import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.decodeX;
import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.decodeY;

/**
 * This aggregator calculates the centroid of a set of geo points. It is assumes that the geo points are encoded as longs.
 * This requires that the planner has planned that points are loaded from the index as doc-values.
 */
@Aggregator(
    {
        @IntermediateState(name = "minX", type = "INT"),
        @IntermediateState(name = "maxX", type = "INT"),
        @IntermediateState(name = "maxY", type = "INT"),
        @IntermediateState(name = "minY", type = "INT") }
)
@GroupingAggregator
class SpatialStExtentGeoPointDocValuesAggregator extends StExtentAggregator {
    public static StExtentState initSingle() {
        return new StExtentState(PointType.GEO);
    }

    public static StExtentGroupingState initGrouping() {
        return new StExtentGroupingState(PointType.GEO);
    }

    public static void combine(StExtentState current, long encoded) {
        current.add(encoded);
    }

    public static void combine(StExtentGroupingState current, int groupId, long encoded) {
        current.add(groupId, encoded);
    }
}
