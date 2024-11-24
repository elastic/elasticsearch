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
import org.elasticsearch.index.mapper.vectors.DenormalizedCosineFloatVectorValues;

import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.decodeLatitude;
import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.decodeLongitude;

/**
 * This aggregator calculates the centroid of a set of geo points. It is assumes that the geo points are encoded as longs.
 * This requires that the planner has planned that points are loaded from the index as doc-values.
 */
@Aggregator({ @IntermediateState(name = "extent", type = "BYTES_REF") })
@GroupingAggregator
class SpatialStExtentGeoPointDocValuesAggregator extends StExtentAggregator {
    public static void combine(StExtentState current, long encoded) {
        current.add(decodePoint(encoded));
    }

    public static void combine(GroupingStExtentState current, int groupId, long encoded) {
        current.add(groupId, decodePoint(encoded));
    }

    private static Point decodePoint(long encoded) {
        return new Point(decodeLongitude(encoded), decodeLatitude(encoded));
    }
}
