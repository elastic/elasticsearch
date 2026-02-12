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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.lucene.spatial.CentroidCalculator;

/**
 * This aggregator calculates the centroid of a set of shapes (geo_shape or cartesian_shape).
 * It is assumed that the shapes are encoded as WKB BytesRef.
 * This requires that the planner has NOT planned that shapes are loaded from the index as doc-values, but from source instead.
 * This is also used for final aggregations and aggregations in the coordinator node.
 */
@Aggregator(
    {
        @IntermediateState(name = "xVal", type = "DOUBLE"),
        @IntermediateState(name = "xDel", type = "DOUBLE"),
        @IntermediateState(name = "yVal", type = "DOUBLE"),
        @IntermediateState(name = "yDel", type = "DOUBLE"),
        @IntermediateState(name = "weight", type = "DOUBLE"),
        @IntermediateState(name = "shapeType", type = "INT") }
)
@GroupingAggregator
class SpatialCentroidShapeSourceValuesAggregator extends CentroidShapeAggregator {
    public static void combine(ShapeCentroidState current, BytesRef wkb) {
        Geometry geometry = SpatialAggregationUtils.decode(wkb);
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(geometry);
        double weight = calculator.sumWeight();
        if (weight > 0) {
            // CentroidCalculator returns weighted x and y sums, so we need to pass them as-is
            // The final centroid is computed as xSum/weight, ySum/weight
            current.add(calculator.getX() * weight, 0d, calculator.getY() * weight, 0d, weight, calculator.getDimensionalShapeType());
        }
    }

    public static void combine(GroupingShapeCentroidState current, int groupId, BytesRef wkb) {
        Geometry geometry = SpatialAggregationUtils.decode(wkb);
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(geometry);
        double weight = calculator.sumWeight();
        if (weight > 0) {
            current.add(
                calculator.getX() * weight,
                0d,
                calculator.getY() * weight,
                0d,
                weight,
                calculator.getDimensionalShapeType(),
                groupId
            );
        }
    }
}
