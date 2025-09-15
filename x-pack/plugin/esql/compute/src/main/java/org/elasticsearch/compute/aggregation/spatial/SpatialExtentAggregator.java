/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

// A bit of abuse of notation here, since we're extending this class to "inherit" its static methods.
// Unfortunately, this is the way it has to be done, since the generated code invokes these methods statically.
abstract class SpatialExtentAggregator {
    public static void combineIntermediate(SpatialExtentState current, int minX, int maxX, int maxY, int minY) {
        current.add(minX, maxX, maxY, minY);
    }

    public static void combineIntermediate(SpatialExtentGroupingState current, int groupId, int minX, int maxX, int maxY, int minY) {
        current.add(groupId, minX, maxX, maxY, minY);
    }

    public static Block evaluateFinal(SpatialExtentState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    public static Block evaluateFinal(SpatialExtentGroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(selected, driverContext);
    }

    public static void combineStates(SpatialExtentGroupingState current, int groupId, SpatialExtentGroupingState inState, int inPosition) {
        current.add(groupId, inState, inPosition);
    }
}
