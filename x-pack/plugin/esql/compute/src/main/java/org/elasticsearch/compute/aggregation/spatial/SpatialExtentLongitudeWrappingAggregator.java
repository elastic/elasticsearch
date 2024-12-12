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
abstract class SpatialExtentLongitudeWrappingAggregator {
    public static void combineIntermediate(
        SpatialExtentStateWrappedLongitudeState current,
        int minNegX,
        int minPosX,
        int maxNegX,
        int maxPosX,
        int maxY,
        int minY
    ) {
        current.add(minNegX, minPosX, maxNegX, maxPosX, maxY, minY);
    }

    public static void combineIntermediate(
        SpatialExtentGroupingStateWrappedLongitudeState current,
        int groupId,
        int minNegX,
        int minPosX,
        int maxNegX,
        int maxPosX,
        int maxY,
        int minY
    ) {
        current.add(groupId, minNegX, minPosX, maxNegX, maxPosX, maxY, minY);
    }

    public static Block evaluateFinal(SpatialExtentStateWrappedLongitudeState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    public static Block evaluateFinal(
        SpatialExtentGroupingStateWrappedLongitudeState state,
        IntVector selected,
        DriverContext driverContext
    ) {
        return state.toBlock(selected, driverContext);
    }

    public static void combineStates(
        SpatialExtentGroupingStateWrappedLongitudeState current,
        int groupId,
        SpatialExtentGroupingStateWrappedLongitudeState inState,
        int inPosition
    ) {
        current.add(groupId, inState, inPosition);
    }
}
