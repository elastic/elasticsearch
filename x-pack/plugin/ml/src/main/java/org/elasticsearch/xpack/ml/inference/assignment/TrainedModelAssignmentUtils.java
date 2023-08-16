/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfoUpdate;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingStateAndReason;

public class TrainedModelAssignmentUtils {
    public static final String NODES_CHANGED_REASON = "nodes changed";
    public static final String NODE_IS_SHUTTING_DOWN = "node is shutting down";

    public static RoutingInfo createShuttingDownRoute(RoutingInfo existingRoute) {
        RoutingInfoUpdate routeUpdate = RoutingInfoUpdate.updateStateAndReason(
            new RoutingStateAndReason(RoutingState.STOPPING, NODE_IS_SHUTTING_DOWN)
        );

        return routeUpdate.apply(existingRoute);
    }

    private TrainedModelAssignmentUtils() {}
}
