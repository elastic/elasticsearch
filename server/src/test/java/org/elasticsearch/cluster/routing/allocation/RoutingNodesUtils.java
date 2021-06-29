/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;

public class RoutingNodesUtils {

    public static int numberOfShardsOfType(RoutingNodes nodes, ShardRoutingState state) {
        int count = 0;
        for (RoutingNode routingNode : nodes) {
            count += routingNode.numberOfShardsWithState(state);
        }
        return count;
    }
}
