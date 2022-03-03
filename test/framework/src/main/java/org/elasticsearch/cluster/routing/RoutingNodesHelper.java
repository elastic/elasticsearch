/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import java.util.ArrayList;
import java.util.List;

public final class RoutingNodesHelper {

    private RoutingNodesHelper() {}

    public static List<ShardRouting> shardsWithState(RoutingNodes routingNodes, ShardRoutingState... state) {
        List<ShardRouting> shards = new ArrayList<>();
        for (RoutingNode routingNode : routingNodes) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                routingNodes.unassigned().forEach(shards::add);
                break;
            }
        }
        return shards;
    }

    public static List<ShardRouting> shardsWithState(RoutingNodes routingNodes, String index, ShardRoutingState... state) {
        List<ShardRouting> shards = new ArrayList<>();
        for (RoutingNode routingNode : routingNodes) {
            shards.addAll(routingNode.shardsWithState(index, state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                for (ShardRouting unassignedShard : routingNodes.unassigned()) {
                    if (unassignedShard.index().getName().equals(index)) {
                        shards.add(unassignedShard);
                    }
                }
                break;
            }
        }
        return shards;
    }

}
