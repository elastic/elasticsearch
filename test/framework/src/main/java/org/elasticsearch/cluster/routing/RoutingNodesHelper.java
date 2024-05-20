/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;

public final class RoutingNodesHelper {

    private RoutingNodesHelper() {}

    public static int numberOfShardsWithState(RoutingNodes nodes, ShardRoutingState state) {
        assert state != ShardRoutingState.UNASSIGNED : "unassigned state is not supported here, use nodes.unassigned().size() instead";
        int count = 0;
        for (RoutingNode routingNode : nodes) {
            count += routingNode.numberOfShardsWithState(state);
        }
        return count;
    }

    public static List<ShardRouting> shardsWithState(RoutingNodes routingNodes, ShardRoutingState state) {
        return state == ShardRoutingState.UNASSIGNED
            ? iterableAsArrayList(routingNodes.unassigned())
            : routingNodes.stream().flatMap(routingNode -> routingNode.shardsWithState(state)).toList();
    }

    public static List<ShardRouting> shardsWithState(RoutingNodes routingNodes, String index, ShardRoutingState states) {
        return shardsWithState(routingNodes, states).stream()
            .filter(shardRouting -> Objects.equals(shardRouting.getIndexName(), index))
            .toList();
    }

    public static Stream<ShardRouting> assignedShardsIn(RoutingNodes routingNodes) {
        return routingNodes.stream().flatMap(RoutingNodesHelper::assignedShardsIn);
    }

    public static Stream<ShardRouting> assignedShardsIn(RoutingNode routingNode) {
        return StreamSupport.stream(routingNode.spliterator(), false);
    }

    /**
     * Returns a stream over all {@link ShardRouting} in a {@link IndexShardRoutingTable}. This is not part of production code on purpose
     * as its too costly to iterate the table like this in many production use cases.
     *
     * @param indexShardRoutingTable index shard routing table to iterate over
     * @return stream over {@link ShardRouting}
     */
    public static Stream<ShardRouting> asStream(IndexShardRoutingTable indexShardRoutingTable) {
        return IntStream.range(0, indexShardRoutingTable.size()).mapToObj(indexShardRoutingTable::shard);
    }

    public static RoutingNode routingNode(String nodeId, DiscoveryNode node, ShardRouting... shards) {
        final RoutingNode routingNode = new RoutingNode(nodeId, node, shards.length);
        for (ShardRouting shardRouting : shards) {
            routingNode.add(shardRouting);
        }
        return routingNode;
    }
}
