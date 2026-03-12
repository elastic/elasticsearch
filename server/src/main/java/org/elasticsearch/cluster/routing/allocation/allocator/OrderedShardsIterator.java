/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * This class iterates all shards from all nodes.
 * The shard order is defined by
 * (1) allocation recency: shards from the node that had a new shard allocation would appear in the end of iteration.
 * (2) shard priority: dictated by the provided {@link ShardRelocationOrder}.
 */
public class OrderedShardsIterator implements Iterator<ShardRouting> {

    private final ArrayDeque<NodeAndShardIterator> queue;

    /**
     * Creates an iterator over all shards in the {@link RoutingNodes} held by {@code allocation} (all shards assigned to a node) that
     * will progress through the shards node by node based on {@link NodeAllocationOrdering}, each node's shards ordered based on the
     * provided {@link ShardRelocationOrder} for necessary moves.
     */
    public static OrderedShardsIterator createForNecessaryMoves(
        RoutingAllocation allocation,
        NodeAllocationOrdering ordering,
        ShardRelocationOrder shardRelocationOrder
    ) {
        return create(allocation, shardRelocationOrder::forNecessaryMoves, ordering);
    }

    /**
     * Creates an iterator over all shards in the {@link RoutingNodes} held by {@code allocation} (all shards assigned to a node) that
     * will progress through the shards node by node based on {@link NodeAllocationOrdering}, each node's shards ordered based on the
     * provided {@link ShardRelocationOrder} for balancing moves.
     */
    public static OrderedShardsIterator createForBalancing(
        RoutingAllocation allocation,
        NodeAllocationOrdering ordering,
        ShardRelocationOrder shardRelocationOrder
    ) {
        return create(allocation, shardRelocationOrder::forBalancing, ordering);
    }

    private static OrderedShardsIterator create(
        RoutingAllocation allocation,
        ShardRelocationOrder.Ordering shardOrdering,
        NodeAllocationOrdering nodeOrder
    ) {
        final var routingNodes = allocation.routingNodes();
        var queue = new ArrayDeque<NodeAndShardIterator>(routingNodes.size());
        for (var nodeId : nodeOrder.sort(routingNodes.getAllNodeIds())) {
            var node = routingNodes.node(nodeId);
            if (node.size() > 0) {
                queue.add(new NodeAndShardIterator(nodeId, shardOrdering.order(allocation, nodeId)));
            }
        }
        return new OrderedShardsIterator(queue);
    }

    private OrderedShardsIterator(ArrayDeque<NodeAndShardIterator> queue) {
        this.queue = queue;
    }

    @Override
    public boolean hasNext() {
        return queue.isEmpty() == false;
    }

    @Override
    public ShardRouting next() {
        if (queue.isEmpty()) {
            throw new NoSuchElementException();
        }
        var entry = queue.peek();
        assert entry.iterator.hasNext();
        final var nextShard = entry.iterator.next();
        if (entry.iterator.hasNext() == false) {
            queue.poll();
        }
        return nextShard;
    }

    public void dePrioritizeNode(String nodeId) {
        var entry = queue.peek();
        if (entry != null && Objects.equals(nodeId, entry.nodeId)) {
            queue.offer(queue.poll());
        }
    }

    private record NodeAndShardIterator(String nodeId, Iterator<ShardRouting> iterator) {}
}
