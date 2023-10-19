/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Iterators;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * This class iterates all shards from all nodes in order of allocation recency.
 * Shards from the node that had a new shard allocation would appear in the end of iteration.
 */
public class OrderedShardsIterator implements Iterator<ShardRouting> {

    private final ArrayDeque<NodeAndShardIterator> queue;

    public static OrderedShardsIterator create(RoutingNodes routingNodes, NodeAllocationOrdering ordering) {
        var queue = new ArrayDeque<NodeAndShardIterator>(routingNodes.size());
        for (var nodeId : ordering.sort(routingNodes.getAllNodeIds())) {
            var node = routingNodes.node(nodeId);
            if (node.size() > 0) {
                queue.add(new NodeAndShardIterator(nodeId, Iterators.forArray(node.copyShards())));
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
