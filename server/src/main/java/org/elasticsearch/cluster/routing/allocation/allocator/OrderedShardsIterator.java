/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.collect.Iterators;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * This class iterates all shards from all nodes.
 * The shard order is defined by
 * (1) allocation recency: shards from the node that had a new shard allocation would appear in the end of iteration.
 * (2) shard priority: for necessary moves data stream write shards, then regular index shards, then the rest
 *                     for rebalancing the order is inverse
 */
public class OrderedShardsIterator implements Iterator<ShardRouting> {

    private final ArrayDeque<NodeAndShardIterator> queue;

    public static OrderedShardsIterator createForNecessaryMoves(RoutingAllocation allocation, NodeAllocationOrdering ordering) {
        return create(allocation.routingNodes(), createShardsComparator(allocation.metadata()), ordering);
    }

    public static OrderedShardsIterator createForBalancing(RoutingAllocation allocation, NodeAllocationOrdering ordering) {
        return create(allocation.routingNodes(), createShardsComparator(allocation.metadata()).reversed(), ordering);
    }

    private static OrderedShardsIterator create(
        RoutingNodes routingNodes,
        Comparator<ShardRouting> shardOrder,
        NodeAllocationOrdering nodeOrder
    ) {
        var queue = new ArrayDeque<NodeAndShardIterator>(routingNodes.size());
        for (var nodeId : nodeOrder.sort(routingNodes.getAllNodeIds())) {
            var node = routingNodes.node(nodeId);
            if (node.size() > 0) {
                queue.add(new NodeAndShardIterator(nodeId, sort(shardOrder, node.copyShards())));
            }
        }
        return new OrderedShardsIterator(queue);
    }

    private static Iterator<ShardRouting> sort(Comparator<ShardRouting> comparator, ShardRouting[] shards) {
        Arrays.sort(shards, comparator);
        return Iterators.forArray(shards);
    }

    private static Comparator<ShardRouting> createShardsComparator(Metadata metadata) {
        return Comparator.comparing(shard -> {
            var lookup = metadata.getIndicesLookup().get(shard.getIndexName());
            if (lookup != null && lookup.getParentDataStream() != null) {
                // prioritize write indices of the data stream
                return Objects.equals(lookup.getParentDataStream().getWriteIndex(), shard.index()) ? 0 : 2;
            } else {
                // regular index
                return 1;
            }
        });
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
