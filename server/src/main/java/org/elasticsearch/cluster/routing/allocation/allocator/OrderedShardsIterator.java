/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
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

    /**
     * This iterator will progress through the shards node by node, each node's shards ordered from most write active to least.
     *
     * @param allocation
     * @param ordering
     * @return An iterator over all shards in the {@link RoutingNodes} held by {@code allocation} (all shards assigned to a node). The
     * iterator will progress node by node, where each node's shards are ordered from data stream write indices, to regular indices and
     * lastly to data stream read indices.
     */
    public static OrderedShardsIterator createForNecessaryMoves(RoutingAllocation allocation, NodeAllocationOrdering ordering) {
        return create(allocation.routingNodes(), createShardsComparator(allocation), ordering);
    }

    /**
     * This iterator will progress through the shards node by node, each node's shards ordered from least write active to most.
     *
     * @param allocation
     * @param ordering
     * @return An iterator over all shards in the {@link RoutingNodes} held by {@code allocation} (all shards assigned to a node). The
     * iterator will progress node by node, where each node's shards are ordered from data stream read indices, to regular indices and
     * lastly to data stream write indices.
     */
    public static OrderedShardsIterator createForBalancing(RoutingAllocation allocation, NodeAllocationOrdering ordering) {
        return create(allocation.routingNodes(), createShardsComparator(allocation).reversed(), ordering);
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

    /**
     * Prioritizes write indices of data streams, and deprioritizes data stream read indices, relative to regular indices.
     */
    private static Comparator<ShardRouting> createShardsComparator(RoutingAllocation allocation) {
        return Comparator.comparing(shard -> {
            final ProjectMetadata project = allocation.metadata().projectFor(shard.index());
            var index = project.getIndicesLookup().get(shard.getIndexName());
            if (index != null && index.getParentDataStream() != null) {
                // prioritize write indices of the data stream
                return Objects.equals(index.getParentDataStream().getWriteIndex(), shard.index()) ? 0 : 2;
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
