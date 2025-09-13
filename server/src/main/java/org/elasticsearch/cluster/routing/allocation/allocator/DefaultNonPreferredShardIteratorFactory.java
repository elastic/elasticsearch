/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Non-preferred shard iterator factory that returns the most desirable shards from most-hot-spotted
 * nodes first.
 * Does not return nodes for which we have no write-pool utilization, or shards for which we have no
 * write-load data.
 */
public class DefaultNonPreferredShardIteratorFactory implements NonPreferredShardIteratorFactory {

    private final WriteLoadConstraintSettings writeLoadConstraintSettings;

    public DefaultNonPreferredShardIteratorFactory(WriteLoadConstraintSettings writeLoadConstraintSettings) {
        this.writeLoadConstraintSettings = writeLoadConstraintSettings;
    }

    @Override
    public Iterable<ShardRouting> createNonPreferredShardIterator(RoutingAllocation allocation) {
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().notFullyEnabled()) {
            return Collections.emptyList();
        }
        final Set<NodeShardIterable> hotSpottedNodes = new TreeSet<>(Comparator.reverseOrder());
        final var nodeUsageStatsForThreadPools = allocation.clusterInfo().getNodeUsageStatsForThreadPools();
        for (RoutingNode node : allocation.routingNodes()) {
            var nodeUsageStats = nodeUsageStatsForThreadPools.get(node.nodeId());
            if (nodeUsageStats != null) {
                final var writeThreadPoolStats = nodeUsageStats.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
                assert writeThreadPoolStats != null;
                hotSpottedNodes.add(new NodeShardIterable(allocation, node, writeThreadPoolStats.maxThreadPoolQueueLatencyMillis()));
            }
        }
        return () -> new LazilyExpandingIterator<>(hotSpottedNodes);
    }

    private static class NodeShardIterable implements Iterable<ShardRouting>, Comparable<NodeShardIterable> {

        private final RoutingAllocation allocation;
        private final RoutingNode routingNode;
        private final long maxQueueLatencyMillis;

        private NodeShardIterable(RoutingAllocation allocation, RoutingNode routingNode, long maxQueueLatencyMillis) {
            this.allocation = allocation;
            this.routingNode = routingNode;
            this.maxQueueLatencyMillis = maxQueueLatencyMillis;
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return createShardIterator();
        }

        @Override
        public int compareTo(NodeShardIterable o) {
            return Long.compare(maxQueueLatencyMillis, o.maxQueueLatencyMillis);
        }

        private Iterator<ShardRouting> createShardIterator() {
            final var shardWriteLoads = allocation.clusterInfo().getShardWriteLoads();
            final List<ShardRouting> sortedRoutings = new ArrayList<>();
            double totalWriteLoad = 0;
            for (ShardRouting shard : routingNode) {
                Double shardWriteLoad = shardWriteLoads.get(shard.shardId());
                if (shardWriteLoad != null) {
                    sortedRoutings.add(shard);
                    totalWriteLoad += shardWriteLoad;
                }
            }
            // TODO: Work out what this order should be
            // Sort by distance-from-mean-write-load
            double meanWriteLoad = totalWriteLoad / sortedRoutings.size();
            sortedRoutings.sort(Comparator.comparing(sr -> Math.abs(shardWriteLoads.get(sr.shardId()) - meanWriteLoad)));
            return sortedRoutings.iterator();
        }
    }

    static class LazilyExpandingIterator<T> implements Iterator<T> {

        private final Iterator<? extends Iterable<T>> allIterables;
        private Iterator<T> currentIterator;

        LazilyExpandingIterator(Iterable<? extends Iterable<T>> allIterables) {
            this.allIterables = allIterables.iterator();
        }

        @Override
        public boolean hasNext() {
            while (currentIterator == null || currentIterator.hasNext() == false) {
                if (allIterables.hasNext() == false) {
                    return false;
                } else {
                    currentIterator = allIterables.next().iterator();
                }
            }
            return true;
        }

        @Override
        public T next() {
            while (currentIterator == null || currentIterator.hasNext() == false) {
                currentIterator = allIterables.next().iterator();
            }
            return currentIterator.next();
        }
    }
}
