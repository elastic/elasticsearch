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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

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
            return StreamSupport.stream(routingNode.spliterator(), false)
                .sorted(new TieringWriteLoadComparator(shardWriteLoads))
                .toList()
                .iterator();
        }
    }

    /**
     * Sorts shards by "tiered" write load, then descending write load inside tiers
     *
     * e.g., MEDIUM/0.7, MEDIUM/0.65, HIGH/0.9, HIGH/0.81, LOW/0.4, LOW/0.2, LOW/0.1
     */
    private static class TieringWriteLoadComparator implements Comparator<ShardRouting> {

        private final Map<ShardId, Double> shardWriteLoads;
        private final double lowThreshold;
        private final double highThreshold;
        private final Comparator<ShardRouting> comparator;

        /**
         * Enum order is prioritization order
         */
        private enum Tier {
            MEDIUM,
            HIGH,
            LOW
        }

        private TieringWriteLoadComparator(Map<ShardId, Double> shardWriteLoads) {
            this.shardWriteLoads = shardWriteLoads;
            double maxWriteLoad = shardWriteLoads.values().stream().reduce(0.0, Double::max);
            this.lowThreshold = maxWriteLoad * 0.5;
            this.highThreshold = maxWriteLoad * 0.8;
            this.comparator = Comparator.comparing(this::getTier)
                .thenComparing(sr -> shardWriteLoads.getOrDefault(sr.shardId(), 0.0), Comparator.reverseOrder());
        }

        @Override
        public int compare(ShardRouting o1, ShardRouting o2) {
            return comparator.compare(o1, o2);
        }

        private Tier getTier(ShardRouting shard) {
            double writeLoad = shardWriteLoads.getOrDefault(shard.shardId(), 0.0);
            if (writeLoad < lowThreshold) {
                return Tier.LOW;
            } else if (writeLoad < highThreshold) {
                return Tier.MEDIUM;
            } else {
                return Tier.HIGH;
            }
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
