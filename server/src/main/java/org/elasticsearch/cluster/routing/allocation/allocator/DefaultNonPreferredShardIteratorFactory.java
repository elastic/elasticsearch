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
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Non-preferred shard iterator factory that returns the most desirable shards from most-hot-spotted
 * nodes first.
 * <ul>
 *  <li>Any nodes missing queue-latency information are considered to have a queue-latency of 0.</li>
 *  <li>Any shards missing write-load information are considered to have a write-load of 0.</li>
 * </ul>
 */
public record DefaultNonPreferredShardIteratorFactory(WriteLoadConstraintSettings writeLoadConstraintSettings)
    implements
        NonPreferredShardIteratorFactory {

    @Override
    public Iterable<ShardRouting> createNonPreferredShardIterator(RoutingAllocation allocation) {
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().notFullyEnabled()) {
            return Collections.emptyList();
        }
        final Set<NodeShardIterable> allClusterNodes = new TreeSet<>(Comparator.reverseOrder());
        final var nodeUsageStatsForThreadPools = allocation.clusterInfo().getNodeUsageStatsForThreadPools();
        for (RoutingNode node : allocation.routingNodes()) {
            var nodeUsageStats = nodeUsageStatsForThreadPools.get(node.nodeId());
            if (nodeUsageStats != null) {
                final var writeThreadPoolStats = nodeUsageStats.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
                assert writeThreadPoolStats != null;
                allClusterNodes.add(new NodeShardIterable(allocation, node, writeThreadPoolStats.maxThreadPoolQueueLatencyMillis()));
            } else {
                allClusterNodes.add(new NodeShardIterable(allocation, node, 0L));
            }
        }
        return () -> new LazilyExpandingIterator<>(allClusterNodes);
    }

    /**
     * Returns all shards from a node in the order
     *
     * <ol>
     *     <li>shards with medium write-load</li>
     *     <li>shards with high write-load</li>
     *     <li>shards with low write-load</li>
     * </ol>
     *
     * Where low and high thresholds are {@link #LOW_THRESHOLD} * <code>max-write-load</code>
     * and {@link #HIGH_THRESHOLD} * <code>max-write-load</code> respectively.
     */
    private record NodeShardIterable(RoutingAllocation allocation, RoutingNode routingNode, long maxQueueLatencyMillis)
        implements
            Iterable<ShardRouting>,
            Comparable<NodeShardIterable> {

        private static final double LOW_THRESHOLD = 0.5;
        private static final double HIGH_THRESHOLD = 0.8;

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
            final WriteLoadFilter filter = WriteLoadFilter.create(shardWriteLoads);
            return Stream.of(
                StreamSupport.stream(routingNode.spliterator(), false).filter(filter::hasMediumLoad),
                StreamSupport.stream(routingNode.spliterator(), false).filter(filter::hasHighLoad),
                StreamSupport.stream(routingNode.spliterator(), false).filter(filter::hasLowLoad)
            ).flatMap(Function.identity()).iterator();
        }

        private record WriteLoadFilter(Map<ShardId, Double> shardWriteLoads, double lowThreshold, double highThreshold) {

            public static WriteLoadFilter create(Map<ShardId, Double> shardWriteLoads) {
                final double maxWriteLoad = shardWriteLoads.values().stream().reduce(0.0, Double::max);
                final double lowThreshold = maxWriteLoad * NodeShardIterable.LOW_THRESHOLD;
                final double highThreshold = maxWriteLoad * NodeShardIterable.HIGH_THRESHOLD;
                return new WriteLoadFilter(shardWriteLoads, lowThreshold, highThreshold);
            }

            public boolean hasMediumLoad(ShardRouting shardRouting) {
                double shardWriteLoad = shardWriteLoad(shardRouting);
                return shardWriteLoad >= lowThreshold && shardWriteLoad < highThreshold;
            }

            public boolean hasHighLoad(ShardRouting shardRouting) {
                return shardWriteLoad(shardRouting) >= highThreshold;
            }

            public boolean hasLowLoad(ShardRouting shardRouting) {
                return shardWriteLoad(shardRouting) < lowThreshold;
            }

            private double shardWriteLoad(ShardRouting shardRouting) {
                return shardWriteLoads.getOrDefault(shardRouting.shardId(), 0.0);
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
