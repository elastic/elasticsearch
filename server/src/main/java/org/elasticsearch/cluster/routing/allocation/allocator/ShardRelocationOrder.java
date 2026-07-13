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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.collect.Iterators;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

/**
 * Defines the order in which shards on a node are chosen for relocation
 */
public interface ShardRelocationOrder {

    Iterator<ShardRouting> forNecessaryMoves(RoutingAllocation allocation, String nodeId);

    Iterator<ShardRouting> forBalancing(RoutingAllocation allocation, String nodeId);

    /**
     * Shard ordering for a given node
     */
    interface Ordering {
        Iterator<ShardRouting> order(RoutingAllocation allocation, String nodeId);
    }

    class DefaultOrder implements ShardRelocationOrder {
        /**
         * Node's shards are ordered from data stream write indices, to regular indices and lastly to data stream read indices.
         * In case of the necessary shard movement, such shards will move first. This might be useful in case of the timeout
         * for the node shutdown.
         */
        @Override
        public Iterator<ShardRouting> forNecessaryMoves(RoutingAllocation allocation, String nodeId) {
            var shards = allocation.routingNodes().node(nodeId).copyShards();
            var comparator = createShardsComparator(allocation);
            Arrays.sort(shards, comparator);
            return Iterators.forArray(shards);
        }

        /**
         * Node's shards are ordered from data stream read indices, to regular indices and lastly to data stream write indices.
         * This is to minimize the impact of relocations on shards with active writes in case of balancing.
         */
        @Override
        public Iterator<ShardRouting> forBalancing(RoutingAllocation allocation, String nodeId) {
            var shards = allocation.routingNodes().node(nodeId).copyShards();
            var comparator = createShardsComparator(allocation).reversed();
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
    };
}
