/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardRelocationOrder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;

public class StatelessShardRelocationOrder extends ShardRelocationOrder.DefaultOrder {

    /**
     * If enabled, necessary shard relocations follow the {@link ShardRelocationOrder.DefaultOrder} (DS write first,
     * then normal, then DS read). If disabled, a custom ordering is applied that interleaves half of the DS read shards
     * with DS write and normal index shards (with write-load spreading), keeping the other half of DS read at the end.
     */
    public static Setting<Boolean> PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING = Setting.boolSetting(
        "stateless.cluster.routing.allocation.prioritize_write_shard_necessary_relocations",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean prioritizeWriteShardRelocation;

    public StatelessShardRelocationOrder(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING, value -> prioritizeWriteShardRelocation = value);
    }

    @Override
    public Iterator<ShardRouting> forNecessaryMoves(RoutingAllocation allocation, String nodeId) {
        if (prioritizeWriteShardRelocation) {
            return super.forNecessaryMoves(allocation, nodeId);
        }
        return orderNecessaryMovesWithInterleaving(allocation, nodeId);
    }

    /**
     * Orders necessary shard relocations by interleaving half of the DS read shards with DS write and normal index
     * shards (with write-load spreading), keeping the other half of DS read shards at the end of the iterator.
     */
    static Iterator<ShardRouting> orderNecessaryMovesWithInterleaving(RoutingAllocation allocation, String nodeId) {
        var routingNode = allocation.routingNodes().node(nodeId);
        if (routingNode.isEmpty()) {
            return Collections.emptyIterator();
        }

        final var result = new LinkedList<ShardRouting>();  // has two parts, the second part are just data stream read shards
        final var shardsToSpread = new LinkedList<ShardRouting>(); // write shards or normal shards with write load
        final var firstHalfRest = new LinkedList<ShardRouting>(); // read shards or normal shards with no write load
        boolean addToSecondHalf = true;
        var writeLoads = allocation.clusterInfo().getShardWriteLoads();

        for (ShardRouting shard : routingNode) {
            var index = allocation.metadata().projectFor(shard.index()).getIndicesLookup().get(shard.getIndexName());
            if (index != null && index.getParentDataStream() != null) {
                // data stream write shards must be always spread around. read shards are split between first and second half.
                if (Objects.equals(index.getParentDataStream().getWriteIndex(), shard.index())) {
                    shardsToSpread.add(shard);
                } else if (addToSecondHalf) {
                    result.add(shard);
                    addToSecondHalf = false;
                } else {
                    firstHalfRest.add(shard);
                    addToSecondHalf = true;
                }
            } else if (writeLoads.getOrDefault(shard.shardId(), 0.0) > 0.0) {
                shardsToSpread.add(shard);
            } else {
                firstHalfRest.add(shard);
            }
        }

        if (shardsToSpread.isEmpty()) {
            firstHalfRest.forEach(result::addFirst);
            return result.iterator();
        }

        // Interleave shardsToSpread with firstHalfRest so that the former are spread out evenly.
        int toInterleave = Math.ceilDiv(firstHalfRest.size(), shardsToSpread.size());
        var firstHalfRestIterator = firstHalfRest.iterator();
        for (ShardRouting shardRouting : shardsToSpread) {
            for (int k = 0; k < toInterleave && firstHalfRestIterator.hasNext(); k++) {
                result.addFirst(firstHalfRestIterator.next());
            }
            result.addFirst(shardRouting);
        }
        assert firstHalfRestIterator.hasNext() == false;

        return result.iterator();
    }

    @Override
    public Iterator<ShardRouting> forBalancing(RoutingAllocation allocation, String nodeId) {
        return super.forBalancing(allocation, nodeId);
    }
}
