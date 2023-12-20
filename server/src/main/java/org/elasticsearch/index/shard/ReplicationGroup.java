/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Replication group for a shard. Used by a primary shard to coordinate replication and recoveries.
 */
public class ReplicationGroup {
    private final IndexShardRoutingTable routingTable;
    private final Set<String> inSyncAllocationIds;
    private final Set<String> trackedAllocationIds;
    private final long version;

    private final Set<String> unavailableInSyncShards; // derived from the other fields
    private final List<ShardRouting> replicationTargets; // derived from the other fields
    private final List<ShardRouting> skippedShards; // derived from the other fields

    public ReplicationGroup(
        IndexShardRoutingTable routingTable,
        Set<String> inSyncAllocationIds,
        Set<String> trackedAllocationIds,
        long version
    ) {
        this.routingTable = routingTable;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.trackedAllocationIds = trackedAllocationIds;
        this.version = version;

        this.unavailableInSyncShards = Sets.difference(inSyncAllocationIds, routingTable.getPromotableAllocationIds());
        this.replicationTargets = new ArrayList<>();
        this.skippedShards = new ArrayList<>();
        for (int copy = 0; copy < routingTable.size(); copy++) {
            ShardRouting shard = routingTable.shard(copy);
            if (shard.isPromotableToPrimary() == false) {
                continue;
            }
            if (shard.unassigned()) {
                assert shard.primary() == false : "primary shard should not be unassigned in a replication group: " + shard;
                skippedShards.add(shard);
            } else {
                if (trackedAllocationIds.contains(shard.allocationId().getId())) {
                    replicationTargets.add(shard);
                } else {
                    assert inSyncAllocationIds.contains(shard.allocationId().getId()) == false
                        : "in-sync shard copy but not tracked: " + shard;
                    skippedShards.add(shard);
                }
                if (shard.relocating()) {
                    ShardRouting relocationTarget = shard.getTargetRelocatingShard();
                    if (trackedAllocationIds.contains(relocationTarget.allocationId().getId())) {
                        replicationTargets.add(relocationTarget);
                    } else {
                        skippedShards.add(relocationTarget);
                        assert inSyncAllocationIds.contains(relocationTarget.allocationId().getId()) == false
                            : "in-sync shard copy but not tracked: " + shard;
                    }
                }
            }
        }
    }

    public long getVersion() {
        return version;
    }

    public IndexShardRoutingTable getRoutingTable() {
        return routingTable;
    }

    public Set<String> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public Set<String> getTrackedAllocationIds() {
        return trackedAllocationIds;
    }

    /**
     * Returns the set of shard allocation ids that are in the in-sync set but have no assigned routing entry
     */
    public Set<String> getUnavailableInSyncShards() {
        return unavailableInSyncShards;
    }

    /**
     * Returns the subset of shards in the routing table that should be replicated to. Includes relocation targets.
     */
    public List<ShardRouting> getReplicationTargets() {
        return replicationTargets;
    }

    /**
     * Returns the subset of shards in the routing table that are unassigned or initializing and not ready yet to receive operations
     * (i.e. engine not opened yet). Includes relocation targets.
     */
    public List<ShardRouting> getSkippedShards() {
        return skippedShards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationGroup that = (ReplicationGroup) o;

        if (routingTable.equals(that.routingTable) == false) return false;
        if (inSyncAllocationIds.equals(that.inSyncAllocationIds) == false) return false;
        return trackedAllocationIds.equals(that.trackedAllocationIds);
    }

    @Override
    public int hashCode() {
        int result = routingTable.hashCode();
        result = 31 * result + inSyncAllocationIds.hashCode();
        result = 31 * result + trackedAllocationIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ReplicationGroup{"
            + "routingTable="
            + routingTable
            + ", inSyncAllocationIds="
            + inSyncAllocationIds
            + ", trackedAllocationIds="
            + trackedAllocationIds
            + '}';
    }

}
