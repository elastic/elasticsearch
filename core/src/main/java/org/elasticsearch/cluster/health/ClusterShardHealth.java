/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.health;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public final class ClusterShardHealth implements Writeable {

    private final int shardId;
    private final ClusterHealthStatus status;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private final boolean primaryActive;

    public ClusterShardHealth(final int shardId, final IndexShardRoutingTable shardRoutingTable, final IndexMetaData indexMetaData) {
        this.shardId = shardId;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        for (ShardRouting shardRouting : shardRoutingTable) {
            if (shardRouting.active()) {
                computeActiveShards++;
                if (shardRouting.relocating()) {
                    // the shard is relocating, the one it is relocating to will be in initializing state, so we don't count it
                    computeRelocatingShards++;
                }
            } else if (shardRouting.initializing()) {
                computeInitializingShards++;
            } else if (shardRouting.unassigned()) {
                computeUnassignedShards++;
            }
        }
        ClusterHealthStatus computeStatus;
        final ShardRouting primaryRouting = shardRoutingTable.primaryShard();
        if (primaryRouting.active()) {
            if (computeActiveShards == shardRoutingTable.size()) {
                computeStatus = ClusterHealthStatus.GREEN;
            } else {
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        } else {
            computeStatus = getInactivePrimaryHealth(primaryRouting, indexMetaData);
        }
        this.status = computeStatus;
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;
        this.primaryActive = primaryRouting.active();
    }

    public ClusterShardHealth(final StreamInput in) throws IOException {
        shardId = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        primaryActive = in.readBoolean();
    }

    public int getId() {
        return shardId;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public boolean isPrimaryActive() {
        return primaryActive;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeByte(status.value());
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeBoolean(primaryActive);
    }

    /**
     * Checks if an inactive primary shard should cause the cluster health to go RED.
     *
     * Normally, an inactive primary shard in an index should cause the cluster health to be RED.  However,
     * there are exceptions where a health status of RED is inappropriate, namely in these scenarios:
     *   1. Index Creation.  When an index is first created, the primary shards are in the initializing state, so
     *      there is a small window where the cluster health is RED due to the primaries not being activated yet.
     *      However, this leads to a false sense that the cluster is in an unhealthy state, when in reality, its
     *      simply a case of needing to wait for the primaries to initialize.
     *   2. When a cluster is in the recovery state, and the shard never had any allocation ids assigned to it,
     *      which indicates the index was created and before allocation of the primary occurred for this shard,
     *      a cluster restart happened.
     *
     * Here, we check for these scenarios and set the cluster health to YELLOW if any are applicable.
     *
     * NB: this method should *not* be called on active shards nor on non-primary shards.
     */
    public static ClusterHealthStatus getInactivePrimaryHealth(final ShardRouting shardRouting, final IndexMetaData indexMetaData) {
        assert shardRouting.primary() : "cannot invoke on a replica shard: " + shardRouting;
        assert shardRouting.active() == false : "cannot invoke on an active shard: " + shardRouting;
        assert shardRouting.unassignedInfo() != null : "cannot invoke on a shard with no UnassignedInfo: " + shardRouting;
        final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        if (unassignedInfo.getLastAllocationStatus() != AllocationStatus.DECIDERS_NO
                && shardRouting.allocatedPostIndexCreate(indexMetaData) == false
                && (unassignedInfo.getReason() == Reason.INDEX_CREATED || unassignedInfo.getReason() == Reason.CLUSTER_RECOVERED)) {
            return ClusterHealthStatus.YELLOW;
        } else {
            return ClusterHealthStatus.RED;
        }
    }

}
