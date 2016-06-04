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

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Set;

public final class ClusterShardHealth implements Writeable {

    private final int shardId;
    private final ClusterHealthStatus status;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private final boolean primaryActive;

    public ClusterShardHealth(final int shardId, final IndexShardRoutingTable shardRoutingTable, final Set<String> activeAllocationIds) {
        this.shardId = shardId;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        boolean computePrimaryActive = false;
        ShardRouting primaryRouting = null;
        for (ShardRouting shardRouting : shardRoutingTable) {
            if (shardRouting.active()) {
                computeActiveShards++;
                if (shardRouting.relocating()) {
                    // the shard is relocating, the one it is relocating to will be in initializing state, so we don't count it
                    computeRelocatingShards++;
                }
                if (shardRouting.primary()) {
                    computePrimaryActive = true;
                }
            } else if (shardRouting.initializing()) {
                computeInitializingShards++;
            } else if (shardRouting.unassigned()) {
                computeUnassignedShards++;
            }
            if (shardRouting.primary()) {
                primaryRouting = shardRouting;
            }
        }
        ClusterHealthStatus computeStatus;
        if (computePrimaryActive) {
            if (computeActiveShards == shardRoutingTable.size()) {
                computeStatus = ClusterHealthStatus.GREEN;
            } else {
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        } else {
            computeStatus = ClusterHealthStatus.RED;
            assert primaryRouting != null;
            /**
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
             */
            if (primaryRouting.unassignedInfo() != null) {
                final UnassignedInfo.Reason reason = primaryRouting.unassignedInfo().getReason();
                if (reason == UnassignedInfo.Reason.INDEX_CREATED
                        || (reason == UnassignedInfo.Reason.CLUSTER_RECOVERED && activeAllocationIds.isEmpty())) {
                    computeStatus = ClusterHealthStatus.YELLOW;
                }
            }
        }
        this.status = computeStatus;
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;
        this.primaryActive = computePrimaryActive;
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
}
