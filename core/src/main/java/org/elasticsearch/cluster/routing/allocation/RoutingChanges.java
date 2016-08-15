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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Records changes made to {@link RoutingNodes} during an allocation round.
 */
public class RoutingChanges {
    private final Map<ShardId, ShardRoutingChanges> shardChanges = new HashMap<>();

    private boolean changed;

    /**
     * Returns whether changes were made
     */
    public boolean isChanged() {
        return changed;
    }

    /**
     * Returns the changes that were made to the routing entries. Changes are grouped per shard id.
     */
    public Map<ShardId, ShardRoutingChanges> getChanges() {
        return Collections.unmodifiableMap(shardChanges);
    }

    /**
     * Called when unassigned shard is initialized. Does not include initializing relocation target shards.
     */
    public void shardInitialized(ShardRouting unassignedShard) {
        assert unassignedShard.unassigned() : "expected unassigned shard " + unassignedShard;
        changes(unassignedShard.shardId()).initializedShards.add(unassignedShard);
    }

    /**
     * Called when an initializing shard is started.
     */
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        assert initializingShard.initializing() : "expected initializing shard " + initializingShard;
        assert startedShard.started() : "expected started shard " + startedShard;
        changes(initializingShard.shardId()).startedShards.add(startedShard);
    }

    /**
     * Called when relocation of a started shard is initiated.
     */
    public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard) {
        assert startedShard.started() : "expected started shard " + startedShard;
        assert targetRelocatingShard.isRelocationTarget() : "expected relocation target shard " + targetRelocatingShard;
        setChanged();
    }

    /**
     * Called when an unassigned shard's unassigned information was updated
     */
    public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
        assert unassignedShard.unassigned() : "expected unassigned shard " + unassignedShard;
        setChanged();
    }

    /**
     * Called when an active shard is failed or cancelled.
     */
    public void activeShardFailed(ShardRouting activeShard, UnassignedInfo unassignedInfo) {
        assert activeShard.active() : "expected active shard " + activeShard;
        changes(activeShard.shardId()).failedActiveShards.add(Tuple.tuple(activeShard, unassignedInfo));
    }

    /**
     * Called when an initializing shard is failed or cancelled.
     */
    public void initializingShardFailed(ShardRouting initializingShard) {
        assert initializingShard.initializing() : "expected initializing shard " + initializingShard;
        setChanged();
    }


    /**
     * Called on relocation source when relocation completes after relocation target is started.
     */
    public void relocationCompleted(ShardRouting removedRelocationSource) {
        assert removedRelocationSource.relocating() : "expected relocating shard " + removedRelocationSource;
        changes(removedRelocationSource.shardId()).relocationCompletedSourceShards.add(removedRelocationSource);
    }

    /**
     * Called on replica relocation target when replica relocation source fails. Promotes the replica relocation target to ordinary
     * initializing shard.
     */
    public void relocationSourceRemoved(ShardRouting removedReplicaRelocationSource) {
        assert removedReplicaRelocationSource.primary() == false && removedReplicaRelocationSource.isRelocationTarget() :
            "expected replica relocation target shard " + removedReplicaRelocationSource;
        setChanged();
    }

    /**
     * Called on started primary shard after it has been promoted from replica to primary and is reinitialized due to shadow replicas.
     */
    public void startedPrimaryReinitialized(ShardRouting startedPrimaryShard, ShardRouting initializedShard) {
        assert startedPrimaryShard.primary() && startedPrimaryShard.started() : "expected started primary shard " + startedPrimaryShard;
        assert initializedShard.primary() && initializedShard.initializing(): "expected initializing primary shard " + initializedShard;
        changes(startedPrimaryShard.shardId()).reinitalizedPrimaryShards.add(startedPrimaryShard);
    }

    /**
     * Called when started replica is promoted to primary.
     */
    public void replicaPromoted(ShardRouting replicaShard) {
        assert replicaShard.started() && replicaShard.primary() == false : "expected started replica shard " + replicaShard;
        changes(replicaShard.shardId()).primaryPromotedShards.add(replicaShard);
    }

    /**
     * Creates a new {@link ShardRoutingChanges} object for the given shard id or returns the current one if such an entry exists already.
     */
    private ShardRoutingChanges changes(ShardId shardId) {
        setChanged();
        return shardChanges.computeIfAbsent(shardId, ShardRoutingChanges::new);
    }

    /**
     * Marks the allocation as changed.
     */
    private void setChanged() {
        changed = true;
    }


    public static class ShardRoutingChanges {
        private final ShardId shardId;
        private final List<ShardRouting> initializedShards = new ArrayList<>();
        private final List<ShardRouting> startedShards = new ArrayList<>();
        private final List<Tuple<ShardRouting, UnassignedInfo>> failedActiveShards = new ArrayList<>();
        private final List<ShardRouting> relocationCompletedSourceShards = new ArrayList<>();
        private final List<ShardRouting> primaryPromotedShards = new ArrayList<>();
        private final List<ShardRouting> reinitalizedPrimaryShards = new ArrayList<>();

        public ShardRoutingChanges(ShardId shardId) {
            this.shardId = shardId;
        }

        /**
         * Returns list of unassigned shards that were initialized. Does not include initializing relocation target shards.
         */
        public List<ShardRouting> getInitializedShards() {
            return Collections.unmodifiableList(initializedShards);
        }

        /**
         * Returns list of shards that were started (returned ShardRouting objects are in initializing state)
         */
        public List<ShardRouting> getStartedShards() {
            return Collections.unmodifiableList(startedShards);
        }

        /**
         * Returns list of active shards that were cancelled or failed (returned ShardRouting objects are in active state)
         */
        public List<Tuple<ShardRouting, UnassignedInfo>> getFailedActiveShards() {
            return Collections.unmodifiableList(failedActiveShards);
        }

        /**
         * Returns list of relocation source shards that where removed after relocation completed.
         */
        public List<ShardRouting> getRelocationCompletedSourceShards() {
            return Collections.unmodifiableList(relocationCompletedSourceShards);
        }

        /**
         * Returns list of started replica shards that were promoted to primary after primary failed.
         */
        public List<ShardRouting> getPrimaryPromotedShards() {
            return Collections.unmodifiableList(primaryPromotedShards);
        }

        /**
         * Returns list of reinitialized primary shards that were promoted from replica to primary and reinitialized due to shadow replicas.
         */
        public List<ShardRouting> getReinitalizedPrimaryShards() {
            return Collections.unmodifiableList(reinitalizedPrimaryShards);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Shard: ").append(shardId).append(", ");
            sb.append("initialized shards: ").append(initializedShards).append(", ");
            sb.append("started shards: ").append(startedShards).append(", ");
            sb.append("failed active shards: ").append(failedActiveShards).append(", ");
            sb.append("removed relocation-source shards: ").append(relocationCompletedSourceShards).append(", ");
            sb.append("primary-promoted shards: ").append(primaryPromotedShards).append(", ");
            sb.append("reinitialized primary shards: ").append(reinitalizedPrimaryShards);
            return sb.toString();
        }
    }
}
