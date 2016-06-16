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
package org.elasticsearch.index.seqno;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;

/**
 * A shard component that is responsible of tracking the global checkpoint. The global checkpoint
 * is the highest seq_no for which all lower (or equal) seq_no have been processed on all shards that
 * are currently active. Since shards count as "active" when the master starts them, and before this primary shard
 * has been notified of this fact, we also include shards in that are in the
 * {@link org.elasticsearch.index.shard.IndexShardState#POST_RECOVERY} state when checking for global checkpoint advancement.
 * We call these shards "in sync" with all operations on the primary (see {@link #inSyncLocalCheckpoints}.
 *
 * <p>
 * The global checkpoint is maintained by the primary shard and is replicated to all the replicas
 * (via {@link GlobalCheckpointSyncAction}).
 */
public class GlobalCheckpointService extends AbstractIndexShardComponent {

    public static String GLOBAL_CHECKPOINT_KEY = "global_checkpoint";

    /**
     * This map holds the last known local checkpoint for every shard copy that's active.
     * All shard copies in this map participate in determining the global checkpoint
     * keyed by allocation ids
     */
    final private ObjectLongMap<String> activeLocalCheckpoints;

    /**
     * This map holds the last known local checkpoint for every initializing shard copy that's has been brought up
     * to speed through recovery. These shards are treated as valid copies and participate in determining the global
     * checkpoint.
     * <p>
     * Keyed by allocation ids.
     */
    final private ObjectLongMap<String> inSyncLocalCheckpoints; // keyed by allocation ids


    /**
     * This map holds the last known local checkpoint for every initializing shard copy that is still undergoing recovery.
     * These shards <strong>do not</strong> participate in determining the global checkpoint. This map is needed to make sure that when
     * shards are promoted to {@link #inSyncLocalCheckpoints} we use the highest known checkpoint, even if we index concurrently
     * while recovering the shard.
     * Keyed by allocation ids
     */
    final private ObjectLongMap<String> trackingLocalCheckpoint;

    private long globalCheckpoint;

    public GlobalCheckpointService(final ShardId shardId, final IndexSettings indexSettings) {
        this(shardId, indexSettings, SequenceNumbersService.UNASSIGNED_SEQ_NO);
    }

    public GlobalCheckpointService(final ShardId shardId, final IndexSettings indexSettings, final long globalCheckpoint) {
        super(shardId, indexSettings);
        activeLocalCheckpoints = new ObjectLongHashMap<>(1 + indexSettings.getNumberOfReplicas());
        inSyncLocalCheckpoints = new ObjectLongHashMap<>(indexSettings.getNumberOfReplicas());
        trackingLocalCheckpoint = new ObjectLongHashMap<>(indexSettings.getNumberOfReplicas());
        this.globalCheckpoint = globalCheckpoint;
    }

    /**
     * notifies the service of a local checkpoint. if the checkpoint is lower than the currently known one,
     * this is a noop. Last, if the allocation id is not yet known, it is ignored. This to prevent late
     * arrivals from shards that are removed to be re-added.
     */
    synchronized public void updateLocalCheckpoint(String allocationId, long localCheckpoint) {
        if (updateLocalCheckpointInMap(allocationId, localCheckpoint, activeLocalCheckpoints, "active")) {
            return;
        }
        if (updateLocalCheckpointInMap(allocationId, localCheckpoint, inSyncLocalCheckpoints, "inSync")) {
            return;
        }
        if (updateLocalCheckpointInMap(allocationId, localCheckpoint, trackingLocalCheckpoint, "tracking")) {
            return;
        }
        logger.trace("local checkpoint of [{}] ([{}]) wasn't found in any map. ignoring.", allocationId, localCheckpoint);
    }

    private boolean updateLocalCheckpointInMap(String allocationId, long localCheckpoint,
                                               ObjectLongMap<String> checkpointsMap, String name) {
        assert Thread.holdsLock(this);
        int indexOfKey = checkpointsMap.indexOf(allocationId);
        if (indexOfKey < 0) {
            return false;
        }
        long current = checkpointsMap.indexGet(indexOfKey);
        // nocommit: this can change when we introduces rollback/resync
        if (current < localCheckpoint) {
            checkpointsMap.indexReplace(indexOfKey, localCheckpoint);
            if (logger.isTraceEnabled()) {
                logger.trace("updated local checkpoint of [{}] to [{}] (type [{}])", allocationId, localCheckpoint,
                    name);
            }
        } else {
            logger.trace("skipping update local checkpoint [{}], current check point is higher " +
                    "(current [{}], incoming [{}], type [{}])",
                allocationId, current, localCheckpoint, allocationId);
        }
        return true;
    }

    /**
     * Scans through the currently known local checkpoints and updates the global checkpoint accordingly.
     *
     * @return true if the checkpoint has been updated or if it can not be updated since one of the local checkpoints
     * of one of the active allocations is not known.
     */
    synchronized public boolean updateCheckpointOnPrimary() {
        long minCheckpoint = Long.MAX_VALUE;
        if (activeLocalCheckpoints.isEmpty() && inSyncLocalCheckpoints.isEmpty()) {
            return false;
        }
        for (ObjectLongCursor<String> cp : activeLocalCheckpoints) {
            if (cp.value == SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                logger.trace("unknown local checkpoint for active allocationId [{}], requesting a sync", cp.key);
                return true;
            }
            minCheckpoint = Math.min(cp.value, minCheckpoint);
        }
        for (ObjectLongCursor<String> cp : inSyncLocalCheckpoints) {
            assert cp.value != SequenceNumbersService.UNASSIGNED_SEQ_NO :
                "in sync allocation ids can not have an unknown checkpoint (aId [" + cp.key + "])";
            minCheckpoint = Math.min(cp.value, minCheckpoint);
        }
        if (minCheckpoint < globalCheckpoint) {
            // nocommit: if this happens - do you we fail the shard?
            throw new IllegalStateException(shardId + " new global checkpoint [" + minCheckpoint
                + "] is lower than previous one [" + globalCheckpoint + "]");
        }
        if (globalCheckpoint != minCheckpoint) {
            logger.trace("global checkpoint updated to [{}]", minCheckpoint);
            globalCheckpoint = minCheckpoint;
            return true;
        }
        return false;
    }

    /**
     * gets the current global checkpoint. See java docs for {@link GlobalCheckpointService} for more details
     */
    synchronized public long getCheckpoint() {
        return globalCheckpoint;
    }

    /**
     * updates the global checkpoint on a replica shard (after it has been updated by the primary).
     */
    synchronized public void updateCheckpointOnReplica(long globalCheckpoint) {
        if (this.globalCheckpoint <= globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
            logger.trace("global checkpoint updated from primary to [{}]", globalCheckpoint);
        } else {
            // nocommit: fail the shard?
            throw new IllegalArgumentException("global checkpoint from primary should never decrease. current [" +
                this.globalCheckpoint + "], got [" + globalCheckpoint + "]");

        }
    }

    /**
     * Notifies the service of the current allocation ids in the cluster state. This method trims any shards that
     * have been removed and adds/promotes any active allocations to the {@link #activeLocalCheckpoints}.
     *
     * @param activeAllocationIds       the allocation ids of the currently active shard copies
     * @param initializingAllocationIds the allocation ids of the currently initializing shard copies
     */
    synchronized public void updateAllocationIdsFromMaster(Set<String> activeAllocationIds,
                                                           Set<String> initializingAllocationIds) {
        activeLocalCheckpoints.removeAll(key -> activeAllocationIds.contains(key) == false);
        for (String activeId : activeAllocationIds) {
            if (activeLocalCheckpoints.containsKey(activeId) == false) {
                long knownCheckpoint = trackingLocalCheckpoint.getOrDefault(activeId, SequenceNumbersService.UNASSIGNED_SEQ_NO);
                knownCheckpoint = inSyncLocalCheckpoints.getOrDefault(activeId, knownCheckpoint);
                activeLocalCheckpoints.put(activeId, knownCheckpoint);
                logger.trace("marking [{}] as active. known checkpoint [{}]", activeId, knownCheckpoint);
            }
        }
        inSyncLocalCheckpoints.removeAll(key -> initializingAllocationIds.contains(key) == false);
        trackingLocalCheckpoint.removeAll(key -> initializingAllocationIds.contains(key) == false);
        // add initializing shards to tracking
        for (String initID : initializingAllocationIds) {
            if (inSyncLocalCheckpoints.containsKey(initID)) {
                continue;
            }
            if (trackingLocalCheckpoint.containsKey(initID)) {
                continue;
            }
            trackingLocalCheckpoint.put(initID, SequenceNumbersService.UNASSIGNED_SEQ_NO);
            logger.trace("added [{}] to the tracking map due to a CS update", initID);

        }
    }

    /**
     * marks the allocationId as "in sync" with the primary shard. This should be called at the end of recovery
     * where the primary knows all operation bellow the global checkpoint have been completed on this shard.
     *
     * @param allocationId    allocationId of the recovering shard
     * @param localCheckpoint the local checkpoint of the shard in question
     */
    synchronized public void markAllocationIdAsInSync(String allocationId, long localCheckpoint) {
        if (trackingLocalCheckpoint.containsKey(allocationId) == false) {
            // master have change its mind and removed this allocation, ignore.
            return;
        }
        long current = trackingLocalCheckpoint.remove(allocationId);
        localCheckpoint = Math.max(current, localCheckpoint);
        logger.trace("marked [{}] as in sync with a local checkpoint of [{}]", allocationId, localCheckpoint);
        inSyncLocalCheckpoints.put(allocationId, localCheckpoint);
    }

    // for testing
    synchronized long getLocalCheckpointForAllocation(String allocationId) {
        if (activeLocalCheckpoints.containsKey(allocationId)) {
            return activeLocalCheckpoints.get(allocationId);
        }
        if (inSyncLocalCheckpoints.containsKey(allocationId)) {
            return inSyncLocalCheckpoints.get(allocationId);
        }
        if (trackingLocalCheckpoint.containsKey(allocationId)) {
            return trackingLocalCheckpoint.get(allocationId);
        }
        return SequenceNumbersService.UNASSIGNED_SEQ_NO;
    }

}
