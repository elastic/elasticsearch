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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.index.seqno.SequenceNumbersService.UNASSIGNED_SEQ_NO;

/**
 * This class is responsible of tracking the global checkpoint. The global checkpoint is the highest sequence number for which all lower (or
 * equal) sequence number have been processed on all shards that are currently active. Since shards count as "active" when the master starts
 * them, and before this primary shard has been notified of this fact, we also include shards that have completed recovery. These shards
 * have received all old operations via the recovery mechanism and are kept up to date by the various replications actions. The set of
 * shards that are taken into account for the global checkpoint calculation are called the "in-sync shards".
 * <p>
 * The global checkpoint is maintained by the primary shard and is replicated to all the replicas (via {@link GlobalCheckpointSyncAction}).
 */
public class GlobalCheckpointTracker extends AbstractIndexShardComponent {

    /*
     * This map holds the last known local checkpoint for every active shard and initializing shard copies that has been brought up to speed
     * through recovery. These shards are treated as valid copies and participate in determining the global checkpoint. This map is keyed by
     * allocation IDs. All accesses to this set are guarded by a lock on this.
     */
    private final ObjectLongMap<String> inSyncLocalCheckpoints;

    /*
     * This set holds the last set of known valid allocation ids as received by the master. This is important to make sure shard that are
     * failed or relocated are cleaned up from {@link #inSyncLocalCheckpoints} and do not hold the global checkpoint back. All accesses to
     * this set are guarded by a lock on this.
     */
    private final Set<String> assignedAllocationIds;

    /*
     * The current global checkpoint for this shard. Note that this field is guarded by a lock on this and thus this field does not need to
     * be volatile.
     */
    private long globalCheckpoint;

    /**
     * Initialize the global checkpoint service. The specified global checkpoint should be set to the last known global checkpoint, or
     * {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}.
     *
     * @param shardId          the shard ID
     * @param indexSettings    the index settings
     * @param globalCheckpoint the last known global checkpoint for this shard, or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}
     */
    GlobalCheckpointTracker(final ShardId shardId, final IndexSettings indexSettings, final long globalCheckpoint) {
        super(shardId, indexSettings);
        assert globalCheckpoint >= UNASSIGNED_SEQ_NO : "illegal initial global checkpoint: " + globalCheckpoint;
        inSyncLocalCheckpoints = new ObjectLongHashMap<>(1 + indexSettings.getNumberOfReplicas());
        assignedAllocationIds = new HashSet<>(1 + indexSettings.getNumberOfReplicas());
        this.globalCheckpoint = globalCheckpoint;
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. If the checkpoint is lower than
     * the currently known one, this is a no-op. If the allocation ID is not in sync, it is ignored. This is to prevent late arrivals from
     * shards that are removed to be re-added.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param checkpoint   the local checkpoint for the shard
     */
    public synchronized void updateLocalCheckpoint(final String allocationId, final long checkpoint) {
        final int indexOfKey = inSyncLocalCheckpoints.indexOf(allocationId);
        if (indexOfKey >= 0) {
            final long current = inSyncLocalCheckpoints.indexGet(indexOfKey);
            if (current < checkpoint) {
                inSyncLocalCheckpoints.indexReplace(indexOfKey, checkpoint);
                if (logger.isTraceEnabled()) {
                    logger.trace("updated local checkpoint of [{}] to [{}] (was [{}])", allocationId, checkpoint, current);
                }
            } else {
                logger.trace(
                    "skipping update of local checkpoint [{}], current checkpoint is higher (current [{}], incoming [{}], type [{}])",
                    allocationId,
                    current,
                    checkpoint,
                    allocationId);
            }
        } else {
            logger.trace("[{}] isn't marked as in sync. ignoring local checkpoint of [{}].", allocationId, checkpoint);
        }
    }

    /**
     * Scans through the currently known local checkpoint and updates the global checkpoint accordingly.
     *
     * @return {@code true} if the checkpoint has been updated or if it can not be updated since one of the local checkpoints of one of the
     * active allocations is not known.
     */
    synchronized boolean updateCheckpointOnPrimary() {
        long minCheckpoint = Long.MAX_VALUE;
        if (inSyncLocalCheckpoints.isEmpty()) {
            return false;
        }
        for (final ObjectLongCursor<String> cp : inSyncLocalCheckpoints) {
            if (cp.value == UNASSIGNED_SEQ_NO) {
                logger.trace("unknown local checkpoint for active allocationId [{}], requesting a sync", cp.key);
                return true;
            }
            minCheckpoint = Math.min(cp.value, minCheckpoint);
        }
        if (minCheckpoint < globalCheckpoint) {
            final String message =
                String.format(Locale.ROOT, "new global checkpoint [%d] is lower than previous one [%d]", minCheckpoint, globalCheckpoint);
            throw new IllegalStateException(message);
        }
        if (globalCheckpoint != minCheckpoint) {
            logger.trace("global checkpoint updated to [{}]", minCheckpoint);
            globalCheckpoint = minCheckpoint;
            return true;
        }
        return false;
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public synchronized long getCheckpoint() {
        return globalCheckpoint;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param checkpoint the global checkpoint
     */
    synchronized void updateCheckpointOnReplica(final long checkpoint) {
        /*
         * The global checkpoint here is a local knowledge which is updated under the mandate of the primary. It can happen that the primary
         * information is lagging compared to a replica (e.g., if a replica is promoted to primary but has stale info relative to other
         * replica shards). In these cases, the local knowledge of the global checkpoint could be higher than sync from the lagging primary.
         */
        if (this.globalCheckpoint <= checkpoint) {
            this.globalCheckpoint = checkpoint;
            logger.trace("global checkpoint updated from primary to [{}]", checkpoint);
        }
    }

    /**
     * Notifies the service of the current allocation ids in the cluster state. This method trims any shards that have been removed.
     *
     * @param activeAllocationIds       the allocation IDs of the currently active shard copies
     * @param initializingAllocationIds the allocation IDs of the currently initializing shard copies
     */
    public synchronized void updateAllocationIdsFromMaster(final Set<String> activeAllocationIds,
                                                           final Set<String> initializingAllocationIds) {
        assignedAllocationIds.removeIf(
            aId -> activeAllocationIds.contains(aId) == false && initializingAllocationIds.contains(aId) == false);
        assignedAllocationIds.addAll(activeAllocationIds);
        assignedAllocationIds.addAll(initializingAllocationIds);
        for (String activeId : activeAllocationIds) {
            if (inSyncLocalCheckpoints.containsKey(activeId) == false) {
                inSyncLocalCheckpoints.put(activeId, UNASSIGNED_SEQ_NO);
            }
        }
        inSyncLocalCheckpoints.removeAll(key -> assignedAllocationIds.contains(key) == false);
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. This should be called at the end of recovery where
     * the primary knows all operations below the global checkpoint have been completed on this shard.
     *
     * @param allocationId the allocation ID of the shard to mark as in-sync
     */
    public synchronized void markAllocationIdAsInSync(final String allocationId) {
        if (assignedAllocationIds.contains(allocationId) == false) {
            // master has removed this allocation, ignore
            return;
        }
        logger.trace("marked [{}] as in sync", allocationId);
        inSyncLocalCheckpoints.put(allocationId, UNASSIGNED_SEQ_NO);
    }

    /**
     * Returns the local checkpoint for the shard with the specified allocation ID, or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO} if
     * the shard is not in-sync.
     *
     * @param allocationId the allocation ID of the shard to obtain the local checkpoint for
     * @return the local checkpoint, or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}
     */
    synchronized long getLocalCheckpointForAllocationId(final String allocationId) {
        if (inSyncLocalCheckpoints.containsKey(allocationId)) {
            return inSyncLocalCheckpoints.get(allocationId);
        }
        return UNASSIGNED_SEQ_NO;
    }

}
