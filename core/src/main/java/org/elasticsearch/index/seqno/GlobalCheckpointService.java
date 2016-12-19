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

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.index.seqno.SequenceNumbersService.UNASSIGNED_SEQ_NO;

/**
 * A shard component that is responsible of tracking the global checkpoint. The global checkpoint
 * is the highest seq_no for which all lower (or equal) seq_no have been processed on all shards that
 * are currently active. Since shards count as "active" when the master starts them, and before this primary shard
 * has been notified of this fact, we also include shards that have completed recovery. These shards have received
 * all old operations via the recovery mechanism and are kept up to date by the various replications actions. The set
 * of shards that are taken into account for the global checkpoint calculation are called the "in sync" shards.
 *
 * <p>
 * The global checkpoint is maintained by the primary shard and is replicated to all the replicas
 * (via {@link GlobalCheckpointSyncAction}).
 */
public class GlobalCheckpointService extends AbstractIndexShardComponent {


    /**
     * This map holds the last known local checkpoint for every active shard and initializing shard copies that has been brought up
     * to speed through recovery. These shards are treated as valid copies and participate in determining the global
     * checkpoint.
     * <p>
     * Keyed by allocation ids.
     */
    private final ObjectLongMap<String> inSyncLocalCheckpoints; // keyed by allocation ids

    /**
     * This set holds the last set of known valid allocation ids as received by the master. This is important to make sure
     * shard that are failed or relocated are cleaned up from {@link #inSyncLocalCheckpoints} and do not hold the global
     * checkpoint back
     */
    private final Set<String> assignedAllocationIds;

    private long globalCheckpoint;

    /**
     * Initialize the global checkpoint service. The {@code globalCheckpoint}
     * should be set to the last known global checkpoint for this shard, or
     * {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}.
     *
     * @param shardId          the shard this service is providing tracking
     *                         local checkpoints for
     * @param indexSettings    the index settings
     * @param globalCheckpoint the last known global checkpoint for this shard,
     *                         or
     *                         {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}
     */
    GlobalCheckpointService(final ShardId shardId, final IndexSettings indexSettings, final long globalCheckpoint) {
        super(shardId, indexSettings);
        assert globalCheckpoint >= UNASSIGNED_SEQ_NO : "illegal initial global checkpoint:" + globalCheckpoint;
        inSyncLocalCheckpoints = new ObjectLongHashMap<>(1 + indexSettings.getNumberOfReplicas());
        assignedAllocationIds = new HashSet<>(1 + indexSettings.getNumberOfReplicas());
        this.globalCheckpoint = globalCheckpoint;
    }

    /**
     * Notifies the service of a local checkpoint. If the checkpoint is lower than the currently known one,
     * this is a noop. Last, if the allocation id is not in sync, it is ignored. This to prevent late
     * arrivals from shards that are removed to be re-added.
     */
    public synchronized void updateLocalCheckpoint(String allocationId, long localCheckpoint) {
        final int indexOfKey = inSyncLocalCheckpoints.indexOf(allocationId);
        if (indexOfKey >= 0) {
            final long current = inSyncLocalCheckpoints.indexGet(indexOfKey);

            if (current < localCheckpoint) {
                inSyncLocalCheckpoints.indexReplace(indexOfKey, localCheckpoint);
                if (logger.isTraceEnabled()) {
                    logger.trace("updated local checkpoint of [{}] to [{}] (was [{}])", allocationId, localCheckpoint, current);
                }
            } else {
                logger.trace("skipping update of local checkpoint [{}], current checkpoint is higher " +
                        "(current [{}], incoming [{}], type [{}])",
                    allocationId, current, localCheckpoint, allocationId);
            }
        } else {
            logger.trace("[{}] isn't marked as in sync. ignoring local checkpoint of [{}].", allocationId, localCheckpoint);
        }
    }

    /**
     * Scans through the currently known local checkpoints and updates the global checkpoint accordingly.
     *
     * @return true if the checkpoint has been updated or if it can not be updated since one of the local checkpoints
     * of one of the active allocations is not known.
     */
    synchronized boolean updateCheckpointOnPrimary() {
        long minCheckpoint = Long.MAX_VALUE;
        if (inSyncLocalCheckpoints.isEmpty()) {
            return false;
        }
        for (ObjectLongCursor<String> cp : inSyncLocalCheckpoints) {
            if (cp.value == UNASSIGNED_SEQ_NO) {
                logger.trace("unknown local checkpoint for active allocationId [{}], requesting a sync", cp.key);
                return true;
            }
            minCheckpoint = Math.min(cp.value, minCheckpoint);
        }
        if (minCheckpoint < globalCheckpoint) {
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
    public synchronized long getCheckpoint() {
        return globalCheckpoint;
    }

    /**
     * updates the global checkpoint on a replica shard (after it has been updated by the primary).
     */
    synchronized void updateCheckpointOnReplica(long globalCheckpoint) {
        /*
         * The global checkpoint here is a local knowledge which is updated under the mandate of the primary. It can happen that the primary
         * information is lagging compared to a replica (e.g., if a replica is promoted to primary but has stale info relative to other
         * replica shards). In these cases, the local knowledge of the global checkpoint could be higher than sync from the lagging primary.
         */
        if (this.globalCheckpoint <= globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
            logger.trace("global checkpoint updated from primary to [{}]", globalCheckpoint);
        }
    }

    /**
     * Notifies the service of the current allocation ids in the cluster state. This method trims any shards that
     * have been removed.
     *
     * @param activeAllocationIds       the allocation ids of the currently active shard copies
     * @param initializingAllocationIds the allocation ids of the currently initializing shard copies
     */
    public synchronized void updateAllocationIdsFromMaster(Set<String> activeAllocationIds,
                                                           Set<String> initializingAllocationIds) {
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
     * marks the allocationId as "in sync" with the primary shard. This should be called at the end of recovery
     * where the primary knows all operation below the global checkpoint have been completed on this shard.
     *
     * @param allocationId    allocationId of the recovering shard
     */
    public synchronized void markAllocationIdAsInSync(String allocationId) {
        if (assignedAllocationIds.contains(allocationId) == false) {
            // master have change it's mind and removed this allocation, ignore.
            return;
        }
        logger.trace("marked [{}] as in sync", allocationId);
        inSyncLocalCheckpoints.put(allocationId, UNASSIGNED_SEQ_NO);
    }

    // for testing
    synchronized long getLocalCheckpointForAllocation(String allocationId) {
        if (inSyncLocalCheckpoints.containsKey(allocationId)) {
            return inSyncLocalCheckpoints.get(allocationId);
        }
        return UNASSIGNED_SEQ_NO;
    }

}
