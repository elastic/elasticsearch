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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.PrimaryContext;
import org.elasticsearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

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

    private long clusterStateVersion;

    /*
     * This map holds the last known local checkpoint for every active shard and initializing shard copies that has been brought up to speed
     * through recovery. These shards are treated as valid copies and participate in determining the global checkpoint. This map is keyed by
     * allocation IDs. All accesses to this set are guarded by a lock on this.
     */
    final ObjectLongMap<String> inSyncLocalCheckpoints;

    /*
     * This map holds the last known local checkpoint for initializing shards that are undergoing recovery. Such shards do not participate
     * in determining the global checkpoint. We must track these local checkpoints so that when a shard is activated we use the highest
     * known checkpoint.
     */
    final ObjectLongMap<String> trackingLocalCheckpoints;

    /*
     * This set contains allocation IDs for which there is a thread actively waiting for the local checkpoint to advance to at least the
     * current global checkpoint.
     */
    final Set<String> pendingInSync;

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
        assert globalCheckpoint >= SequenceNumbersService.UNASSIGNED_SEQ_NO : "illegal initial global checkpoint: " + globalCheckpoint;
        this.inSyncLocalCheckpoints = new ObjectLongHashMap<>(1 + indexSettings.getNumberOfReplicas());
        this.trackingLocalCheckpoints = new ObjectLongHashMap<>(indexSettings.getNumberOfReplicas());
        this.globalCheckpoint = globalCheckpoint;
        this.pendingInSync = new HashSet<>();
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. If the checkpoint is lower than
     * the currently known one, this is a no-op. If the allocation ID is not tracked, it is ignored. This is to prevent late arrivals from
     * shards that are removed to be re-added.
     *
     * @param allocationId    the allocation ID of the shard to update the local checkpoint for
     * @param localCheckpoint the local checkpoint for the shard
     */
    public synchronized void updateLocalCheckpoint(final String allocationId, final long localCheckpoint) {
        final boolean updated;
        if (updateLocalCheckpoint(allocationId, localCheckpoint, inSyncLocalCheckpoints, "in-sync")) {
            updated = true;
            updateGlobalCheckpointOnPrimary();
        } else if (updateLocalCheckpoint(allocationId, localCheckpoint, trackingLocalCheckpoints, "tracking")) {
            updated = true;
        } else {
            logger.trace("ignored local checkpoint [{}] of [{}], allocation ID is not tracked", localCheckpoint, allocationId);
            updated = false;
        }
        if (updated) {
            notifyAllWaiters();
        }
    }

    /**
     * Notify all threads waiting on the monitor on this tracker. These threads should be waiting for the local checkpoint on a specific
     * allocation ID to catch up to the global checkpoint.
     */
    @SuppressForbidden(reason = "Object#notifyAll waiters for local checkpoint advancement")
    private synchronized void notifyAllWaiters() {
        this.notifyAll();
    }

    /**
     * Update the local checkpoint for the specified allocation ID in the specified tracking map. If the checkpoint is lower than the
     * currently known one, this is a no-op. If the allocation ID is not tracked, it is ignored.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param localCheckpoint the local checkpoint for the shard
     * @param map the tracking map
     * @param reason the reason for the update (used for logging)
     * @return {@code true} if the local checkpoint was updated, otherwise {@code false} if this was a no-op
     */
    private boolean updateLocalCheckpoint(
            final String allocationId, final long localCheckpoint, ObjectLongMap<String> map, final String reason) {
        final int index = map.indexOf(allocationId);
        if (index >= 0) {
            final long current = map.indexGet(index);
            if (current < localCheckpoint) {
                map.indexReplace(index, localCheckpoint);
                logger.trace("updated local checkpoint of [{}] in [{}] from [{}] to [{}]", allocationId, reason, current, localCheckpoint);
            } else {
                logger.trace(
                        "skipped updating local checkpoint of [{}] in [{}] from [{}] to [{}], current checkpoint is higher",
                        allocationId,
                        reason,
                        current,
                        localCheckpoint);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Scans through the currently known local checkpoint and updates the global checkpoint accordingly.
     */
    private synchronized void updateGlobalCheckpointOnPrimary() {
        long minLocalCheckpoint = Long.MAX_VALUE;
        if (inSyncLocalCheckpoints.isEmpty() || !pendingInSync.isEmpty()) {
            return;
        }
        for (final ObjectLongCursor<String> localCheckpoint : inSyncLocalCheckpoints) {
            if (localCheckpoint.value == SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                logger.trace("unknown local checkpoint for active allocation ID [{}], requesting a sync", localCheckpoint.key);
                return;
            }
            minLocalCheckpoint = Math.min(localCheckpoint.value, minLocalCheckpoint);
        }
        assert minLocalCheckpoint != SequenceNumbersService.UNASSIGNED_SEQ_NO : "new global checkpoint must be assigned";
        if (minLocalCheckpoint < globalCheckpoint) {
            final String message =
                    String.format(
                            Locale.ROOT,
                            "new global checkpoint [%d] is lower than previous one [%d]",
                            minLocalCheckpoint,
                            globalCheckpoint);
            throw new IllegalStateException(message);
        }
        if (globalCheckpoint != minLocalCheckpoint) {
            logger.trace("global checkpoint updated to [{}]", minLocalCheckpoint);
            globalCheckpoint = minLocalCheckpoint;
        }
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public synchronized long getGlobalCheckpoint() {
        return globalCheckpoint;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint
     */
    synchronized void updateGlobalCheckpointOnReplica(final long globalCheckpoint) {
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
     * Notifies the service of the current allocation ids in the cluster state. This method trims any shards that have been removed.
     *
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
     * @param activeAllocationIds         the allocation IDs of the currently active shard copies
     * @param initializingAllocationIds   the allocation IDs of the currently initializing shard copies
     */
    public synchronized void updateAllocationIdsFromMaster(
            final long applyingClusterStateVersion, final Set<String> activeAllocationIds, final Set<String> initializingAllocationIds) {
        if (applyingClusterStateVersion < clusterStateVersion) {
            return;
        }

        clusterStateVersion = applyingClusterStateVersion;

        // remove shards whose allocation ID no longer exists
        inSyncLocalCheckpoints.removeAll(a -> !activeAllocationIds.contains(a) && !initializingAllocationIds.contains(a));

        // add any new active allocation IDs
        for (final String a : activeAllocationIds) {
            if (!inSyncLocalCheckpoints.containsKey(a)) {
                final long localCheckpoint = trackingLocalCheckpoints.getOrDefault(a, SequenceNumbersService.UNASSIGNED_SEQ_NO);
                inSyncLocalCheckpoints.put(a, localCheckpoint);
                logger.trace("marked [{}] as in-sync with local checkpoint [{}] via cluster state update from master", a, localCheckpoint);
            }
        }

        trackingLocalCheckpoints.removeAll(a -> !initializingAllocationIds.contains(a));
        for (final String a : initializingAllocationIds) {
            if (inSyncLocalCheckpoints.containsKey(a)) {
                /*
                 * This can happen if we mark the allocation ID as in sync at the end of recovery before seeing a cluster state update from
                 * marking the shard as active.
                 */
                continue;
            }
            if (trackingLocalCheckpoints.containsKey(a)) {
                // we are already tracking this allocation ID
                continue;
            }
            // this is a new allocation ID
            trackingLocalCheckpoints.put(a, SequenceNumbersService.UNASSIGNED_SEQ_NO);
            logger.trace("tracking [{}] via cluster state update from master", a);
        }

        updateGlobalCheckpointOnPrimary();
    }

    /**
     * Get the primary context for the shard. This includes the state of the global checkpoint tracker.
     *
     * @return the primary context
     */
    synchronized PrimaryContext primaryContext() {
        final ObjectLongMap<String> inSyncLocalCheckpoints = new ObjectLongHashMap<>(this.inSyncLocalCheckpoints);
        final ObjectLongMap<String> trackingLocalCheckpoints = new ObjectLongHashMap<>(this.trackingLocalCheckpoints);
        return new PrimaryContext(clusterStateVersion, inSyncLocalCheckpoints, trackingLocalCheckpoints);
    }

    /**
     * Updates the known allocation IDs and the local checkpoints for the corresponding allocations from a primary relocation source.
     *
     * @param primaryContext the primary context
     */
    synchronized void updateAllocationIdsFromPrimaryContext(final PrimaryContext primaryContext) {
        /*
         * We are gathered here today to witness the relocation handoff transferring knowledge from the relocation source to the relocation
         * target. We need to consider the possibility that the version of the cluster state on the relocation source when the primary
         * context was sampled is different than the version of the cluster state on the relocation target at this exact moment. We define
         * the following values:
         *  - version(source) = the cluster state version on the relocation source used to ensure a minimum cluster state version on the
         *    relocation target
         *  - version(context) = the cluster state version on the relocation source when the primary context was sampled
         *  - version(target) = the current cluster state version on the relocation target
         *
         * We know that version(source) <= version(target) and version(context) < version(target), version(context) = version(target), and
         * version(target) < version(context) are all possibilities.
         *
         * The case of version(context) = version(target) causes no issues as in this case the knowledge of the in-sync and initializing
         * shards the target receives from the master will be equal to the knowledge of the in-sync and initializing shards the target
         * receives from the relocation source via the primary context.
         *
         * In the case when version(context) < version(target) or version(target) < version(context), we first consider shards that could be
         * contained in the primary context but not contained in the cluster state applied on the target.
         *
         * Suppose there is such a shard and that it is an in-sync shard. However, marking a shard as in-sync requires an operation permit
         * on the primary shard. Such a permit can not be obtained after the relocation handoff has started as the relocation handoff blocks
         * all operations. Therefore, there can not be such a shard that is marked in-sync.
         *
         * Now consider the case of an initializing shard that is contained in the primary context but not contained in the cluster state
         * applied on the target.
         *
         * If version(context) < version(target) it means that the shard has been removed by a later cluster state update that is already
         * applied on the target and we only need to ensure that we do not add it to the tracking map on the target. The call to
         * GlobalCheckpointTracker#updateLocalCheckpoint(String, long) is a no-op for such shards and this is safe.
         *
         * If version(target) < version(context) it means that the shard has started initializing by a later cluster state update has not
         * yet arrived on the target. However, there is a delay on recoveries before we ensure that version(source) <= version(target).
         * Therefore, such a shard can never initialize from the relocation source and will have to await the handoff completing. As such,
         * these shards are not problematic.
         *
         * Now we consider shards that are contained in the cluster state applied on the target but not contained in the primary context.
         *
         * If version(context) < version(target) it means that the target has learned of an initializing shard that the source is not aware
         * of. As explained above, this initialization can only succeed after the relocation is complete, and only with the target as the
         * source of the recovery.
         *
         * Otherwise, if version(target) < version(context) it only means that the global checkpoint on the target will be held back until a
         * later cluster state update arrives because the target will not learn of the removal until later.
         *
         * In both cases, no calls to update the local checkpoint for such shards will be made. This case is safe too.
         */

        if (primaryContext.clusterStateVersion() > clusterStateVersion) {
            final Set<String> activeAllocationIds =
                    new HashSet<>(Arrays.asList(primaryContext.inSyncLocalCheckpoints().keys().toArray(String.class)));
            final Set<String> initializingAllocationIds =
                    new HashSet<>(Arrays.asList(primaryContext.trackingLocalCheckpoints().keys().toArray(String.class)));
            updateAllocationIdsFromMaster(primaryContext.clusterStateVersion(), activeAllocationIds, initializingAllocationIds);
        }

        for (final ObjectLongCursor<String> cursor : primaryContext.inSyncLocalCheckpoints()) {
            updateLocalCheckpoint(cursor.key, cursor.value);
            assert cursor.value >= globalCheckpoint
                    : "local checkpoint [" + cursor.value + "] violates being at least the global checkpoint [" + globalCheckpoint + "]";
            if (trackingLocalCheckpoints.containsKey(cursor.key)) {
                final long current = trackingLocalCheckpoints.remove(cursor.key);
                inSyncLocalCheckpoints.put(cursor.key, current);
            }
        }

        for (final ObjectLongCursor<String> cursor : primaryContext.trackingLocalCheckpoints()) {
            updateLocalCheckpoint(cursor.key, cursor.value);
        }
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. This method will block until the local checkpoint
     * on the specified shard advances above the current global checkpoint.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     *
     * @throws InterruptedException if the thread is interrupted waiting for the local checkpoint on the shard to advance
     */
    public synchronized void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        if (!trackingLocalCheckpoints.containsKey(allocationId)) {
            /*
             * This can happen if the recovery target has been failed and the cluster state update from the master has triggered removing
             * this allocation ID from the tracking map but this recovery thread has not yet been made aware that the recovery is
             * cancelled.
             */
            return;
        }

        updateLocalCheckpoint(allocationId, localCheckpoint, trackingLocalCheckpoints, "tracking");
        if (!pendingInSync.add(allocationId)) {
            throw new IllegalStateException("there is already a pending sync in progress for allocation ID [" + allocationId + "]");
        }
        try {
            waitForAllocationIdToBeInSync(allocationId);
        } finally {
            pendingInSync.remove(allocationId);
            updateGlobalCheckpointOnPrimary();
        }
    }

    /**
     * Wait for knowledge of the local checkpoint for the specified allocation ID to advance to the global checkpoint. Global checkpoint
     * advancement is blocked while there are any allocation IDs waiting to catch up to the global checkpoint.
     *
     * @param allocationId the allocation ID
     * @throws InterruptedException if this thread was interrupted before of during waiting
     */
    private synchronized void waitForAllocationIdToBeInSync(final String allocationId) throws InterruptedException {
        while (true) {
            /*
             * If the allocation has been cancelled and so removed from the tracking map from a cluster state update from the master it
             * means that this recovery will be cancelled; we are here on a cancellable recovery thread and so this thread will throw an
             * interrupted exception as soon as it tries to wait on the monitor.
             */
            final long current = trackingLocalCheckpoints.getOrDefault(allocationId, Long.MIN_VALUE);
            if (current >= globalCheckpoint) {
                logger.trace("marked [{}] as in-sync with local checkpoint [{}]", allocationId, current);
                trackingLocalCheckpoints.remove(allocationId);
                /*
                 * This is prematurely adding the allocation ID to the in-sync map as at this point recovery is not yet finished and could
                 * still abort. At this point we will end up with a shard in the in-sync map holding back the global checkpoint because the
                 * shard never recovered and we would have to wait until either the recovery retries and completes successfully, or the
                 * master fails the shard and issues a cluster state update that removes the shard from the set of active allocation IDs.
                 */
                inSyncLocalCheckpoints.put(allocationId, current);
                break;
            } else {
                waitForLocalCheckpointToAdvance();
            }
        }
    }

    /**
     * Wait for the local checkpoint to advance to the global checkpoint.
     *
     * @throws InterruptedException if this thread was interrupted before of during waiting
     */
    @SuppressForbidden(reason = "Object#wait for local checkpoint advancement")
    private synchronized void waitForLocalCheckpointToAdvance() throws InterruptedException {
        this.wait();
    }

    /**
     * Check if there are any recoveries pending in-sync.
     *
     * @return {@code true} if there is at least one shard pending in-sync, otherwise false
     */
    public boolean pendingInSync() {
        return !pendingInSync.isEmpty();
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
        return SequenceNumbersService.UNASSIGNED_SEQ_NO;
    }

}
