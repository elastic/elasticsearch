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

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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

    /**
     * The global checkpoint tracker can operate in two modes:
     * - primary: this shard is in charge of collecting local checkpoint information from all shard copies and computing the global
     *            checkpoint based on the local checkpoints of all in-sync shard copies.
     * - replica: this shard receives global checkpoint information from the primary (see {@link #updateGlobalCheckpointOnReplica}).
     *
     * When a shard is initialized (be it a primary or replica), it initially operates in replica mode. The global checkpoint tracker is
     * then switched to primary mode in the following three scenarios:
     *
     * - An initializing primary shard that is not a relocation target is moved to primary mode (using {@link #initializeAsPrimary}) once
     *   the shard becomes active.
     * - An active replica shard is moved to primary mode (using {@link #initializeAsPrimary}) once it is promoted to primary.
     * - A primary relocation target is moved to primary mode (using {@link #initializeWithPrimaryContext}) during the primary relocation
     *   handoff. If the target shard is successfully initialized in primary mode, the source shard of a primary relocation is then moved
     *   to replica mode (using {@link #completeRelocationHandOff}), as the relocation target will be in charge of the global checkpoint
     *   computation from that point on.
     */
    boolean primaryMode;
    /**
     * Boolean flag that indicates if a relocation handoff is in progress. A handoff is started by calling {@link #startRelocationHandOff}
     * and is finished by either calling {@link #completeRelocationHandOff} or {@link #abortRelocationHandOff}, depending on whether the
     * handoff was successful or not. During the handoff, which has as main objective to transfer the internal state of the global
     * checkpoint tracker from the relocation source to the target, the list of in-sync shard copies cannot grow, otherwise the relocation
     * target might miss this information and increase the global checkpoint to eagerly. As consequence, some of the methods in this class
     * are not allowed to be called while a handoff is in progress, in particular {@link #markAllocationIdAsInSync}.
     *
     * A notable exception to this is the method {@link #updateFromMaster}, which is still allowed to be called during a relocation handoff.
     * The reason for this is that the handoff might fail and can be aborted (using {@link #abortRelocationHandOff}), in which case
     * it is important that the global checkpoint tracker does not miss any state updates that might happened during the handoff attempt.
     * This means, however, that the global checkpoint can still advance after the primary relocation handoff has been initiated, but only
     * because the master could have failed some of the in-sync shard copies and marked them as stale. That is ok though, as this
     * information is conveyed through cluster state updates, and the new primary relocation target will also eventually learn about those.
     */
    boolean handOffInProgress;

    /**
     * The global checkpoint tracker relies on the property that cluster state updates are applied in-order. After transferring a primary
     * context from the primary relocation source to the target and initializing the target, it is possible for the target to apply a
     * cluster state that is older than the one upon which the primary context was based. If we allowed this old cluster state
     * to influence the list of in-sync shard copies here, this could possibly remove such an in-sync copy from the internal structures
     * until the newer cluster state were to be applied, which would unsafely advance the global checkpoint. This field thus captures
     * the version of the last applied cluster state to ensure in-order updates.
     */
    long appliedClusterStateVersion;

    /**
     * Local checkpoint information for all shard copies that are tracked. Has an entry for all shard copies that are either initializing
     * and / or in-sync, possibly also containing information about unassigned in-sync shard copies. The information that is tracked for
     * each shard copy is explained in the docs for the {@link LocalCheckPointState} class.
     */
    Map<String, LocalCheckPointState> localCheckpoints;

    /**
     * The global checkpoint:
     * - computed based on local checkpoints, if the tracker is in primary mode
     * - received from the primary, if the tracker is in replica mode
     */
    long globalCheckpoint;

    public static class LocalCheckPointState implements Writeable {

        /**
         * the last local checkpoint information that we have for this shard
         */
        long localCheckPoint;
        /**
         * whether this shard is treated as in-sync and thus contributes to the global checkpoint calculation
         */
        boolean inSync;
        /**
         * whether this shard is considered as initializing by the master
         */
        boolean masterInitializing;
        /**
         * whether this shard is considered as in-sync by the master
         */
        boolean masterInSync;
        /**
         * whether this shard currently blocks the globalCheckPoint from advancing, used when transitioning a shard copy to in-sync
         * in order to give it a chance to catch up with the current global checkpoint.
         */
        boolean blockGCPAdvance;

        public LocalCheckPointState(long localCheckPoint, boolean inSync, boolean masterInitializing, boolean masterInSync,
                                    boolean blockGCPAdvance) {
            this.localCheckPoint = localCheckPoint;
            this.masterInitializing = masterInitializing;
            this.masterInSync = masterInSync;
            this.inSync = inSync;
            this.blockGCPAdvance = blockGCPAdvance;
        }

        public LocalCheckPointState(StreamInput in) throws IOException {
            this.localCheckPoint = in.readZLong();
            this.inSync = in.readBoolean();
            this.masterInitializing = in.readBoolean();
            this.masterInSync = in.readBoolean();
            this.blockGCPAdvance = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(localCheckPoint);
            out.writeBoolean(inSync);
            out.writeBoolean(masterInitializing);
            out.writeBoolean(masterInSync);
            out.writeBoolean(blockGCPAdvance);
        }

        /**
         * Returns a full copy of this object
         */
        public LocalCheckPointState copy() {
            return new LocalCheckPointState(localCheckPoint, inSync, masterInitializing, masterInSync, blockGCPAdvance);
        }

        public long getLocalCheckPoint() {
            return localCheckPoint;
        }

        @Override
        public String toString() {
            return "RecoveryHandoffPrimaryContextRequest{" +
                "localCheckPoint=" + localCheckPoint +
                ", inSync=" + inSync +
                ", masterInitializing=" + masterInitializing +
                ", masterInSync=" + masterInSync +
                ", blockGCPAdvance=" + blockGCPAdvance +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LocalCheckPointState that = (LocalCheckPointState) o;

            if (localCheckPoint != that.localCheckPoint) return false;
            if (inSync != that.inSync) return false;
            if (masterInitializing != that.masterInitializing) return false;
            if (masterInSync != that.masterInSync) return false;
            return blockGCPAdvance == that.blockGCPAdvance;
        }

        @Override
        public int hashCode() {
            int result = (int) (localCheckPoint ^ (localCheckPoint >>> 32));
            result = 31 * result + (inSync ? 1 : 0);
            result = 31 * result + (masterInitializing ? 1 : 0);
            result = 31 * result + (masterInSync ? 1 : 0);
            result = 31 * result + (blockGCPAdvance ? 1 : 0);
            return result;
        }
    }

    /**
     * Class invariant that should hold before and after every invocation of public methods on this class. As Java lacks implication
     * as a logical operator, many of the invariants are written under the form (!A || B), they should be read as (A implies B) however.
     */
    private boolean invariant() {
        /**
         * local checkpoints are only tracked in primary mode
         */
        assert primaryMode == !localCheckpoints.isEmpty();
        /**
         * the last applied cluster state version is only tracked in primary mode
         */
        assert primaryMode == (appliedClusterStateVersion != -1L);
        /**
         * relocation handoff can only occur in primary mode
         */
        assert !handOffInProgress || primaryMode;
        /**
         * there is at least one in-sync shard copy when the global checkpoint tracker operates in primary mode (i.e. the shard itself)
         */
        assert !primaryMode || localCheckpoints.values().stream().anyMatch(lcps -> lcps.inSync);
        /**
         * during relocation handoff there are no entries blocking global checkpoint advancement
         */
        assert !handOffInProgress || localCheckpoints.values().stream().noneMatch(lcps -> lcps.blockGCPAdvance);
        /**
         * the computed global checkpoint is always up-to-date
         */
        assert !primaryMode || globalCheckpoint == computeGlobalCheckPoint(localCheckpoints.values(), globalCheckpoint);

        for (LocalCheckPointState lcps : localCheckpoints.values()) {
            /**
             * shards that are neither considered as initializing nor as in-sync by the master are not tracked
             */
            assert lcps.masterInitializing || lcps.masterInSync;
            /**
             * if the master considers a shard as in-sync, then the global checkpoint tracker should do so as well
             */
            assert !lcps.masterInSync || lcps.inSync;
            /**
             * blocking global checkpoint advancement can only be done for initializing shards that are not in-sync
             */
            assert !lcps.blockGCPAdvance || (lcps.masterInitializing && !lcps.masterInSync && !lcps.inSync);
        }
        return true;
    }

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
        this.primaryMode = false;
        this.handOffInProgress = false;
        this.appliedClusterStateVersion = -1L;
        this.globalCheckpoint = globalCheckpoint;
        this.localCheckpoints = new HashMap<>(1 + indexSettings.getNumberOfReplicas());
        assert invariant();
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
    public synchronized void updateGlobalCheckpointOnReplica(final long globalCheckpoint) {
        assert invariant();
        assert primaryMode == false;
        /*
         * The global checkpoint here is a local knowledge which is updated under the mandate of the primary. It can happen that the primary
         * information is lagging compared to a replica (e.g., if a replica is promoted to primary but has stale info relative to other
         * replica shards). In these cases, the local knowledge of the global checkpoint could be higher than sync from the lagging primary.
         */
        if (this.globalCheckpoint <= globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
            logger.trace("global checkpoint updated from primary to [{}]", globalCheckpoint);
        }
        assert invariant();
    }

    /**
     * Initializes the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary activation or promotion.
     *
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
     * @param inSyncAllocationIds         the allocation IDs of the currently in-sync shard copies
     * @param initializingAllocationIds   the allocation IDs of the currently initializing shard copies
     */
    public synchronized void initializeAsPrimary(final long applyingClusterStateVersion, final Set<String> inSyncAllocationIds,
                                                 final Set<String> initializingAllocationIds) {
        assert invariant();
        assert primaryMode == false;
        primaryMode = true;
        for (String allocationId : Sets.union(inSyncAllocationIds, initializingAllocationIds)) {
            long localCheckPoint = SequenceNumbersService.UNASSIGNED_SEQ_NO;
            boolean masterInSync = inSyncAllocationIds.contains(allocationId);
            boolean inSync = masterInSync;
            boolean masterInitializing = initializingAllocationIds.contains(allocationId);
            boolean blockGCPAdvance = false;
            localCheckpoints.put(allocationId,
                new LocalCheckPointState(localCheckPoint, inSync, masterInitializing, masterInSync, blockGCPAdvance));
        }
        appliedClusterStateVersion = applyingClusterStateVersion;
        assert invariant();
    }

    /**
     * Notifies the tracker of the current allocation IDs in the cluster state.
     *
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
     * @param inSyncAllocationIds         the allocation IDs of the currently in-sync shard copies
     * @param initializingAllocationIds   the allocation IDs of the currently initializing shard copies
     */
    public synchronized void updateFromMaster(final long applyingClusterStateVersion, final Set<String> inSyncAllocationIds,
                                              final Set<String> initializingAllocationIds) {
        assert invariant();
        assert primaryMode;
        if (applyingClusterStateVersion > appliedClusterStateVersion) {
            // check that the master does not fabricate new in-sync entries out of thin air
            assert inSyncAllocationIds.stream().allMatch(
                inSyncId -> localCheckpoints.containsKey(inSyncId) && localCheckpoints.get(inSyncId).inSync);
            // remove entries which don't exist on master
            boolean removedEntries = localCheckpoints.keySet().removeIf(
                aid -> !inSyncAllocationIds.contains(aid) && !initializingAllocationIds.contains(aid));
            // updates entries where local info has masterInSync == false but now master has it insync
            // this happens after a shard that recovered from this primary was successfully activated on master
            for (String inSyncId : inSyncAllocationIds) {
                LocalCheckPointState lcps = localCheckpoints.get(inSyncId);
                if (lcps.masterInSync == false) {
                    assert lcps.masterInitializing && lcps.inSync && !initializingAllocationIds.contains(inSyncId);
                    lcps.masterInSync = true;
                    lcps.masterInitializing = false;
                }
            }
            // add new initializingIds that are missing locally. these are fresh shard copies - and not in-sync
            for (String initializingId : initializingAllocationIds) {
                if (localCheckpoints.containsKey(initializingId) == false) {
                    long localCheckPoint = SequenceNumbersService.UNASSIGNED_SEQ_NO;
                    assert inSyncAllocationIds.contains(initializingId) == false;
                    boolean inSync = false;
                    boolean masterInitializing = true;
                    boolean masterInSync = false;
                    boolean blockGCPAdvance = false;
                    localCheckpoints.put(initializingId,
                        new LocalCheckPointState(localCheckPoint, inSync, masterInitializing, masterInSync, blockGCPAdvance));
                }
            }
            appliedClusterStateVersion = applyingClusterStateVersion;
            if (removedEntries) {
                updateGlobalCheckpointOnPrimary();
            }
        }
        assert invariant();
    }

    /**
     * Called when the recovery process for a shard is ready to open the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     */
    public synchronized void initiateTracking(final String allocationId) {
        assert invariant();
        assert primaryMode;
        LocalCheckPointState lcps = localCheckpoints.get(allocationId);
        if (lcps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available");
        }
        assert lcps.masterInitializing;
        if (lcps.masterInSync == false && lcps.inSync) {
            // this might happen if recovery failed after markAllocationIdAsInSync previously succeeded,
            // an oddity we could get rid of if recoveries were purely primary-driven
            // if masterInSync == true, we cannot set inSync back to false (that would violate invariant), we just keep it as is.
            lcps.inSync = false;
            lcps.localCheckPoint = SequenceNumbersService.UNASSIGNED_SEQ_NO;
            updateGlobalCheckpointOnPrimary();
        }
        assert invariant();
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. This method will block until the local checkpoint
     * on the specified shard advances above the current global checkpoint.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     */
    public synchronized void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        assert invariant();
        assert primaryMode;
        assert handOffInProgress == false;
        LocalCheckPointState lcps = localCheckpoints.get(allocationId);
        if (lcps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available");
        }
        assert lcps.masterInitializing;
        assert lcps.blockGCPAdvance == false;
        updateLocalCheckpoint(allocationId, lcps, localCheckpoint);
        assert !lcps.masterInSync || (lcps.localCheckPoint >= globalCheckpoint);
        if (lcps.localCheckPoint < globalCheckpoint) {
            lcps.blockGCPAdvance = true;
            try {
                while (true) {
                    if (lcps.blockGCPAdvance) {
                        waitForLocalCheckpointToAdvance();
                    } else {
                        break;
                    }
                }
            } finally {
                lcps.blockGCPAdvance = false;
            }
        } else {
            lcps.inSync = true;
            logger.trace("marked [{}] as in-sync", allocationId);
        }

        assert invariant();
    }

    private boolean updateLocalCheckpoint(String allocationId, LocalCheckPointState lcps, long localCheckpoint) {
        if (localCheckpoint > lcps.localCheckPoint) {
            logger.trace("updated local checkpoint of [{}] from [{}] to [{}]", allocationId, lcps.localCheckPoint, localCheckpoint);
            lcps.localCheckPoint = localCheckpoint;
            return true;
        } else {
            logger.trace("skipped updating local checkpoint of [{}] from [{}] to [{}], current checkpoint is higher", allocationId,
                lcps.localCheckPoint, localCheckpoint);
            return false;
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. If the checkpoint is lower than
     * the currently known one, this is a no-op. If the allocation ID is not tracked, it is ignored.
     *
     * @param allocationId    the allocation ID of the shard to update the local checkpoint for
     * @param localCheckpoint the local checkpoint for the shard
     */
    public synchronized void updateLocalCheckpoint(final String allocationId, final long localCheckpoint) {
        assert invariant();
        assert primaryMode;
        assert handOffInProgress == false;
        LocalCheckPointState lcps = localCheckpoints.get(allocationId);
        if (lcps == null) {
            // can happen if replica was removed from cluster but replication process is unaware of it yet
            return;
        }
        boolean increasedLocalCheckpoint = updateLocalCheckpoint(allocationId, lcps, localCheckpoint);
        if (lcps.blockGCPAdvance && lcps.localCheckPoint >= globalCheckpoint) {
            lcps.blockGCPAdvance = false;
            lcps.inSync = true;
            logger.trace("marked [{}] as in-sync", allocationId);
            notifyAllWaiters();
        }
        if (increasedLocalCheckpoint && lcps.blockGCPAdvance == false) {
            updateGlobalCheckpointOnPrimary();
        }
        assert invariant();
    }

    /**
     * Computes the global checkpoint based on the given local checkpoints. In case where there are entries preventing the
     * computation to happen (for example due to blocking), it returns the fallback value.
     */
    private static long computeGlobalCheckPoint(final Collection<LocalCheckPointState> localCheckpoints, final long fallback) {
        long minLocalCheckpoint = Long.MAX_VALUE;
        for (final LocalCheckPointState lcps : localCheckpoints) {
            if (lcps.blockGCPAdvance) {
                return fallback;
            }
            if (lcps.inSync) {
                if (lcps.localCheckPoint == SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                    // unassigned in-sync replica or 5.x replica
                    return fallback;
                }
                minLocalCheckpoint = Math.min(lcps.localCheckPoint, minLocalCheckpoint);
            }
        }
        assert minLocalCheckpoint != Long.MAX_VALUE;
        return minLocalCheckpoint;
    }

    /**
     * Scans through the currently known local checkpoint and updates the global checkpoint accordingly.
     */
    private synchronized void updateGlobalCheckpointOnPrimary() {
        assert primaryMode;
        final long computedGlobalCheckpoint = computeGlobalCheckPoint(localCheckpoints.values(), globalCheckpoint);
        assert computedGlobalCheckpoint >= globalCheckpoint : "new global checkpoint [" + computedGlobalCheckpoint +
            "] is lower than previous one [" + globalCheckpoint + "]";
        if (globalCheckpoint != computedGlobalCheckpoint) {
            logger.trace("global checkpoint updated to [{}]", computedGlobalCheckpoint);
            globalCheckpoint = computedGlobalCheckpoint;
        }
    }

    /**
     * Initiates a relocation handoff and returns the corresponding primary context.
     */
    public synchronized PrimaryContext startRelocationHandOff() {
        assert invariant();
        assert primaryMode;
        assert handOffInProgress == false;
        handOffInProgress = true;
        // copy clusterStateVersion and localCheckPoints and return
        // all the entries from localCheckPoints that are inSync: the reason we don't need to care about initializing non-insync entries
        // is that they will have to undergo a recovery attempt on the relocation target, and will hence be supplied by the cluster state
        // update on the relocation target once relocation completes). We could alternatively also copy the map as-is (it’s safe), and it
        // would be cleaned up on the target by cluster state updates.
        Map<String, LocalCheckPointState> localCheckpointsCopy = new HashMap<>();
        for (Map.Entry<String, LocalCheckPointState> entry : localCheckpoints.entrySet()) {
            localCheckpointsCopy.put(entry.getKey(), entry.getValue().copy());
        }
        assert invariant();
        return new PrimaryContext(appliedClusterStateVersion, localCheckpointsCopy);
    }

    /**
     * Fails a relocation handoff attempt.
     */
    public synchronized void abortRelocationHandOff() {
        assert invariant();
        assert primaryMode;
        assert handOffInProgress;
        handOffInProgress = false;
        assert invariant();
    }

    /**
     * Marks a relocation handoff attempt as successful. Moves the tracker into replica mode.
     */
    public synchronized void completeRelocationHandOff() {
        assert invariant();
        assert primaryMode;
        assert handOffInProgress;
        primaryMode = false;
        handOffInProgress = false;
        localCheckpoints.clear();
        appliedClusterStateVersion = -1L;
        assert invariant();
    }

    /**
     * Initializes the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary relocation target during
     * primary relocation handoff.
     *
     * @param primaryContext the primary context used to initialize the state
     */
    public synchronized void initializeWithPrimaryContext(PrimaryContext primaryContext) {
        assert invariant();
        assert primaryMode == false;
        primaryMode = true;
        appliedClusterStateVersion = primaryContext.clusterStateVersion();
        for (Map.Entry<String, LocalCheckPointState> entry : primaryContext.localCheckpoints.entrySet()) {
            localCheckpoints.put(entry.getKey(), entry.getValue().copy());
        }
        updateGlobalCheckpointOnPrimary();
        assert invariant();
    }

    /**
     * Whether the are shards blocking global checkpoint advancement. Used by tests.
     */
    public synchronized boolean pendingInSync() {
        assert primaryMode;
        return localCheckpoints.values().stream().anyMatch(lcps -> lcps.blockGCPAdvance);
    }

    /**
     * Returns the local checkpoint information tracked for a specific shard. Used by tests.
     */
    public synchronized LocalCheckPointState getTrackedLocalCheckpointForShard(String allocationId) {
        assert primaryMode;
        return localCheckpoints.get(allocationId);
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
     * Wait for the local checkpoint to advance to the global checkpoint.
     *
     * @throws InterruptedException if this thread was interrupted before of during waiting
     */
    @SuppressForbidden(reason = "Object#wait for local checkpoint advancement")
    private synchronized void waitForLocalCheckpointToAdvance() throws InterruptedException {
        this.wait();
    }

    /**
     * Represents the sequence number component of the primary context. This is the knowledge on the primary of the in-sync and initializing
     * shards and their local checkpoints.
     */
    public static class PrimaryContext implements Writeable {

        private final long clusterStateVersion;
        private final Map<String, LocalCheckPointState> localCheckpoints;

        public PrimaryContext(long clusterStateVersion, Map<String, LocalCheckPointState> localCheckpoints) {
            this.clusterStateVersion = clusterStateVersion;
            this.localCheckpoints = localCheckpoints;
        }

        public PrimaryContext(StreamInput in) throws IOException {
            clusterStateVersion = in.readVLong();
            localCheckpoints = in.readMap(StreamInput::readString, LocalCheckPointState::new);
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        public Map<String, LocalCheckPointState> getLocalCheckpoints() {
            return localCheckpoints;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(clusterStateVersion);
            out.writeMap(localCheckpoints, (streamOutput, s) -> out.writeString(s), (streamOutput, lcps) -> lcps.writeTo(out));
        }

        @Override
        public String toString() {
            return "PrimaryContext{" +
                    "clusterStateVersion=" + clusterStateVersion +
                    ", localCheckpoints=" + localCheckpoints +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PrimaryContext that = (PrimaryContext) o;

            if (clusterStateVersion != that.clusterStateVersion) return false;
            return localCheckpoints.equals(that.localCheckpoints);
        }

        @Override
        public int hashCode() {
            int result = (int) (clusterStateVersion ^ (clusterStateVersion >>> 32));
            result = 31 * result + localCheckpoints.hashCode();
            return result;
        }
    }
}
