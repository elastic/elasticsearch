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

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
     * - replica: this shard receives global checkpoint information from the primary (see
     *   {@link #updateGlobalCheckpointOnReplica(long, String)}).
     *
     * When a shard is initialized (be it a primary or replica), it initially operates in replica mode. The global checkpoint tracker is
     * then switched to primary mode in the following three scenarios:
     *
     * - An initializing primary shard that is not a relocation target is moved to primary mode (using {@link #activatePrimaryMode}) once
     *   the shard becomes active.
     * - An active replica shard is moved to primary mode (using {@link #activatePrimaryMode}) once it is promoted to primary.
     * - A primary relocation target is moved to primary mode (using {@link #activateWithPrimaryContext}) during the primary relocation
     *   handoff. If the target shard is successfully initialized in primary mode, the source shard of a primary relocation is then moved
     *   to replica mode (using {@link #completeRelocationHandoff}), as the relocation target will be in charge of the global checkpoint
     *   computation from that point on.
     */
    boolean primaryMode;
    /**
     * Boolean flag that indicates if a relocation handoff is in progress. A handoff is started by calling {@link #startRelocationHandoff}
     * and is finished by either calling {@link #completeRelocationHandoff} or {@link #abortRelocationHandoff}, depending on whether the
     * handoff was successful or not. During the handoff, which has as main objective to transfer the internal state of the global
     * checkpoint tracker from the relocation source to the target, the list of in-sync shard copies cannot grow, otherwise the relocation
     * target might miss this information and increase the global checkpoint to eagerly. As consequence, some of the methods in this class
     * are not allowed to be called while a handoff is in progress, in particular {@link #markAllocationIdAsInSync}.
     *
     * A notable exception to this is the method {@link #updateFromMaster}, which is still allowed to be called during a relocation handoff.
     * The reason for this is that the handoff might fail and can be aborted (using {@link #abortRelocationHandoff}), in which case
     * it is important that the global checkpoint tracker does not miss any state updates that might happened during the handoff attempt.
     * This means, however, that the global checkpoint can still advance after the primary relocation handoff has been initiated, but only
     * because the master could have failed some of the in-sync shard copies and marked them as stale. That is ok though, as this
     * information is conveyed through cluster state updates, and the new primary relocation target will also eventually learn about those.
     */
    boolean handoffInProgress;

    /**
     * The global checkpoint tracker relies on the property that cluster state updates are applied in-order. After transferring a primary
     * context from the primary relocation source to the target and initializing the target, it is possible for the target to apply a
     * cluster state that is older than the one upon which the primary context was based. If we allowed this old cluster state
     * to influence the list of in-sync shard copies here, this could possibly remove such an in-sync copy from the internal structures
     * until the newer cluster state were to be applied, which would unsafely advance the global checkpoint. This field thus captures
     * the version of the last applied cluster state to ensure in-order updates.
     */
    long appliedClusterStateVersion;

    IndexShardRoutingTable routingTable;

    /**
     * Local checkpoint information for all shard copies that are tracked. Has an entry for all shard copies that are either initializing
     * and / or in-sync, possibly also containing information about unassigned in-sync shard copies. The information that is tracked for
     * each shard copy is explained in the docs for the {@link LocalCheckpointState} class.
     */
    final Map<String, LocalCheckpointState> localCheckpoints;

    /**
     * This set contains allocation IDs for which there is a thread actively waiting for the local checkpoint to advance to at least the
     * current global checkpoint.
     */
    final Set<String> pendingInSync;

    /**
     * The global checkpoint:
     * - computed based on local checkpoints, if the tracker is in primary mode
     * - received from the primary, if the tracker is in replica mode
     */
    volatile long globalCheckpoint;

    /**
     * Cached value for the last replication group that was computed
     */
    volatile ReplicationGroup replicationGroup;

    public static class LocalCheckpointState implements Writeable {

        /**
         * the last local checkpoint information that we have for this shard
         */
        long localCheckpoint;
        /**
         * whether this shard is treated as in-sync and thus contributes to the global checkpoint calculation
         */
        boolean inSync;

        public LocalCheckpointState(long localCheckpoint, boolean inSync) {
            this.localCheckpoint = localCheckpoint;
            this.inSync = inSync;
        }

        public LocalCheckpointState(StreamInput in) throws IOException {
            this.localCheckpoint = in.readZLong();
            this.inSync = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(localCheckpoint);
            out.writeBoolean(inSync);
        }

        /**
         * Returns a full copy of this object
         */
        public LocalCheckpointState copy() {
            return new LocalCheckpointState(localCheckpoint, inSync);
        }

        public long getLocalCheckpoint() {
            return localCheckpoint;
        }

        @Override
        public String toString() {
            return "LocalCheckpointState{" +
                "localCheckpoint=" + localCheckpoint +
                ", inSync=" + inSync +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LocalCheckpointState that = (LocalCheckpointState) o;

            if (localCheckpoint != that.localCheckpoint) return false;
            return inSync == that.inSync;
        }

        @Override
        public int hashCode() {
            int result = (int) (localCheckpoint ^ (localCheckpoint >>> 32));
            result = 31 * result + (inSync ? 1 : 0);
            return result;
        }
    }

    /**
     * Class invariant that should hold before and after every invocation of public methods on this class. As Java lacks implication
     * as a logical operator, many of the invariants are written under the form (!A || B), they should be read as (A implies B) however.
     */
    private boolean invariant() {
        // local checkpoints only set during primary mode
        assert primaryMode || localCheckpoints.values().stream()
            .allMatch(lcps -> lcps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO ||
                lcps.localCheckpoint == SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT);

        // relocation handoff can only occur in primary mode
        assert !handoffInProgress || primaryMode;

        // there is at least one in-sync shard copy when the global checkpoint tracker operates in primary mode (i.e. the shard itself)
        assert !primaryMode || localCheckpoints.values().stream().anyMatch(lcps -> lcps.inSync);

        // the routing table and replication group is set when the global checkpoint tracker operates in primary mode
        assert !primaryMode || (routingTable != null && replicationGroup != null) :
            "primary mode but routing table is " + routingTable + " and replication group is " + replicationGroup;

        // during relocation handoff there are no entries blocking global checkpoint advancement
        assert !handoffInProgress || pendingInSync.isEmpty() :
            "entries blocking global checkpoint advancement during relocation handoff: " + pendingInSync;

        // entries blocking global checkpoint advancement can only exist in primary mode and when not having a relocation handoff
        assert pendingInSync.isEmpty() || (primaryMode && !handoffInProgress);

        // the computed global checkpoint is always up-to-date
        assert !primaryMode || globalCheckpoint == computeGlobalCheckpoint(pendingInSync, localCheckpoints.values(), globalCheckpoint) :
            "global checkpoint is not up-to-date, expected: " +
                computeGlobalCheckpoint(pendingInSync, localCheckpoints.values(), globalCheckpoint) + " but was: " + globalCheckpoint;

        // we have a routing table iff we have a replication group
        assert (routingTable == null) == (replicationGroup == null) :
            "routing table is " + routingTable + " but replication group is " + replicationGroup;

        assert replicationGroup == null || replicationGroup.equals(calculateReplicationGroup()) :
            "cached replication group out of sync: expected: " + calculateReplicationGroup() + " but was: " + replicationGroup;

        // all assigned shards from the routing table are tracked
        assert routingTable == null || localCheckpoints.keySet().containsAll(routingTable.getAllAllocationIds()) :
            "local checkpoints " + localCheckpoints + " not in-sync with routing table " + routingTable;

        for (Map.Entry<String, LocalCheckpointState> entry : localCheckpoints.entrySet()) {
            // blocking global checkpoint advancement only happens for shards that are not in-sync
            assert !pendingInSync.contains(entry.getKey()) || !entry.getValue().inSync :
                "shard copy " + entry.getKey() + " blocks global checkpoint advancement but is in-sync";
        }

        return true;
    }

    /**
     * Initialize the global checkpoint service. The specified global checkpoint should be set to the last known global checkpoint, or
     * {@link SequenceNumbers#UNASSIGNED_SEQ_NO}.
     *
     * @param shardId          the shard ID
     * @param indexSettings    the index settings
     * @param globalCheckpoint the last known global checkpoint for this shard, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     */
    GlobalCheckpointTracker(final ShardId shardId, final IndexSettings indexSettings, final long globalCheckpoint) {
        super(shardId, indexSettings);
        assert globalCheckpoint >= SequenceNumbers.UNASSIGNED_SEQ_NO : "illegal initial global checkpoint: " + globalCheckpoint;
        this.primaryMode = false;
        this.handoffInProgress = false;
        this.appliedClusterStateVersion = -1L;
        this.globalCheckpoint = globalCheckpoint;
        this.localCheckpoints = new HashMap<>(1 + indexSettings.getNumberOfReplicas());
        this.pendingInSync = new HashSet<>();
        this.routingTable = null;
        this.replicationGroup = null;
        assert invariant();
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        assert primaryMode;
        return replicationGroup;
    }

    private ReplicationGroup calculateReplicationGroup() {
        return new ReplicationGroup(routingTable,
            localCheckpoints.entrySet().stream().filter(e -> e.getValue().inSync).map(Map.Entry::getKey).collect(Collectors.toSet()));
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getGlobalCheckpoint() {
        return globalCheckpoint;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint
     * @param reason           the reason the global checkpoint was updated
     */
    public synchronized void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
        assert invariant();
        assert primaryMode == false;
        /*
         * The global checkpoint here is a local knowledge which is updated under the mandate of the primary. It can happen that the primary
         * information is lagging compared to a replica (e.g., if a replica is promoted to primary but has stale info relative to other
         * replica shards). In these cases, the local knowledge of the global checkpoint could be higher than sync from the lagging primary.
         */
        if (this.globalCheckpoint <= globalCheckpoint) {
            logger.trace("updating global checkpoint from [{}] to [{}] due to [{}]", this.globalCheckpoint, globalCheckpoint, reason);
            this.globalCheckpoint = globalCheckpoint;
        }
        assert invariant();
    }

    /**
     * Initializes the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary activation or promotion.
     */
    public synchronized void activatePrimaryMode(final String allocationId, final long localCheckpoint) {
        assert invariant();
        assert primaryMode == false;
        assert localCheckpoints.get(allocationId) != null && localCheckpoints.get(allocationId).inSync &&
            localCheckpoints.get(allocationId).localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "expected " + allocationId + " to have initialized entry in " + localCheckpoints + " when activating primary";
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED;
        primaryMode = true;
        updateLocalCheckpoint(allocationId, localCheckpoints.get(allocationId), localCheckpoint);
        updateGlobalCheckpointOnPrimary();
        assert invariant();
    }

    /**
     * Notifies the tracker of the current allocation IDs in the cluster state.
     *
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
     * @param inSyncAllocationIds         the allocation IDs of the currently in-sync shard copies
     * @param routingTable                the shard routing table
     * @param pre60AllocationIds          the allocation IDs of shards that are allocated to pre-6.0 nodes
     */
    public synchronized void updateFromMaster(final long applyingClusterStateVersion, final Set<String> inSyncAllocationIds,
                                              final IndexShardRoutingTable routingTable, final Set<String> pre60AllocationIds) {
        assert invariant();
        if (applyingClusterStateVersion > appliedClusterStateVersion) {
            // check that the master does not fabricate new in-sync entries out of thin air once we are in primary mode
            assert !primaryMode || inSyncAllocationIds.stream().allMatch(
                inSyncId -> localCheckpoints.containsKey(inSyncId) && localCheckpoints.get(inSyncId).inSync) :
                "update from master in primary mode contains in-sync ids " + inSyncAllocationIds +
                    " that have no matching entries in " + localCheckpoints;
            // remove entries which don't exist on master
            Set<String> initializingAllocationIds = routingTable.getAllInitializingShards().stream()
                .map(ShardRouting::allocationId).map(AllocationId::getId).collect(Collectors.toSet());
            boolean removedEntries = localCheckpoints.keySet().removeIf(
                aid -> !inSyncAllocationIds.contains(aid) && !initializingAllocationIds.contains(aid));

            if (primaryMode) {
                // add new initializingIds that are missing locally. These are fresh shard copies - and not in-sync
                for (String initializingId : initializingAllocationIds) {
                    if (localCheckpoints.containsKey(initializingId) == false) {
                        final boolean inSync = inSyncAllocationIds.contains(initializingId);
                        assert inSync == false : "update from master in primary mode has " + initializingId +
                            " as in-sync but it does not exist locally";
                        final long localCheckpoint = pre60AllocationIds.contains(initializingId) ?
                            SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT : SequenceNumbers.UNASSIGNED_SEQ_NO;
                        localCheckpoints.put(initializingId, new LocalCheckpointState(localCheckpoint, inSync));
                    }
                }
            } else {
                for (String initializingId : initializingAllocationIds) {
                    final long localCheckpoint = pre60AllocationIds.contains(initializingId) ?
                        SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT : SequenceNumbers.UNASSIGNED_SEQ_NO;
                    localCheckpoints.put(initializingId, new LocalCheckpointState(localCheckpoint, false));
                }
                for (String inSyncId : inSyncAllocationIds) {
                    final long localCheckpoint = pre60AllocationIds.contains(inSyncId) ?
                        SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT : SequenceNumbers.UNASSIGNED_SEQ_NO;
                    localCheckpoints.put(inSyncId, new LocalCheckpointState(localCheckpoint, true));
                }
            }
            appliedClusterStateVersion = applyingClusterStateVersion;
            this.routingTable = routingTable;
            replicationGroup = calculateReplicationGroup();
            if (primaryMode && removedEntries) {
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
        LocalCheckpointState lcps = localCheckpoints.get(allocationId);
        if (lcps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available");
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
        assert handoffInProgress == false;
        LocalCheckpointState lcps = localCheckpoints.get(allocationId);
        if (lcps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available for " + allocationId);
        }
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED :
            "expected known local checkpoint for " + allocationId + " but was " + localCheckpoint;
        assert pendingInSync.contains(allocationId) == false : "shard copy " + allocationId + " is already marked as pending in-sync";
        updateLocalCheckpoint(allocationId, lcps, localCheckpoint);
        // if it was already in-sync (because of a previously failed recovery attempt), global checkpoint must have been
        // stuck from advancing
        assert !lcps.inSync || (lcps.localCheckpoint >= globalCheckpoint) :
            "shard copy " + allocationId + " that's already in-sync should have a local checkpoint " + lcps.localCheckpoint +
                " that's above the global checkpoint " + globalCheckpoint;
        if (lcps.localCheckpoint < globalCheckpoint) {
            pendingInSync.add(allocationId);
            try {
                while (true) {
                    if (pendingInSync.contains(allocationId)) {
                        waitForLocalCheckpointToAdvance();
                    } else {
                        break;
                    }
                }
            } finally {
                pendingInSync.remove(allocationId);
            }
        } else {
            lcps.inSync = true;
            replicationGroup = calculateReplicationGroup();
            logger.trace("marked [{}] as in-sync", allocationId);
            updateGlobalCheckpointOnPrimary();
        }

        assert invariant();
    }

    private boolean updateLocalCheckpoint(String allocationId, LocalCheckpointState lcps, long localCheckpoint) {
        // a local checkpoint of PRE_60_NODE_LOCAL_CHECKPOINT cannot be overridden
        assert lcps.localCheckpoint != SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT ||
            localCheckpoint == SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT :
            "pre-6.0 shard copy " + allocationId + " unexpected to send valid local checkpoint " + localCheckpoint;
        // a local checkpoint for a shard copy should be a valid sequence number or the pre-6.0 sequence number indicator
        assert localCheckpoint != SequenceNumbers.UNASSIGNED_SEQ_NO :
                "invalid local checkpoint for shard copy [" + allocationId + "]";
        if (localCheckpoint > lcps.localCheckpoint) {
            logger.trace("updated local checkpoint of [{}] from [{}] to [{}]", allocationId, lcps.localCheckpoint, localCheckpoint);
            lcps.localCheckpoint = localCheckpoint;
            return true;
        } else {
            logger.trace("skipped updating local checkpoint of [{}] from [{}] to [{}], current checkpoint is higher", allocationId,
                lcps.localCheckpoint, localCheckpoint);
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
        assert handoffInProgress == false;
        LocalCheckpointState lcps = localCheckpoints.get(allocationId);
        if (lcps == null) {
            // can happen if replica was removed from cluster but replication process is unaware of it yet
            return;
        }
        boolean increasedLocalCheckpoint = updateLocalCheckpoint(allocationId, lcps, localCheckpoint);
        boolean pending = pendingInSync.contains(allocationId);
        if (pending && lcps.localCheckpoint >= globalCheckpoint) {
            pendingInSync.remove(allocationId);
            pending = false;
            lcps.inSync = true;
            replicationGroup = calculateReplicationGroup();
            logger.trace("marked [{}] as in-sync", allocationId);
            notifyAllWaiters();
        }
        if (increasedLocalCheckpoint && pending == false) {
            updateGlobalCheckpointOnPrimary();
        }
        assert invariant();
    }

    /**
     * Computes the global checkpoint based on the given local checkpoints. In case where there are entries preventing the
     * computation to happen (for example due to blocking), it returns the fallback value.
     */
    private static long computeGlobalCheckpoint(final Set<String> pendingInSync, final Collection<LocalCheckpointState> localCheckpoints,
                                                final long fallback) {
        long minLocalCheckpoint = Long.MAX_VALUE;
        if (pendingInSync.isEmpty() == false) {
            return fallback;
        }
        for (final LocalCheckpointState lcps : localCheckpoints) {
            if (lcps.inSync) {
                if (lcps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // unassigned in-sync replica
                    return fallback;
                } else if (lcps.localCheckpoint == SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT) {
                    // 5.x replica, ignore for global checkpoint calculation
                } else {
                    minLocalCheckpoint = Math.min(lcps.localCheckpoint, minLocalCheckpoint);
                }
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
        final long computedGlobalCheckpoint = computeGlobalCheckpoint(pendingInSync, localCheckpoints.values(), globalCheckpoint);
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
    public synchronized PrimaryContext startRelocationHandoff() {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        assert pendingInSync.isEmpty() : "relocation handoff started while there are still shard copies pending in-sync: " + pendingInSync;
        handoffInProgress = true;
        // copy clusterStateVersion and localCheckpoints and return
        // all the entries from localCheckpoints that are inSync: the reason we don't need to care about initializing non-insync entries
        // is that they will have to undergo a recovery attempt on the relocation target, and will hence be supplied by the cluster state
        // update on the relocation target once relocation completes). We could alternatively also copy the map as-is (it’s safe), and it
        // would be cleaned up on the target by cluster state updates.
        Map<String, LocalCheckpointState> localCheckpointsCopy = new HashMap<>();
        for (Map.Entry<String, LocalCheckpointState> entry : localCheckpoints.entrySet()) {
            localCheckpointsCopy.put(entry.getKey(), entry.getValue().copy());
        }
        assert invariant();
        return new PrimaryContext(appliedClusterStateVersion, localCheckpointsCopy, routingTable);
    }

    /**
     * Fails a relocation handoff attempt.
     */
    public synchronized void abortRelocationHandoff() {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress;
        handoffInProgress = false;
        assert invariant();
    }

    /**
     * Marks a relocation handoff attempt as successful. Moves the tracker into replica mode.
     */
    public synchronized void completeRelocationHandoff() {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress;
        primaryMode = false;
        handoffInProgress = false;
        // forget all checkpoint information
        localCheckpoints.values().stream().forEach(lcps -> {
            if (lcps.localCheckpoint != SequenceNumbers.UNASSIGNED_SEQ_NO &&
                lcps.localCheckpoint != SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT) {
                lcps.localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
            }
        });
        assert invariant();
    }

    /**
     * Activates the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary relocation target during
     * primary relocation handoff.
     *
     * @param primaryContext the primary context used to initialize the state
     */
    public synchronized void activateWithPrimaryContext(PrimaryContext primaryContext) {
        assert invariant();
        assert primaryMode == false;
        final Runnable runAfter = getMasterUpdateOperationFromCurrentState();
        primaryMode = true;
        // capture current state to possibly replay missed cluster state update
        appliedClusterStateVersion = primaryContext.clusterStateVersion();
        localCheckpoints.clear();
        for (Map.Entry<String, LocalCheckpointState> entry : primaryContext.localCheckpoints.entrySet()) {
            localCheckpoints.put(entry.getKey(), entry.getValue().copy());
        }
        routingTable = primaryContext.getRoutingTable();
        replicationGroup = calculateReplicationGroup();
        updateGlobalCheckpointOnPrimary();
        // reapply missed cluster state update
        // note that if there was no cluster state update between start of the engine of this shard and the call to
        // initializeWithPrimaryContext, we might still have missed a cluster state update. This is best effort.
        runAfter.run();
        assert invariant();
    }

    private Runnable getMasterUpdateOperationFromCurrentState() {
        assert primaryMode == false;
        final long lastAppliedClusterStateVersion = appliedClusterStateVersion;
        final Set<String> inSyncAllocationIds = new HashSet<>();
        final Set<String> pre60AllocationIds = new HashSet<>();
        localCheckpoints.entrySet().forEach(entry -> {
            if (entry.getValue().inSync) {
                inSyncAllocationIds.add(entry.getKey());
            }
            if (entry.getValue().getLocalCheckpoint() == SequenceNumbersService.PRE_60_NODE_LOCAL_CHECKPOINT) {
                pre60AllocationIds.add(entry.getKey());
            }
        });
        final IndexShardRoutingTable lastAppliedRoutingTable = routingTable;
        return () -> updateFromMaster(lastAppliedClusterStateVersion, inSyncAllocationIds, lastAppliedRoutingTable, pre60AllocationIds);
    }

    /**
     * Whether the are shards blocking global checkpoint advancement. Used by tests.
     */
    public synchronized boolean pendingInSync() {
        assert primaryMode;
        return pendingInSync.isEmpty() == false;
    }

    /**
     * Returns the local checkpoint information tracked for a specific shard. Used by tests.
     */
    public synchronized LocalCheckpointState getTrackedLocalCheckpointForShard(String allocationId) {
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
        private final Map<String, LocalCheckpointState> localCheckpoints;
        private final IndexShardRoutingTable routingTable;

        public PrimaryContext(long clusterStateVersion, Map<String, LocalCheckpointState> localCheckpoints,
                              IndexShardRoutingTable routingTable) {
            this.clusterStateVersion = clusterStateVersion;
            this.localCheckpoints = localCheckpoints;
            this.routingTable = routingTable;
        }

        public PrimaryContext(StreamInput in) throws IOException {
            clusterStateVersion = in.readVLong();
            localCheckpoints = in.readMap(StreamInput::readString, LocalCheckpointState::new);
            routingTable = IndexShardRoutingTable.Builder.readFrom(in);
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        public Map<String, LocalCheckpointState> getLocalCheckpoints() {
            return localCheckpoints;
        }

        public IndexShardRoutingTable getRoutingTable() {
            return routingTable;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(clusterStateVersion);
            out.writeMap(localCheckpoints, (streamOutput, s) -> out.writeString(s), (streamOutput, lcps) -> lcps.writeTo(out));
            IndexShardRoutingTable.Builder.writeTo(routingTable, out);
        }

        @Override
        public String toString() {
            return "PrimaryContext{" +
                    "clusterStateVersion=" + clusterStateVersion +
                    ", localCheckpoints=" + localCheckpoints +
                    ", routingTable=" + routingTable +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PrimaryContext that = (PrimaryContext) o;

            if (clusterStateVersion != that.clusterStateVersion) return false;
            if (routingTable.equals(that.routingTable)) return false;
            return routingTable.equals(that.routingTable);
        }

        @Override
        public int hashCode() {
            int result = (int) (clusterStateVersion ^ (clusterStateVersion >>> 32));
            result = 31 * result + localCheckpoints.hashCode();
            result = 31 * result + routingTable.hashCode();
            return result;
        }
    }
}
