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
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
     * The allocation ID for the shard to which this tracker is a component of.
     */
    final String shardAllocationId;

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
     * each shard copy is explained in the docs for the {@link CheckpointState} class.
     */
    final Map<String, CheckpointState> checkpoints;

    /**
     * This set contains allocation IDs for which there is a thread actively waiting for the local checkpoint to advance to at least the
     * current global checkpoint.
     */
    final Set<String> pendingInSync;

    /**
     * Cached value for the last replication group that was computed
     */
    volatile ReplicationGroup replicationGroup;

    public static class CheckpointState implements Writeable {

        /**
         * the last local checkpoint information that we have for this shard
         */
        long localCheckpoint;

        /**
         * the last global checkpoint information that we have for this shard. This information is computed for the primary if
         * the tracker is in primary mode and received from the primary if in replica mode.
         */
        long globalCheckpoint;
        /**
         * whether this shard is treated as in-sync and thus contributes to the global checkpoint calculation
         */
        boolean inSync;

        public CheckpointState(long localCheckpoint, long globalCheckpoint, boolean inSync) {
            this.localCheckpoint = localCheckpoint;
            this.globalCheckpoint = globalCheckpoint;
            this.inSync = inSync;
        }

        public CheckpointState(StreamInput in) throws IOException {
            this.localCheckpoint = in.readZLong();
            this.globalCheckpoint = in.readZLong();
            this.inSync = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(localCheckpoint);
            out.writeZLong(globalCheckpoint);
            out.writeBoolean(inSync);
        }

        /**
         * Returns a full copy of this object
         */
        public CheckpointState copy() {
            return new CheckpointState(localCheckpoint, globalCheckpoint, inSync);
        }

        public long getLocalCheckpoint() {
            return localCheckpoint;
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public String toString() {
            return "LocalCheckpointState{" +
                "localCheckpoint=" + localCheckpoint +
                ", globalCheckpoint=" + globalCheckpoint +
                ", inSync=" + inSync +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CheckpointState that = (CheckpointState) o;

            if (localCheckpoint != that.localCheckpoint) return false;
            if (globalCheckpoint != that.globalCheckpoint) return false;
            return inSync == that.inSync;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(localCheckpoint);
            result = 31 * result + Long.hashCode(globalCheckpoint);
            result = 31 * result + Boolean.hashCode(inSync);
            return result;
        }
    }

    /**
     * Get the local knowledge of the global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the global checkpoint for that allocation ID
     */
    synchronized ObjectLongMap<String> getInSyncGlobalCheckpoints() {
        assert primaryMode;
        assert handoffInProgress == false;
        final ObjectLongMap<String> globalCheckpoints = new ObjectLongHashMap<>(checkpoints.size()); // upper bound on the size
        checkpoints
                .entrySet()
                .stream()
                .filter(e -> e.getValue().inSync)
                .forEach(e -> globalCheckpoints.put(e.getKey(), e.getValue().globalCheckpoint));
        return globalCheckpoints;
    }

    /**
     * Class invariant that should hold before and after every invocation of public methods on this class. As Java lacks implication
     * as a logical operator, many of the invariants are written under the form (!A || B), they should be read as (A implies B) however.
     */
    private boolean invariant() {
        assert checkpoints.get(shardAllocationId) != null :
            "checkpoints map should always have an entry for the current shard";

        // local checkpoints only set during primary mode
        assert primaryMode || checkpoints.values().stream()
            .allMatch(lcps -> lcps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO ||
                lcps.localCheckpoint == SequenceNumbers.PRE_60_NODE_CHECKPOINT);

        // global checkpoints for other shards only set during primary mode
        assert primaryMode
                || checkpoints
                .entrySet()
                .stream()
                .filter(e -> e.getKey().equals(shardAllocationId) == false)
                .map(Map.Entry::getValue)
                .allMatch(cps ->
                        (cps.globalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO
                                || cps.globalCheckpoint == SequenceNumbers.PRE_60_NODE_CHECKPOINT));

        // relocation handoff can only occur in primary mode
        assert !handoffInProgress || primaryMode;

        // the current shard is marked as in-sync when the global checkpoint tracker operates in primary mode
        assert !primaryMode || checkpoints.get(shardAllocationId).inSync;

        // the routing table and replication group is set when the global checkpoint tracker operates in primary mode
        assert !primaryMode || (routingTable != null && replicationGroup != null) :
            "primary mode but routing table is " + routingTable + " and replication group is " + replicationGroup;

        // when in primary mode, the current allocation ID is the allocation ID of the primary or the relocation allocation ID
        assert !primaryMode
                || (routingTable.primaryShard().allocationId().getId().equals(shardAllocationId)
                || routingTable.primaryShard().allocationId().getRelocationId().equals(shardAllocationId));

        // during relocation handoff there are no entries blocking global checkpoint advancement
        assert !handoffInProgress || pendingInSync.isEmpty() :
            "entries blocking global checkpoint advancement during relocation handoff: " + pendingInSync;

        // entries blocking global checkpoint advancement can only exist in primary mode and when not having a relocation handoff
        assert pendingInSync.isEmpty() || (primaryMode && !handoffInProgress);

        // the computed global checkpoint is always up-to-date
        assert !primaryMode
                || getGlobalCheckpoint() == computeGlobalCheckpoint(pendingInSync, checkpoints.values(), getGlobalCheckpoint())
                : "global checkpoint is not up-to-date, expected: " +
                computeGlobalCheckpoint(pendingInSync, checkpoints.values(), getGlobalCheckpoint()) + " but was: " + getGlobalCheckpoint();

        // when in primary mode, the global checkpoint is at most the minimum local checkpoint on all in-sync shard copies
        assert !primaryMode
                || getGlobalCheckpoint() <= inSyncCheckpointStates(checkpoints, CheckpointState::getLocalCheckpoint, LongStream::min)
                : "global checkpoint [" + getGlobalCheckpoint() + "] "
                + "for primary mode allocation ID [" + shardAllocationId + "] "
                + "more than in-sync local checkpoints [" + checkpoints + "]";

        // we have a routing table iff we have a replication group
        assert (routingTable == null) == (replicationGroup == null) :
            "routing table is " + routingTable + " but replication group is " + replicationGroup;

        assert replicationGroup == null || replicationGroup.equals(calculateReplicationGroup()) :
            "cached replication group out of sync: expected: " + calculateReplicationGroup() + " but was: " + replicationGroup;

        // all assigned shards from the routing table are tracked
        assert routingTable == null || checkpoints.keySet().containsAll(routingTable.getAllAllocationIds()) :
            "local checkpoints " + checkpoints + " not in-sync with routing table " + routingTable;

        for (Map.Entry<String, CheckpointState> entry : checkpoints.entrySet()) {
            // blocking global checkpoint advancement only happens for shards that are not in-sync
            assert !pendingInSync.contains(entry.getKey()) || !entry.getValue().inSync :
                "shard copy " + entry.getKey() + " blocks global checkpoint advancement but is in-sync";
        }

        return true;
    }

    private static long inSyncCheckpointStates(
            final Map<String, CheckpointState> checkpoints,
            ToLongFunction<CheckpointState> function,
            Function<LongStream, OptionalLong> reducer) {
        final OptionalLong value =
                reducer.apply(
                        checkpoints
                                .values()
                                .stream()
                                .filter(cps -> cps.inSync)
                                .mapToLong(function)
                                .filter(v -> v != SequenceNumbers.PRE_60_NODE_CHECKPOINT && v != SequenceNumbers.UNASSIGNED_SEQ_NO));
        return value.isPresent() ? value.getAsLong() : SequenceNumbers.UNASSIGNED_SEQ_NO;
    }

    /**
     * Initialize the global checkpoint service. The specified global checkpoint should be set to the last known global checkpoint, or
     * {@link SequenceNumbers#UNASSIGNED_SEQ_NO}.
     *
     * @param shardId          the shard ID
     * @param allocationId     the allocation ID
     * @param indexSettings    the index settings
     * @param globalCheckpoint the last known global checkpoint for this shard, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     */
    GlobalCheckpointTracker(
            final ShardId shardId,
            final String allocationId,
            final IndexSettings indexSettings,
            final long globalCheckpoint) {
        super(shardId, indexSettings);
        assert globalCheckpoint >= SequenceNumbers.UNASSIGNED_SEQ_NO : "illegal initial global checkpoint: " + globalCheckpoint;
        this.shardAllocationId = allocationId;
        this.primaryMode = false;
        this.handoffInProgress = false;
        this.appliedClusterStateVersion = -1L;
        this.checkpoints = new HashMap<>(1 + indexSettings.getNumberOfReplicas());
        checkpoints.put(allocationId, new CheckpointState(SequenceNumbers.UNASSIGNED_SEQ_NO, globalCheckpoint, false));
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
            checkpoints.entrySet().stream().filter(e -> e.getValue().inSync).map(Map.Entry::getKey).collect(Collectors.toSet()));
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public synchronized long getGlobalCheckpoint() {
        final CheckpointState cps = checkpoints.get(shardAllocationId);
        assert cps != null;
        return cps.globalCheckpoint;
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
         * replica shards). In these cases, the local knowledge of the global checkpoint could be higher than the sync from the lagging
         * primary.
         */
        updateGlobalCheckpoint(
                shardAllocationId,
                globalCheckpoint,
                current -> logger.trace("updating global checkpoint from [{}] to [{}] due to [{}]", current, globalCheckpoint, reason));
        assert invariant();
    }

    /**
     * Update the local knowledge of the global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     */
    public synchronized void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        assert primaryMode;
        assert handoffInProgress == false;
        assert invariant();
        updateGlobalCheckpoint(
                allocationId,
                globalCheckpoint,
                current -> logger.trace(
                        "updating local knowledge for [{}] on the primary of the global checkpoint from [{}] to [{}]",
                        allocationId,
                        current,
                        globalCheckpoint));
        assert invariant();
    }

    private void updateGlobalCheckpoint(final String allocationId, final long globalCheckpoint, LongConsumer ifUpdated) {
        final CheckpointState cps = checkpoints.get(allocationId);
        assert !this.shardAllocationId.equals(allocationId) || cps != null;
        if (cps != null && globalCheckpoint > cps.globalCheckpoint) {
            ifUpdated.accept(cps.globalCheckpoint);
            cps.globalCheckpoint = globalCheckpoint;
        }
    }

    /**
     * Initializes the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary activation or promotion.
     */
    public synchronized void activatePrimaryMode(final long localCheckpoint) {
        assert invariant();
        assert primaryMode == false;
        assert checkpoints.get(shardAllocationId) != null && checkpoints.get(shardAllocationId).inSync &&
            checkpoints.get(shardAllocationId).localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "expected " + shardAllocationId + " to have initialized entry in " + checkpoints + " when activating primary";
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED;
        primaryMode = true;
        updateLocalCheckpoint(shardAllocationId, checkpoints.get(shardAllocationId), localCheckpoint);
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
                inSyncId -> checkpoints.containsKey(inSyncId) && checkpoints.get(inSyncId).inSync) :
                "update from master in primary mode contains in-sync ids " + inSyncAllocationIds +
                    " that have no matching entries in " + checkpoints;
            // remove entries which don't exist on master
            Set<String> initializingAllocationIds = routingTable.getAllInitializingShards().stream()
                .map(ShardRouting::allocationId).map(AllocationId::getId).collect(Collectors.toSet());
            boolean removedEntries = checkpoints.keySet().removeIf(
                aid -> !inSyncAllocationIds.contains(aid) && !initializingAllocationIds.contains(aid));

            if (primaryMode) {
                // add new initializingIds that are missing locally. These are fresh shard copies - and not in-sync
                for (String initializingId : initializingAllocationIds) {
                    if (checkpoints.containsKey(initializingId) == false) {
                        final boolean inSync = inSyncAllocationIds.contains(initializingId);
                        assert inSync == false : "update from master in primary mode has " + initializingId +
                            " as in-sync but it does not exist locally";
                        final long localCheckpoint = pre60AllocationIds.contains(initializingId) ?
                            SequenceNumbers.PRE_60_NODE_CHECKPOINT : SequenceNumbers.UNASSIGNED_SEQ_NO;
                        final long globalCheckpoint = localCheckpoint;
                        checkpoints.put(initializingId, new CheckpointState(localCheckpoint, globalCheckpoint, inSync));
                    }
                }
            } else {
                for (String initializingId : initializingAllocationIds) {
                    if (shardAllocationId.equals(initializingId) == false) {
                        final long localCheckpoint = pre60AllocationIds.contains(initializingId) ?
                            SequenceNumbers.PRE_60_NODE_CHECKPOINT : SequenceNumbers.UNASSIGNED_SEQ_NO;
                        final long globalCheckpoint = localCheckpoint;
                        checkpoints.put(initializingId, new CheckpointState(localCheckpoint, globalCheckpoint, false));
                    }
                }
                for (String inSyncId : inSyncAllocationIds) {
                    if (shardAllocationId.equals(inSyncId)) {
                        // current shard is initially marked as not in-sync because we don't know better at that point
                        checkpoints.get(shardAllocationId).inSync = true;
                    } else {
                        final long localCheckpoint = pre60AllocationIds.contains(inSyncId) ?
                            SequenceNumbers.PRE_60_NODE_CHECKPOINT : SequenceNumbers.UNASSIGNED_SEQ_NO;
                        final long globalCheckpoint = localCheckpoint;
                        checkpoints.put(inSyncId, new CheckpointState(localCheckpoint, globalCheckpoint, true));
                    }
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
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
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
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available for " + allocationId);
        }
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED :
            "expected known local checkpoint for " + allocationId + " but was " + localCheckpoint;
        assert pendingInSync.contains(allocationId) == false : "shard copy " + allocationId + " is already marked as pending in-sync";
        updateLocalCheckpoint(allocationId, cps, localCheckpoint);
        // if it was already in-sync (because of a previously failed recovery attempt), global checkpoint must have been
        // stuck from advancing
        assert !cps.inSync || (cps.localCheckpoint >= getGlobalCheckpoint()) :
            "shard copy " + allocationId + " that's already in-sync should have a local checkpoint " + cps.localCheckpoint +
                " that's above the global checkpoint " + getGlobalCheckpoint();
        if (cps.localCheckpoint < getGlobalCheckpoint()) {
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
            cps.inSync = true;
            replicationGroup = calculateReplicationGroup();
            logger.trace("marked [{}] as in-sync", allocationId);
            updateGlobalCheckpointOnPrimary();
        }

        assert invariant();
    }

    private boolean updateLocalCheckpoint(String allocationId, CheckpointState cps, long localCheckpoint) {
        // a local checkpoint of PRE_60_NODE_CHECKPOINT cannot be overridden
        assert cps.localCheckpoint != SequenceNumbers.PRE_60_NODE_CHECKPOINT ||
            localCheckpoint == SequenceNumbers.PRE_60_NODE_CHECKPOINT :
            "pre-6.0 shard copy " + allocationId + " unexpected to send valid local checkpoint " + localCheckpoint;
        // a local checkpoint for a shard copy should be a valid sequence number or the pre-6.0 sequence number indicator
        assert localCheckpoint != SequenceNumbers.UNASSIGNED_SEQ_NO :
                "invalid local checkpoint for shard copy [" + allocationId + "]";
        if (localCheckpoint > cps.localCheckpoint) {
            logger.trace("updated local checkpoint of [{}] from [{}] to [{}]", allocationId, cps.localCheckpoint, localCheckpoint);
            cps.localCheckpoint = localCheckpoint;
            return true;
        } else {
            logger.trace("skipped updating local checkpoint of [{}] from [{}] to [{}], current checkpoint is higher", allocationId,
                cps.localCheckpoint, localCheckpoint);
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
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but replication process is unaware of it yet
            return;
        }
        boolean increasedLocalCheckpoint = updateLocalCheckpoint(allocationId, cps, localCheckpoint);
        boolean pending = pendingInSync.contains(allocationId);
        if (pending && cps.localCheckpoint >= getGlobalCheckpoint()) {
            pendingInSync.remove(allocationId);
            pending = false;
            cps.inSync = true;
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
    private static long computeGlobalCheckpoint(final Set<String> pendingInSync, final Collection<CheckpointState> localCheckpoints,
                                                final long fallback) {
        long minLocalCheckpoint = Long.MAX_VALUE;
        if (pendingInSync.isEmpty() == false) {
            return fallback;
        }
        for (final CheckpointState cps : localCheckpoints) {
            if (cps.inSync) {
                if (cps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // unassigned in-sync replica
                    return fallback;
                } else if (cps.localCheckpoint == SequenceNumbers.PRE_60_NODE_CHECKPOINT) {
                    // 5.x replica, ignore for global checkpoint calculation
                } else {
                    minLocalCheckpoint = Math.min(cps.localCheckpoint, minLocalCheckpoint);
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
        final CheckpointState cps = checkpoints.get(shardAllocationId);
        final long globalCheckpoint = cps.globalCheckpoint;
        final long computedGlobalCheckpoint = computeGlobalCheckpoint(pendingInSync, checkpoints.values(), getGlobalCheckpoint());
        assert computedGlobalCheckpoint >= globalCheckpoint : "new global checkpoint [" + computedGlobalCheckpoint +
            "] is lower than previous one [" + globalCheckpoint + "]";
        if (globalCheckpoint != computedGlobalCheckpoint) {
            logger.trace("global checkpoint updated to [{}]", computedGlobalCheckpoint);
            cps.globalCheckpoint = computedGlobalCheckpoint;
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
        // copy clusterStateVersion and checkpoints and return
        // all the entries from checkpoints that are inSync: the reason we don't need to care about initializing non-insync entries
        // is that they will have to undergo a recovery attempt on the relocation target, and will hence be supplied by the cluster state
        // update on the relocation target once relocation completes). We could alternatively also copy the map as-is (it’s safe), and it
        // would be cleaned up on the target by cluster state updates.
        Map<String, CheckpointState> localCheckpointsCopy = new HashMap<>();
        for (Map.Entry<String, CheckpointState> entry : checkpoints.entrySet()) {
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
        // forget all checkpoint information except for global checkpoint of current shard
        checkpoints.entrySet().stream().forEach(e -> {
            final CheckpointState cps = e.getValue();
            if (cps.localCheckpoint != SequenceNumbers.UNASSIGNED_SEQ_NO &&
                cps.localCheckpoint != SequenceNumbers.PRE_60_NODE_CHECKPOINT) {
                cps.localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
            }
            if (e.getKey().equals(shardAllocationId) == false) {
                // don't throw global checkpoint information of current shard away
                if (cps.globalCheckpoint != SequenceNumbers.UNASSIGNED_SEQ_NO &&
                    cps.globalCheckpoint != SequenceNumbers.PRE_60_NODE_CHECKPOINT) {
                    cps.globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                }
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
        checkpoints.clear();
        for (Map.Entry<String, CheckpointState> entry : primaryContext.checkpoints.entrySet()) {
            checkpoints.put(entry.getKey(), entry.getValue().copy());
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
        checkpoints.entrySet().forEach(entry -> {
            if (entry.getValue().inSync) {
                inSyncAllocationIds.add(entry.getKey());
            }
            if (entry.getValue().getLocalCheckpoint() == SequenceNumbers.PRE_60_NODE_CHECKPOINT) {
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
    public synchronized CheckpointState getTrackedLocalCheckpointForShard(String allocationId) {
        assert primaryMode;
        return checkpoints.get(allocationId);
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
        private final Map<String, CheckpointState> checkpoints;
        private final IndexShardRoutingTable routingTable;

        public PrimaryContext(long clusterStateVersion, Map<String, CheckpointState> checkpoints,
                              IndexShardRoutingTable routingTable) {
            this.clusterStateVersion = clusterStateVersion;
            this.checkpoints = checkpoints;
            this.routingTable = routingTable;
        }

        public PrimaryContext(StreamInput in) throws IOException {
            clusterStateVersion = in.readVLong();
            checkpoints = in.readMap(StreamInput::readString, CheckpointState::new);
            routingTable = IndexShardRoutingTable.Builder.readFrom(in);
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        public Map<String, CheckpointState> getCheckpointStates() {
            return checkpoints;
        }

        public IndexShardRoutingTable getRoutingTable() {
            return routingTable;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(clusterStateVersion);
            out.writeMap(checkpoints, (streamOutput, s) -> out.writeString(s), (streamOutput, cps) -> cps.writeTo(out));
            IndexShardRoutingTable.Builder.writeTo(routingTable, out);
        }

        @Override
        public String toString() {
            return "PrimaryContext{" +
                    "clusterStateVersion=" + clusterStateVersion +
                    ", checkpoints=" + checkpoints +
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
            int result = Long.hashCode(clusterStateVersion);
            result = 31 * result + checkpoints.hashCode();
            result = 31 * result + routingTable.hashCode();
            return result;
        }
    }
}
