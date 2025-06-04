/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING;

/**
 * Holds additional information as to why the shard is in an unassigned state.
 *
 * @param reason why the shard is unassigned.
 * @param message optional details explaining the reasons.
 * @param failure additional failure exception details if exists.
 * @param failedAllocations number of previously failed allocations of this shard.
 * @param delayed true if allocation of this shard is delayed due to {@link #INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING}.
 * @param unassignedTimeMillis The timestamp in milliseconds when the shard became unassigned, based on System.currentTimeMillis().
 *                             Note, we use timestamp here since we want to make sure its preserved across node serializations.
 * @param unassignedTimeNanos The timestamp in nanoseconds when the shard became unassigned, based on System.nanoTime().
 *                            Used to calculate the delay for delayed shard allocation.
 *                            ONLY EXPOSED FOR TESTS!
 * @param lastAllocationStatus status for the last allocation attempt for this shard.
 * @param failedNodeIds A set of nodeIds that failed to complete allocations for this shard.
 *                      {@link org.elasticsearch.gateway.ReplicaShardAllocator} uses this bset to avoid repeatedly canceling ongoing
 *                      recoveries for copies on those nodes, although they can perform noop recoveries. This set will be discarded when a
 *                      shard moves to started. And if a shard is failed while started (i.e., from started to unassigned), the currently
 *                      assigned node won't be added to this set.
 *                      @see org.elasticsearch.gateway.ReplicaShardAllocator#processExistingRecoveries
 *                      @see org.elasticsearch.cluster.routing.allocation.AllocationService#applyFailedShards(ClusterState, List, List)
 * @param lastAllocatedNodeId ID of the node this shard was last allocated to, or null if unavailable.
 */
public record UnassignedInfo(
    Reason reason,
    @Nullable String message,
    @Nullable Exception failure,
    int failedAllocations,
    long unassignedTimeNanos,
    long unassignedTimeMillis,
    boolean delayed,
    AllocationStatus lastAllocationStatus,
    Set<String> failedNodeIds,
    @Nullable String lastAllocatedNodeId
) implements ToXContentFragment, Writeable {

    private static final TransportVersion VERSION_UNPROMOTABLE_REPLICA_ADDED = TransportVersions.V_8_7_0;

    public static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("date_optional_time").withZone(ZoneOffset.UTC);

    public static final Setting<TimeValue> INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING = Setting.timeSetting(
        "index.unassigned.node_left.delayed_timeout",
        settings -> EXISTING_SHARDS_ALLOCATOR_SETTING.get(settings).equals("stateless")
            ? TimeValue.timeValueSeconds(10)
            : TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMillis(0),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Reason why the shard is in unassigned state.
     * <p>
     * Note, ordering of the enum is important, make sure to add new values
     * at the end and handle version serialization properly.
     */
    public enum Reason {
        /**
         * Unassigned as a result of an API creation of an index.
         */
        INDEX_CREATED,
        /**
         * Unassigned as a result of a full cluster recovery.
         */
        CLUSTER_RECOVERED,
        /**
         * Unassigned as a result of opening a closed index.
         */
        INDEX_REOPENED,
        /**
         * Unassigned as a result of importing a dangling index.
         */
        DANGLING_INDEX_IMPORTED,
        /**
         * Unassigned as a result of restoring into a new index.
         */
        NEW_INDEX_RESTORED,
        /**
         * Unassigned as a result of restoring into a closed index.
         */
        EXISTING_INDEX_RESTORED,
        /**
         * Unassigned as a result of explicit addition of a replica.
         */
        REPLICA_ADDED,
        /**
         * Unassigned as a result of a failed allocation of the shard.
         */
        ALLOCATION_FAILED,
        /**
         * Unassigned as a result of the node hosting it leaving the cluster.
         */
        NODE_LEFT,
        /**
         * Unassigned as a result of explicit cancel reroute command.
         */
        REROUTE_CANCELLED,
        /**
         * When a shard moves from started back to initializing.
         */
        REINITIALIZED,
        /**
         * A better replica location is identified and causes the existing replica allocation to be cancelled.
         */
        REALLOCATED_REPLICA,
        /**
         * Unassigned as a result of a failed primary while the replica was initializing.
         */
        PRIMARY_FAILED,
        /**
         * Unassigned after forcing an empty primary
         */
        FORCED_EMPTY_PRIMARY,
        /**
         * Forced manually to allocate
         */
        MANUAL_ALLOCATION,
        /**
         * Unassigned as a result of closing an index.
         */
        INDEX_CLOSED,
        /**
         * Similar to NODE_LEFT, but at the time the node left, it had been registered for a restart via the Node Shutdown API. Note that
         * there is no verification that it was ready to be restarted, so this may be an intentional restart or a node crash.
         */
        NODE_RESTARTING,
        /**
         * Replica is unpromotable and the primary failed.
         */
        UNPROMOTABLE_REPLICA,
        /**
         * New shard added as part of index re-sharding operation
         */
        RESHARD_ADDED
    }

    /**
     * Captures the status of an unsuccessful allocation attempt for the shard,
     * causing it to remain in the unassigned state.
     *
     * Note, ordering of the enum is important, make sure to add new values
     * at the end and handle version serialization properly.
     */
    public enum AllocationStatus implements Writeable {
        /**
         * The shard was denied allocation to a node because the allocation deciders all returned a NO decision
         */
        DECIDERS_NO((byte) 0),
        /**
         * The shard was denied allocation to a node because there were no valid shard copies found for it;
         * this can happen on node restart with gateway allocation
         */
        NO_VALID_SHARD_COPY((byte) 1),
        /**
         * The allocation attempt was throttled on the shard by the allocation deciders
         */
        DECIDERS_THROTTLED((byte) 2),
        /**
         * Waiting on getting shard data from all nodes before making a decision about where to allocate the shard
         */
        FETCHING_SHARD_DATA((byte) 3),
        /**
         * Allocation decision has been delayed
         */
        DELAYED_ALLOCATION((byte) 4),
        /**
         * No allocation attempt has been made yet
         */
        NO_ATTEMPT((byte) 5);

        private final byte id;

        AllocationStatus(byte id) {
            this.id = id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static AllocationStatus readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            return switch (id) {
                case 0 -> DECIDERS_NO;
                case 1 -> NO_VALID_SHARD_COPY;
                case 2 -> DECIDERS_THROTTLED;
                case 3 -> FETCHING_SHARD_DATA;
                case 4 -> DELAYED_ALLOCATION;
                case 5 -> NO_ATTEMPT;
                default -> throw new IllegalArgumentException("Unknown AllocationStatus value [" + id + "]");
            };
        }

        public static AllocationStatus fromDecision(Decision.Type decision) {
            Objects.requireNonNull(decision);
            return switch (decision) {
                case NO -> DECIDERS_NO;
                case THROTTLE -> DECIDERS_THROTTLED;
                default -> throw new IllegalArgumentException("no allocation attempt from decision[" + decision + "]");
            };
        }

        public String value() {
            return toString().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * creates an UnassignedInfo object based on **current** time
     *
     * @param reason  the cause for making this shard unassigned. See {@link Reason} for more information.
     * @param message more information about cause.
     **/
    public UnassignedInfo(Reason reason, String message) {
        this(
            reason,
            message,
            null,
            reason == Reason.ALLOCATION_FAILED ? 1 : 0,
            System.nanoTime(),
            System.currentTimeMillis(),
            false,
            AllocationStatus.NO_ATTEMPT,
            Collections.emptySet(),
            null
        );
    }

    /**
     * @param reason                          the cause for making this shard unassigned. See {@link Reason} for more information.
     * @param message                         more information about cause.
     * @param failure                         the shard level failure that caused this shard to be unassigned, if exists.
     * @param unassignedTimeNanos             the time to use as the base for any delayed re-assignment calculation
     * @param unassignedTimeMillis            the time of unassignment used to display to in our reporting.
     * @param delayed                         if allocation of this shard is delayed due to INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.
     * @param lastAllocationStatus            the result of the last allocation attempt for this shard
     * @param failedNodeIds                   a set of nodeIds that failed to complete allocations for this shard
     * @param lastAllocatedNodeId             the ID of the node this shard was last allocated to
     */
    public UnassignedInfo {
        Objects.requireNonNull(reason);
        Objects.requireNonNull(lastAllocationStatus);
        failedNodeIds = Set.copyOf(failedNodeIds);
        assert (failedAllocations > 0) == (reason == Reason.ALLOCATION_FAILED)
            : "failedAllocations: " + failedAllocations + " for reason " + reason;
        assert (message == null && failure != null) == false : "provide a message if a failure exception is provided";
        assert (delayed && reason != Reason.NODE_LEFT && reason != Reason.NODE_RESTARTING) == false
            : "shard can only be delayed if it is unassigned due to a node leaving";
        // The below check should be expanded to require `lastAllocatedNodeId` for `NODE_LEFT` as well, once we no longer have to consider
        // BWC with versions prior to `VERSION_LAST_ALLOCATED_NODE_ADDED`.
        assert (reason == Reason.NODE_RESTARTING && lastAllocatedNodeId == null) == false
            : "last allocated node ID must be set if the shard is unassigned due to a node restarting";
    }

    public static UnassignedInfo fromStreamInput(StreamInput in) throws IOException {
        // Because Reason.NODE_RESTARTING is new and can't be sent by older versions, there's no need to vary the deserialization behavior
        var reason = Reason.values()[(int) in.readByte()];
        var unassignedTimeMillis = in.readLong();
        // As System.nanoTime() cannot be compared across different JVMs, reset it to now.
        // This means that in master fail-over situations, elapsed delay time is forgotten.
        var unassignedTimeNanos = System.nanoTime();
        var delayed = in.readBoolean();
        var message = in.readOptionalString();
        var failure = in.readException();
        var failedAllocations = in.readVInt();
        var lastAllocationStatus = AllocationStatus.readFrom(in);
        var failedNodeIds = in.readCollectionAsImmutableSet(StreamInput::readString);
        String lastAllocatedNodeId;
        lastAllocatedNodeId = in.readOptionalString();
        return new UnassignedInfo(
            reason,
            message,
            failure,
            failedAllocations,
            unassignedTimeNanos,
            unassignedTimeMillis,
            delayed,
            lastAllocationStatus,
            failedNodeIds,
            lastAllocatedNodeId
        );
    }

    public void writeTo(StreamOutput out) throws IOException {
        if (reason.equals(Reason.UNPROMOTABLE_REPLICA) && out.getTransportVersion().before(VERSION_UNPROMOTABLE_REPLICA_ADDED)) {
            out.writeByte((byte) Reason.PRIMARY_FAILED.ordinal());
        } else if (reason.equals(Reason.RESHARD_ADDED)
            && out.getTransportVersion().before(TransportVersions.UNASSIGENEDINFO_RESHARD_ADDED)) {
                // We should have protection to ensure we do not reshard in mixed clusters
                assert false;
                out.writeByte((byte) Reason.FORCED_EMPTY_PRIMARY.ordinal());
            } else {
                out.writeByte((byte) reason.ordinal());
            }
        out.writeLong(unassignedTimeMillis);
        // Do not serialize unassignedTimeNanos as System.nanoTime() cannot be compared across different JVMs
        out.writeBoolean(delayed);
        out.writeOptionalString(message);
        out.writeException(failure);
        out.writeVInt(failedAllocations);
        lastAllocationStatus.writeTo(out);
        out.writeStringCollection(failedNodeIds);
        out.writeOptionalString(lastAllocatedNodeId);
    }

    /**
     * Builds a string representation of the message and the failure if exists.
     */
    @Nullable
    public String details() {
        if (message == null) {
            return null;
        }
        return message + (failure == null ? "" : ", failure " + ExceptionsHelper.stackTrace(failure));
    }

    /**
     * Calculates the delay left based on current time (in nanoseconds) and the delay defined by the index settings.
     * Only relevant if shard is effectively delayed (see {@link #delayed()})
     * Returns 0 if delay is negative
     *
     * @return calculated delay in nanoseconds
     */
    public long remainingDelay(final long nanoTimeNow, final Settings indexSettings, final NodesShutdownMetadata nodesShutdownMetadata) {
        final long indexLevelDelay = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexSettings).nanos();
        long delayTimeoutNanos = Optional.ofNullable(lastAllocatedNodeId)
            // If the node wasn't restarting when this became unassigned, use default delay
            .filter(nodeId -> reason.equals(Reason.NODE_RESTARTING))
            .map(nodeId -> nodesShutdownMetadata.get(nodeId, SingleNodeShutdownMetadata.Type.RESTART))
            .map(SingleNodeShutdownMetadata::getAllocationDelay)
            .map(TimeValue::nanos)
            .map(knownRestartDelay -> Math.max(indexLevelDelay, knownRestartDelay))
            .orElse(indexLevelDelay);
        assert nanoTimeNow - unassignedTimeNanos >= 0;
        return Math.max(0L, delayTimeoutNanos - (nanoTimeNow - unassignedTimeNanos));
    }

    /**
     * Returns the number of shards that are unassigned and currently being delayed.
     */
    public static int getNumberOfDelayedUnassigned(ClusterState state) {
        int count = 0;
        for (ShardRouting shard : state.getRoutingNodes().unassigned()) {
            if (shard.unassignedInfo().delayed()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Finds the next (closest) delay expiration of an delayed shard in nanoseconds based on current time.
     * Returns 0 if delay is negative.
     * Returns -1 if no delayed shard is found.
     */
    public static long findNextDelayedAllocation(long currentNanoTime, ClusterState state) {
        Metadata metadata = state.metadata();
        long nextDelayNanos = Long.MAX_VALUE;
        for (ShardRouting shard : state.getRoutingNodes().unassigned()) {
            UnassignedInfo unassignedInfo = shard.unassignedInfo();
            if (unassignedInfo.delayed()) {
                Settings indexSettings = metadata.indexMetadata(shard.index()).getSettings();
                // calculate next time to schedule
                final long newComputedLeftDelayNanos = unassignedInfo.remainingDelay(
                    currentNanoTime,
                    indexSettings,
                    metadata.nodeShutdowns()
                );
                if (newComputedLeftDelayNanos < nextDelayNanos) {
                    nextDelayNanos = newComputedLeftDelayNanos;
                }
            }
        }
        return nextDelayNanos == Long.MAX_VALUE ? -1L : nextDelayNanos;
    }

    public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("[reason=").append(reason).append("]");
        sb.append(", at[").append(DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(unassignedTimeMillis))).append("]");
        if (failedAllocations > 0) {
            sb.append(", failed_attempts[").append(failedAllocations).append("]");
        }
        if (failedNodeIds.isEmpty() == false) {
            sb.append(", failed_nodes[").append(failedNodeIds).append("]");
        }
        sb.append(", delayed=").append(delayed);
        if (lastAllocatedNodeId != null) {
            sb.append(", last_node[").append(lastAllocatedNodeId).append("]");
        }
        String details = details();
        if (details != null) {
            sb.append(", details[").append(details).append("]");
        }
        sb.append(", allocation_status[").append(lastAllocationStatus.value()).append("]");
        return sb.toString();
    }

    @Override
    public String toString() {
        return "unassigned_info[" + shortSummary() + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("unassigned_info");
        builder.field("reason", reason);
        builder.field("at", DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(unassignedTimeMillis)));
        if (failedAllocations > 0) {
            builder.field("failed_attempts", failedAllocations);
        }
        if (failedNodeIds.isEmpty() == false) {
            builder.stringListField("failed_nodes", failedNodeIds);
        }
        builder.field("delayed", delayed);
        if (lastAllocatedNodeId != null) {
            builder.field("last_node", lastAllocatedNodeId);
        }
        String details = details();
        if (details != null) {
            builder.field("details", details);
        }
        builder.field("allocation_status", lastAllocationStatus.value());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnassignedInfo that = (UnassignedInfo) o;

        if (unassignedTimeMillis != that.unassignedTimeMillis) {
            return false;
        }
        if (delayed != that.delayed) {
            return false;
        }
        if (failedAllocations != that.failedAllocations) {
            return false;
        }
        if (reason != that.reason) {
            return false;
        }
        if (Objects.equals(message, that.message) == false) {
            return false;
        }
        if (lastAllocationStatus != that.lastAllocationStatus) {
            return false;
        }
        if (Objects.equals(failure, that.failure) == false) {
            return false;
        }

        if (Objects.equals(lastAllocatedNodeId, that.lastAllocatedNodeId) == false) {
            return false;
        }

        return failedNodeIds.equals(that.failedNodeIds);
    }

    @Override
    public int hashCode() {
        int result = reason.hashCode();
        result = 31 * result + Boolean.hashCode(delayed);
        result = 31 * result + Integer.hashCode(failedAllocations);
        result = 31 * result + Long.hashCode(unassignedTimeMillis);
        result = 31 * result + (message != null ? message.hashCode() : 0);
        result = 31 * result + (failure != null ? failure.hashCode() : 0);
        result = 31 * result + lastAllocationStatus.hashCode();
        result = 31 * result + failedNodeIds.hashCode();
        result = 31 * result + (lastAllocatedNodeId != null ? lastAllocatedNodeId.hashCode() : 0);
        return result;
    }

}
