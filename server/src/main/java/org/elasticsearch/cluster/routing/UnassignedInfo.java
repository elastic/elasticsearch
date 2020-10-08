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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Holds additional information as to why the shard is in unassigned state.
 */
public final class UnassignedInfo implements ToXContentFragment, Writeable {

    public static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("date_optional_time").withZone(ZoneOffset.UTC);

    public static final Setting<TimeValue> INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("index.unassigned.node_left.delayed_timeout", TimeValue.timeValueMinutes(1), Property.Dynamic,
            Property.IndexScope);
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
        INDEX_CLOSED
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
            switch (id) {
                case 0:
                    return DECIDERS_NO;
                case 1:
                    return NO_VALID_SHARD_COPY;
                case 2:
                    return DECIDERS_THROTTLED;
                case 3:
                    return FETCHING_SHARD_DATA;
                case 4:
                    return DELAYED_ALLOCATION;
                case 5:
                    return NO_ATTEMPT;
                default:
                    throw new IllegalArgumentException("Unknown AllocationStatus value [" + id + "]");
            }
        }

        public static AllocationStatus fromDecision(Decision.Type decision) {
            Objects.requireNonNull(decision);
            switch (decision) {
                case NO:
                    return DECIDERS_NO;
                case THROTTLE:
                    return DECIDERS_THROTTLED;
                default:
                    throw new IllegalArgumentException("no allocation attempt from decision[" + decision + "]");
            }
        }

        public String value() {
            return toString().toLowerCase(Locale.ROOT);
        }
    }

    private final Reason reason;
    private final long unassignedTimeMillis; // used for display and log messages, in milliseconds
    private final long unassignedTimeNanos; // in nanoseconds, used to calculate delay for delayed shard allocation
    private final boolean delayed; // if allocation of this shard is delayed due to INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING
    private final String message;
    private final Exception failure;
    private final int failedAllocations;
    private final Set<String> failedNodeIds;
    private final AllocationStatus lastAllocationStatus; // result of the last allocation attempt for this shard

    /**
     * creates an UnassignedInfo object based on **current** time
     *
     * @param reason  the cause for making this shard unassigned. See {@link Reason} for more information.
     * @param message more information about cause.
     **/
    public UnassignedInfo(Reason reason, String message) {
        this(reason, message, null, reason == Reason.ALLOCATION_FAILED ? 1 : 0, System.nanoTime(), System.currentTimeMillis(), false,
             AllocationStatus.NO_ATTEMPT, Collections.emptySet());
    }

    /**
     * @param reason               the cause for making this shard unassigned. See {@link Reason} for more information.
     * @param message              more information about cause.
     * @param failure              the shard level failure that caused this shard to be unassigned, if exists.
     * @param unassignedTimeNanos  the time to use as the base for any delayed re-assignment calculation
     * @param unassignedTimeMillis the time of unassignment used to display to in our reporting.
     * @param delayed              if allocation of this shard is delayed due to INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.
     * @param lastAllocationStatus the result of the last allocation attempt for this shard
     * @param failedNodeIds        a set of nodeIds that failed to complete allocations for this shard
     */
    public UnassignedInfo(Reason reason, @Nullable String message, @Nullable Exception failure, int failedAllocations,
                          long unassignedTimeNanos, long unassignedTimeMillis, boolean delayed, AllocationStatus lastAllocationStatus,
                          Set<String> failedNodeIds) {
        this.reason = Objects.requireNonNull(reason);
        this.unassignedTimeMillis = unassignedTimeMillis;
        this.unassignedTimeNanos = unassignedTimeNanos;
        this.delayed = delayed;
        this.message = message;
        this.failure = failure;
        this.failedAllocations = failedAllocations;
        this.lastAllocationStatus = Objects.requireNonNull(lastAllocationStatus);
        this.failedNodeIds = Collections.unmodifiableSet(failedNodeIds);
        assert (failedAllocations > 0) == (reason == Reason.ALLOCATION_FAILED) :
            "failedAllocations: " + failedAllocations + " for reason " + reason;
        assert !(message == null && failure != null) : "provide a message if a failure exception is provided";
        assert !(delayed && reason != Reason.NODE_LEFT) : "shard can only be delayed if it is unassigned due to a node leaving";
    }

    public UnassignedInfo(StreamInput in) throws IOException {
        this.reason = Reason.values()[(int) in.readByte()];
        this.unassignedTimeMillis = in.readLong();
        // As System.nanoTime() cannot be compared across different JVMs, reset it to now.
        // This means that in master fail-over situations, elapsed delay time is forgotten.
        this.unassignedTimeNanos = System.nanoTime();
        this.delayed = in.readBoolean();
        this.message = in.readOptionalString();
        this.failure = in.readException();
        this.failedAllocations = in.readVInt();
        this.lastAllocationStatus = AllocationStatus.readFrom(in);
        this.failedNodeIds = Collections.unmodifiableSet(in.readSet(StreamInput::readString));
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) reason.ordinal());
        out.writeLong(unassignedTimeMillis);
        // Do not serialize unassignedTimeNanos as System.nanoTime() cannot be compared across different JVMs
        out.writeBoolean(delayed);
        out.writeOptionalString(message);
        out.writeException(failure);
        out.writeVInt(failedAllocations);
        lastAllocationStatus.writeTo(out);
        out.writeCollection(failedNodeIds, StreamOutput::writeString);
    }

    /**
     * Returns the number of previously failed allocations of this shard.
     */
    public int getNumFailedAllocations() {
        return failedAllocations;
    }

    /**
     * Returns true if allocation of this shard is delayed due to {@link #INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING}
     */
    public boolean isDelayed() {
        return delayed;
    }

    /**
     * The reason why the shard is unassigned.
     */
    public Reason getReason() {
        return this.reason;
    }

    /**
     * The timestamp in milliseconds when the shard became unassigned, based on System.currentTimeMillis().
     * Note, we use timestamp here since we want to make sure its preserved across node serializations.
     */
    public long getUnassignedTimeInMillis() {
        return this.unassignedTimeMillis;
    }

    /**
     * The timestamp in nanoseconds when the shard became unassigned, based on System.nanoTime().
     * Used to calculate the delay for delayed shard allocation.
     * ONLY EXPOSED FOR TESTS!
     */
    public long getUnassignedTimeInNanos() {
        return this.unassignedTimeNanos;
    }

    /**
     * Returns optional details explaining the reasons.
     */
    @Nullable
    public String getMessage() {
        return this.message;
    }

    /**
     * Returns additional failure exception details if exists.
     */
    @Nullable
    public Exception getFailure() {
        return failure;
    }

    /**
     * Builds a string representation of the message and the failure if exists.
     */
    @Nullable
    public String getDetails() {
        if (message == null) {
            return null;
        }
        return message + (failure == null ? "" : ", failure " + ExceptionsHelper.stackTrace(failure));
    }

    /**
     * Get the status for the last allocation attempt for this shard.
     */
    public AllocationStatus getLastAllocationStatus() {
        return lastAllocationStatus;
    }

    /**
     * A set of nodeIds that failed to complete allocations for this shard. {@link org.elasticsearch.gateway.ReplicaShardAllocator}
     * uses this set to avoid repeatedly canceling ongoing recoveries for copies on those nodes although they can perform noop recoveries.
     * This set will be discarded when a shard moves to started. And if a shard is failed while started (i.e., from started to unassigned),
     * the currently assigned node won't be added to this set.
     *
     * @see org.elasticsearch.gateway.ReplicaShardAllocator#processExistingRecoveries(RoutingAllocation)
     * @see org.elasticsearch.cluster.routing.allocation.AllocationService#applyFailedShards(ClusterState, List, List)
     */
    public Set<String> getFailedNodeIds() {
        return failedNodeIds;
    }

    /**
     * Calculates the delay left based on current time (in nanoseconds) and the delay defined by the index settings.
     * Only relevant if shard is effectively delayed (see {@link #isDelayed()})
     * Returns 0 if delay is negative
     *
     * @return calculated delay in nanoseconds
     */
    public long getRemainingDelay(final long nanoTimeNow, final Settings indexSettings) {
        long delayTimeoutNanos = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexSettings).nanos();
        assert nanoTimeNow >= unassignedTimeNanos;
        return Math.max(0L, delayTimeoutNanos - (nanoTimeNow - unassignedTimeNanos));
    }

    /**
     * Returns the number of shards that are unassigned and currently being delayed.
     */
    public static int getNumberOfDelayedUnassigned(ClusterState state) {
        int count = 0;
        for (ShardRouting shard : state.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED)) {
            if (shard.unassignedInfo().isDelayed()) {
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
        RoutingTable routingTable = state.routingTable();
        long nextDelayNanos = Long.MAX_VALUE;
        for (ShardRouting shard : routingTable.shardsWithState(ShardRoutingState.UNASSIGNED)) {
            UnassignedInfo unassignedInfo = shard.unassignedInfo();
            if (unassignedInfo.isDelayed()) {
                Settings indexSettings = metadata.index(shard.index()).getSettings();
                // calculate next time to schedule
                final long newComputedLeftDelayNanos = unassignedInfo.getRemainingDelay(currentNanoTime, indexSettings);
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
        if (failedAllocations >  0) {
            sb.append(", failed_attempts[").append(failedAllocations).append("]");
        }
        if (failedNodeIds.isEmpty() == false) {
            sb.append(", failed_nodes[").append(failedNodeIds).append("]");
        }
        sb.append(", delayed=").append(delayed);
        String details = getDetails();

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
        if (failedAllocations >  0) {
            builder.field("failed_attempts", failedAllocations);
        }
        if (failedNodeIds.isEmpty() == false) {
            builder.field("failed_nodes", failedNodeIds);
        }
        builder.field("delayed", delayed);
        String details = getDetails();
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
        return result;
    }

}
