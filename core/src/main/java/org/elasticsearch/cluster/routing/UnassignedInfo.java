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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Holds additional information as to why the shard is in unassigned state.
 */
public final class UnassignedInfo implements ToXContent, Writeable {

    public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("dateOptionalTime");

    public static final Setting<TimeValue> INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING =
        Setting.timeSetting("index.unassigned.node_left.delayed_timeout", TimeValue.timeValueMinutes(1), Property.Dynamic,
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
         * When a shard moves from started back to initializing, for example, during shadow replica
         */
        REINITIALIZED,
        /**
         * A better replica location is identified and causes the existing replica allocation to be cancelled.
         */
        REALLOCATED_REPLICA,
        /**
         * Unassigned as a result of a failed primary while the replica was initializing.
         */
        PRIMARY_FAILED;
    }

    private final Reason reason;
    private final long unassignedTimeMillis; // used for display and log messages, in milliseconds
    private final long unassignedTimeNanos; // in nanoseconds, used to calculate delay for delayed shard allocation
    private final boolean delayed; // if allocation of this shard is delayed due to INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING
    private final String message;
    private final Throwable failure;
    private final int failedAllocations;

    /**
     * creates an UnassignedInfo object based on **current** time
     *
     * @param reason  the cause for making this shard unassigned. See {@link Reason} for more information.
     * @param message more information about cause.
     **/
    public UnassignedInfo(Reason reason, String message) {
        this(reason, message, null, reason == Reason.ALLOCATION_FAILED ? 1 : 0, System.nanoTime(), System.currentTimeMillis(), false);
    }

    /**
     * @param reason               the cause for making this shard unassigned. See {@link Reason} for more information.
     * @param message              more information about cause.
     * @param failure              the shard level failure that caused this shard to be unassigned, if exists.
     * @param unassignedTimeNanos  the time to use as the base for any delayed re-assignment calculation
     * @param unassignedTimeMillis the time of unassignment used to display to in our reporting.
     * @param delayed              if allocation of this shard is delayed due to INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.
     */
    public UnassignedInfo(Reason reason, @Nullable String message, @Nullable Throwable failure, int failedAllocations,
        long unassignedTimeNanos, long unassignedTimeMillis, boolean delayed) {
        this.reason = reason;
        this.unassignedTimeMillis = unassignedTimeMillis;
        this.unassignedTimeNanos = unassignedTimeNanos;
        this.delayed = delayed;
        this.message = message;
        this.failure = failure;
        this.failedAllocations = failedAllocations;
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
        this.failure = in.readThrowable();
        this.failedAllocations = in.readVInt();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) reason.ordinal());
        out.writeLong(unassignedTimeMillis);
        // Do not serialize unassignedTimeNanos as System.nanoTime() cannot be compared across different JVMs
        out.writeBoolean(delayed);
        out.writeOptionalString(message);
        out.writeThrowable(failure);
        out.writeVInt(failedAllocations);
    }

    public UnassignedInfo readFrom(StreamInput in) throws IOException {
        return new UnassignedInfo(in);
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
    public Throwable getFailure() {
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
        return message + (failure == null ? "" : ", failure " + ExceptionsHelper.detailedMessage(failure));
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
        MetaData metaData = state.metaData();
        RoutingTable routingTable = state.routingTable();
        long nextDelayNanos = Long.MAX_VALUE;
        for (ShardRouting shard : routingTable.shardsWithState(ShardRoutingState.UNASSIGNED)) {
            UnassignedInfo unassignedInfo = shard.unassignedInfo();
            if (unassignedInfo.isDelayed()) {
                Settings indexSettings = metaData.index(shard.index()).getSettings();
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
        sb.append(", at[").append(DATE_TIME_FORMATTER.printer().print(unassignedTimeMillis)).append("]");
        if (failedAllocations >  0) {
            sb.append(", failed_attempts[").append(failedAllocations).append("]");
        }
        sb.append(", delayed=").append(delayed);
        String details = getDetails();

        if (details != null) {
            sb.append(", details[").append(details).append("]");
        }
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
        builder.field("at", DATE_TIME_FORMATTER.printer().print(unassignedTimeMillis));
        if (failedAllocations >  0) {
            builder.field("failed_attempts", failedAllocations);
        }
        builder.field("delayed", delayed);
        String details = getDetails();
        if (details != null) {
            builder.field("details", details);
        }
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
        if (message != null ? !message.equals(that.message) : that.message != null) {
            return false;
        }
        return !(failure != null ? !failure.equals(that.failure) : that.failure != null);
    }

    @Override
    public int hashCode() {
        int result = reason != null ? reason.hashCode() : 0;
        result = 31 * result + Boolean.hashCode(delayed);
        result = 31 * result + Integer.hashCode(failedAllocations);
        result = 31 * result + Long.hashCode(unassignedTimeMillis);
        result = 31 * result + (message != null ? message.hashCode() : 0);
        result = 31 * result + (failure != null ? failure.hashCode() : 0);
        return result;
    }
}
