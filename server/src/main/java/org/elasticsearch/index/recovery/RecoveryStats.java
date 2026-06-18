/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// Recovery related statistics, starting at the shard level and allowing aggregation to indices and node level
public class RecoveryStats implements ToXContentFragment, Writeable {

    private static final TransportVersion SOURCE_QUEUED_STATS = TransportVersion.fromName("recovery_source_queued_stats");
    private static final TransportVersion STORE_AND_TARGET_QUEUED_STATS = TransportVersion.fromName(
        "recovery_store_and_target_queued_stats"
    );

    private final AtomicInteger currentAsSource = new AtomicInteger();
    private final AtomicInteger currentAsSourceQueued = new AtomicInteger();
    private final AtomicInteger currentAsTarget = new AtomicInteger();
    private final AtomicInteger currentAsTargetQueued = new AtomicInteger();
    private final AtomicInteger currentFromStore = new AtomicInteger();
    private final AtomicInteger currentFromStoreQueued = new AtomicInteger();

    private final AtomicLong throttleTimeInNanos = new AtomicLong();

    public RecoveryStats() {}

    /// Deserializes a [RecoveryStats] from the given stream.
    public RecoveryStats(StreamInput in) throws IOException {
        currentAsSource.set(in.readVInt());
        if (in.getTransportVersion().supports(SOURCE_QUEUED_STATS)) {
            currentAsSourceQueued.set(in.readVInt());
        } // else we cannot have any queued recoveries, the cluster is too old
        currentAsTarget.set(in.readVInt());
        if (in.getTransportVersion().supports(STORE_AND_TARGET_QUEUED_STATS)) {
            currentAsTargetQueued.set(in.readVInt());
            currentFromStore.set(in.readVInt());
            currentFromStoreQueued.set(in.readVInt());
        }
        throttleTimeInNanos.set(in.readLong());
    }

    /// Adds all counters and totals from `recoveryStats` into this instance.
    public void add(RecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.currentAsSource.addAndGet(recoveryStats.currentAsSource());
            this.currentAsSourceQueued.addAndGet(recoveryStats.currentAsSourceQueued());
            this.currentAsTarget.addAndGet(recoveryStats.currentAsTarget());
            this.currentAsTargetQueued.addAndGet(recoveryStats.currentAsTargetQueued());
            this.currentFromStore.addAndGet(recoveryStats.currentFromStore());
            this.currentFromStoreQueued.addAndGet(recoveryStats.currentFromStoreQueued());
        }
        addTotals(recoveryStats);
    }

    /// Adds only throttle-time totals from `recoveryStats` into this instance.
    public void addTotals(RecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.throttleTimeInNanos.addAndGet(recoveryStats.throttleTime().nanos());
        }
    }

    /// Number of ongoing peer recoveries for which a shard serves as a source.
    public int currentAsSource() {
        return currentAsSource.get();
    }

    /// Number of queued peer recoveries for which a shard serves as a source.
    public int currentAsSourceQueued() {
        return currentAsSourceQueued.get();
    }

    /// Number of ongoing peer recoveries for which a shard serves as a target.
    public int currentAsTarget() {
        return currentAsTarget.get();
    }

    /// Number of queued peer recoveries for which a shard serves as a target.
    public int currentAsTargetQueued() {
        return currentAsTargetQueued.get();
    }

    /// Number of ongoing non-peer (store) recoveries.
    public int currentFromStore() {
        return currentFromStore.get();
    }

    /// Number of queued non-peer (store) recoveries.
    public int currentFromStoreQueued() {
        return currentFromStoreQueued.get();
    }

    /// Returns `true` if there are no ongoing or queued recoveries for this shard.
    public boolean noCurrentRecoveries() {
        return currentAsSource.get() == 0
            && currentAsTarget.get() == 0
            && currentAsSourceQueued.get() == 0
            && currentAsTargetQueued.get() == 0
            && currentFromStore.get() == 0
            && currentFromStoreQueued.get() == 0;
    }

    /// Total time recoveries waited due to throttling.
    public TimeValue throttleTime() {
        return TimeValue.timeValueNanos(throttleTimeInNanos.get());
    }

    /// Records that a source-side peer recovery has been queued for this shard.
    public void sourceRecoveryQueued() {
        currentAsSourceQueued.incrementAndGet();
    }

    /// Records that a queued source-side peer recovery was discarded without starting.
    public void sourceQueuedRecoveryDiscarded() {
        currentAsSourceQueued.decrementAndGet();
    }

    /// Records that a source-side peer recovery has started directly, without a prior queue step.
    public void sourceRecoveryStarted() {
        currentAsSource.incrementAndGet();
    }

    /// Records that a queued source-side peer recovery has been dequeued and started.
    public void sourceRecoveryDequeuedAndStarted() {
        currentAsSourceQueued.decrementAndGet();
        currentAsSource.incrementAndGet();
    }

    /// Records that an active source-side peer recovery has completed.
    public void sourceRecoveryCompleted() {
        currentAsSource.decrementAndGet();
    }

    /// Records that a target-side (peer or non-peer) recovery has been queued for this shard.
    public void targetRecoveryQueued(RecoverySource.Type type) {
        if (type == RecoverySource.Type.PEER) {
            currentAsTargetQueued.incrementAndGet();
        } else {
            currentFromStoreQueued.incrementAndGet();
        }
    }

    /// Records that a queued target-side (peer or non-peer) recovery for this shard was discarded without starting.
    public void targetQueuedRecoveryDiscarded(RecoverySource.Type type) {
        if (type == RecoverySource.Type.PEER) {
            currentAsTargetQueued.decrementAndGet();
        } else {
            currentFromStoreQueued.decrementAndGet();
        }
    }

    /// Records that a queued target-side (peer or non-peer) recovery has been dequeued and started for this shard.
    public void targetRecoveryDequeuedAndStarted(RecoverySource.Type type) {
        if (type == RecoverySource.Type.PEER) {
            currentAsTargetQueued.decrementAndGet();
            currentAsTarget.incrementAndGet();
        } else {
            currentFromStoreQueued.decrementAndGet();
            currentFromStore.incrementAndGet();
        }
    }

    /// Records that an active target-side (peer or non-peer) recovery has completed for this shard.
    public void targetRecoveryCompleted(RecoverySource.Type type) {
        if (type == RecoverySource.Type.PEER) {
            currentAsTarget.decrementAndGet();
        } else {
            currentFromStore.decrementAndGet();
        }
    }

    /// Adds `nanos` to the total throttle time accumulated by this shard.
    public void addThrottleTime(long nanos) {
        throttleTimeInNanos.addAndGet(nanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.RECOVERY);
        builder.field(Fields.CURRENT_AS_SOURCE, currentAsSource());
        builder.field(Fields.CURRENT_AS_SOURCE_QUEUED, currentAsSourceQueued());
        builder.field(Fields.CURRENT_AS_TARGET, currentAsTarget());
        builder.field(Fields.CURRENT_AS_TARGET_QUEUED, currentAsTargetQueued());
        builder.field(Fields.CURRENT_FROM_STORE, currentFromStore());
        builder.field(Fields.CURRENT_FROM_STORE_QUEUED, currentFromStoreQueued());
        builder.humanReadableField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, throttleTime());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String RECOVERY = "recovery";
        static final String CURRENT_AS_SOURCE = "current_as_source";
        static final String CURRENT_AS_SOURCE_QUEUED = "current_as_source_queued";
        static final String CURRENT_AS_TARGET = "current_as_target";
        static final String CURRENT_AS_TARGET_QUEUED = "current_as_target_queued";
        static final String CURRENT_FROM_STORE = "current_from_store";
        static final String CURRENT_FROM_STORE_QUEUED = "current_from_store_queued";
        static final String THROTTLE_TIME = "throttle_time";
        static final String THROTTLE_TIME_IN_MILLIS = "throttle_time_in_millis";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(currentAsSource.get());
        if (out.getTransportVersion().supports(SOURCE_QUEUED_STATS)) {
            out.writeVInt(currentAsSourceQueued.get());
        }
        out.writeVInt(currentAsTarget.get());
        if (out.getTransportVersion().supports(STORE_AND_TARGET_QUEUED_STATS)) {
            out.writeVInt(currentAsTargetQueued.get());
            out.writeVInt(currentFromStore.get());
            out.writeVInt(currentFromStoreQueued.get());
        }
        out.writeLong(throttleTimeInNanos.get());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecoveryStats that = (RecoveryStats) o;
        return currentAsSource() == that.currentAsSource()
            && currentAsSourceQueued() == that.currentAsSourceQueued()
            && currentAsTarget() == that.currentAsTarget()
            && currentAsTargetQueued() == that.currentAsTargetQueued()
            && currentFromStore() == that.currentFromStore()
            && currentFromStoreQueued() == that.currentFromStoreQueued()
            && Objects.equals(throttleTime(), that.throttleTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            currentAsSource(),
            currentAsSourceQueued(),
            currentAsTarget(),
            currentAsTargetQueued(),
            currentFromStore(),
            currentFromStoreQueued(),
            throttleTime()
        );
    }

    @Override
    public String toString() {
        return "recoveryStats, currentAsSource ["
            + currentAsSource()
            + "], currentAsSourceQueued ["
            + currentAsSourceQueued()
            + "], currentAsTarget ["
            + currentAsTarget()
            + "], currentAsTargetQueued ["
            + currentAsTargetQueued()
            + "], currentFromStore ["
            + currentFromStore()
            + "], currentFromStoreQueued ["
            + currentFromStoreQueued()
            + "], throttle ["
            + throttleTime()
            + "]";
    }
}
