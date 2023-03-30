/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.recovery;

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

/**
 * Recovery related statistics, starting at the shard level and allowing aggregation to
 * indices and node level
 */
public class RecoveryStats implements ToXContentFragment, Writeable {

    private final AtomicInteger currentAsSource = new AtomicInteger();
    private final AtomicInteger currentAsTarget = new AtomicInteger();
    private final AtomicLong throttleTimeInNanos = new AtomicLong();

    public RecoveryStats() {}

    public RecoveryStats(StreamInput in) throws IOException {
        currentAsSource.set(in.readVInt());
        currentAsTarget.set(in.readVInt());
        throttleTimeInNanos.set(in.readLong());
    }

    public void add(RecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.currentAsSource.addAndGet(recoveryStats.currentAsSource());
            this.currentAsTarget.addAndGet(recoveryStats.currentAsTarget());
        }
        addTotals(recoveryStats);
    }

    public void addTotals(RecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.throttleTimeInNanos.addAndGet(recoveryStats.throttleTime().nanos());
        }
    }

    /**
     * Number of ongoing recoveries for which a shard serves as a source
     */
    public int currentAsSource() {
        return currentAsSource.get();
    }

    /**
     * Number of ongoing recoveries for which a shard serves as a target
     */
    public int currentAsTarget() {
        return currentAsTarget.get();
    }

    /**
     * Total time recoveries waited due to throttling
     */
    public TimeValue throttleTime() {
        return TimeValue.timeValueNanos(throttleTimeInNanos.get());
    }

    public void incCurrentAsTarget() {
        currentAsTarget.incrementAndGet();
    }

    public void decCurrentAsTarget() {
        currentAsTarget.decrementAndGet();
    }

    public void incCurrentAsSource() {
        currentAsSource.incrementAndGet();
    }

    public void decCurrentAsSource() {
        currentAsSource.decrementAndGet();
    }

    public void addThrottleTime(long nanos) {
        throttleTimeInNanos.addAndGet(nanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.RECOVERY);
        builder.field(Fields.CURRENT_AS_SOURCE, currentAsSource());
        builder.field(Fields.CURRENT_AS_TARGET, currentAsTarget());
        builder.humanReadableField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, throttleTime());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String RECOVERY = "recovery";
        static final String CURRENT_AS_SOURCE = "current_as_source";
        static final String CURRENT_AS_TARGET = "current_as_target";
        static final String THROTTLE_TIME = "throttle_time";
        static final String THROTTLE_TIME_IN_MILLIS = "throttle_time_in_millis";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(currentAsSource.get());
        out.writeVInt(currentAsTarget.get());
        out.writeLong(throttleTimeInNanos.get());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecoveryStats that = (RecoveryStats) o;
        return currentAsSource() == that.currentAsSource()
            && currentAsTarget() == that.currentAsTarget()
            && Objects.equals(throttleTime(), that.throttleTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentAsSource(), currentAsTarget(), throttleTime());
    }

    @Override
    public String toString() {
        return "recoveryStats, currentAsSource ["
            + currentAsSource()
            + "],currentAsTarget ["
            + currentAsTarget()
            + "], throttle ["
            + throttleTime()
            + "]";
    }
}
