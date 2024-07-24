/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.flush;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class FlushStats implements Writeable, ToXContentFragment {

    private long total;
    private long periodic;
    private long totalTimeInMillis;
    private long totalTimeExcludingWaitingOnLockInMillis;

    public FlushStats() {

    }

    public FlushStats(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
        periodic = in.readVLong();
        totalTimeExcludingWaitingOnLockInMillis = in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0L;
    }

    public FlushStats(long total, long periodic, long totalTimeInMillis, long totalTimeExcludingWaitingOnLockInMillis) {
        this.total = total;
        this.periodic = periodic;
        this.totalTimeInMillis = totalTimeInMillis;
        this.totalTimeExcludingWaitingOnLockInMillis = totalTimeExcludingWaitingOnLockInMillis;
    }

    public void add(long total, long periodic, long totalTimeInMillis, long totalTimeWithoutWaitingInMillis) {
        this.total += total;
        this.periodic += periodic;
        this.totalTimeInMillis += totalTimeInMillis;
        this.totalTimeExcludingWaitingOnLockInMillis += totalTimeWithoutWaitingInMillis;
    }

    public void add(FlushStats flushStats) {
        addTotals(flushStats);
    }

    public void addTotals(FlushStats flushStats) {
        if (flushStats == null) {
            return;
        }
        this.total += flushStats.total;
        this.periodic += flushStats.periodic;
        this.totalTimeInMillis += flushStats.totalTimeInMillis;
        this.totalTimeExcludingWaitingOnLockInMillis += flushStats.totalTimeExcludingWaitingOnLockInMillis;
    }

    /**
     * The total number of flush executed.
     */
    public long getTotal() {
        return this.total;
    }

    /**
     * The number of flushes that were periodically triggered when translog exceeded the flush threshold.
     */
    public long getPeriodic() {
        return periodic;
    }

    /**
     * The total time merges have been executed (in milliseconds).
     */
    public long getTotalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time flushes have been executed.
     */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    /**
     * The total time flushes have been executed excluding waiting time on locks (in milliseconds).
     */
    public long getTotalTimeExcludingWaitingOnLockMillis() {
        return totalTimeExcludingWaitingOnLockInMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FLUSH);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.PERIODIC, periodic);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, getTotalTime());
        builder.humanReadableField(
            Fields.TOTAL_TIME_EXCLUDING_WAITING_ON_LOCK_IN_MILLIS,
            Fields.TOTAL_TIME_EXCLUDING_WAITING,
            new TimeValue(getTotalTimeExcludingWaitingOnLockMillis())
        );
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String FLUSH = "flush";
        static final String TOTAL = "total";
        static final String PERIODIC = "periodic";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String TOTAL_TIME_EXCLUDING_WAITING = "total_time_excluding_waiting";
        static final String TOTAL_TIME_EXCLUDING_WAITING_ON_LOCK_IN_MILLIS = "total_time_excluding_waiting_on_lock_in_millis";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(periodic);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeVLong(totalTimeExcludingWaitingOnLockInMillis);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlushStats that = (FlushStats) o;
        return total == that.total
            && totalTimeInMillis == that.totalTimeInMillis
            && periodic == that.periodic
            && totalTimeExcludingWaitingOnLockInMillis == that.totalTimeExcludingWaitingOnLockInMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, totalTimeInMillis, periodic, totalTimeExcludingWaitingOnLockInMillis);
    }
}
