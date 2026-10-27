/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.flush;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class FlushStats implements Writeable, ToXContentFragment {

    private static final TransportVersion FLUSH_STATS_INCLUDE_FAILURES = TransportVersion.fromName("flush_stats_include_failures");

    private long total;
    private long periodic;
    private long totalTimeInMillis;
    private long totalTimeExcludingWaitingOnLockInMillis;
    private long failed;
    private long failedShardClosed;
    private long failedEngineClosed;
    private long failedPendingTranslogRecovery;

    public FlushStats() {

    }

    public FlushStats(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
        periodic = in.readVLong();
        totalTimeExcludingWaitingOnLockInMillis = in.readVLong();
        if (in.getTransportVersion().supports(FLUSH_STATS_INCLUDE_FAILURES)) {
            failed = in.readVLong();
            failedShardClosed = in.readVLong();
            failedEngineClosed = in.readVLong();
            failedPendingTranslogRecovery = in.readVLong();
        }
    }

    public FlushStats(
        long total,
        long periodic,
        long totalTimeInMillis,
        long totalTimeExcludingWaitingOnLockInMillis,
        long failed,
        long failedShardClosed,
        long failedEngineClosed,
        long failedPendingTranslogRecovery
    ) {
        this.total = total;
        this.periodic = periodic;
        this.totalTimeInMillis = totalTimeInMillis;
        this.totalTimeExcludingWaitingOnLockInMillis = totalTimeExcludingWaitingOnLockInMillis;
        this.failed = failed;
        this.failedShardClosed = failedShardClosed;
        this.failedEngineClosed = failedEngineClosed;
        this.failedPendingTranslogRecovery = failedPendingTranslogRecovery;
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
        this.failed += flushStats.failed;
        this.failedShardClosed += flushStats.failedShardClosed;
        this.failedEngineClosed += flushStats.failedEngineClosed;
        this.failedPendingTranslogRecovery += flushStats.failedPendingTranslogRecovery;
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

    /**
     * The number of flushes that failed for any reason, including the expected failures counted by {@link #getFailedShardClosed()},
     * {@link #getFailedEngineClosed()} and {@link #getFailedPendingTranslogRecovery()}. The flush that an engine runs as it closes is not
     * counted.
     */
    public long getFailed() {
        return failed;
    }

    /**
     * The number of flushes that failed because the shard was closed (a subset of {@link #getFailed()}). This is a best-effort count: a
     * shard counts these failures only after it is closed, which is after its stats are added to the node totals, so node level stats
     * do not include them.
     */
    public long getFailedShardClosed() {
        return failedShardClosed;
    }

    /**
     * The number of flushes that failed because the engine was closed without a failure, for example during relocation or shutdown
     * (a subset of {@link #getFailed()}).
     */
    public long getFailedEngineClosed() {
        return failedEngineClosed;
    }

    /**
     * The number of flushes that failed because the translog recovery was pending (a subset of {@link #getFailed()}).
     */
    public long getFailedPendingTranslogRecovery() {
        return failedPendingTranslogRecovery;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FLUSH);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.PERIODIC, periodic);
        builder.field(Fields.FAILED, failed);
        builder.field(Fields.FAILED_SHARD_CLOSED, failedShardClosed);
        builder.field(Fields.FAILED_ENGINE_CLOSED, failedEngineClosed);
        builder.field(Fields.FAILED_PENDING_TRANSLOG_RECOVERY, failedPendingTranslogRecovery);
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
        static final String FAILED = "failed";
        static final String FAILED_SHARD_CLOSED = "failed_shard_closed";
        static final String FAILED_ENGINE_CLOSED = "failed_engine_closed";
        static final String FAILED_PENDING_TRANSLOG_RECOVERY = "failed_pending_translog_recovery";
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
        out.writeVLong(totalTimeExcludingWaitingOnLockInMillis);
        if (out.getTransportVersion().supports(FLUSH_STATS_INCLUDE_FAILURES)) {
            out.writeVLong(failed);
            out.writeVLong(failedShardClosed);
            out.writeVLong(failedEngineClosed);
            out.writeVLong(failedPendingTranslogRecovery);
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
            && totalTimeExcludingWaitingOnLockInMillis == that.totalTimeExcludingWaitingOnLockInMillis
            && failed == that.failed
            && failedShardClosed == that.failedShardClosed
            && failedEngineClosed == that.failedEngineClosed
            && failedPendingTranslogRecovery == that.failedPendingTranslogRecovery;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            total,
            totalTimeInMillis,
            periodic,
            totalTimeExcludingWaitingOnLockInMillis,
            failed,
            failedShardClosed,
            failedEngineClosed,
            failedPendingTranslogRecovery
        );
    }
}
