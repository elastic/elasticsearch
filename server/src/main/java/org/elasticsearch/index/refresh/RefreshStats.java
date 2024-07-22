/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.refresh;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class RefreshStats implements Writeable, ToXContentFragment {

    private long total;

    private long totalTimeInMillis;

    private long externalTotal;

    private long externalTotalTimeInMillis;

    /**
     * Number of waiting refresh listeners.
     */
    private int listeners;

    private long lastRefreshTime;

    private long lastExternalRefreshTime;

    private boolean hasUnwrittenChanges;

    public RefreshStats() {}

    public RefreshStats(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_2_0)) {
            externalTotal = in.readVLong();
            externalTotalTimeInMillis = in.readVLong();
        }
        listeners = in.readVInt();
        if (in.getTransportVersion().onOrAfter(TransportVersions.LAST_REFRESH_TIME_STATS)) {
            lastRefreshTime = in.readVLong();
            lastExternalRefreshTime = in.readVLong();
            hasUnwrittenChanges = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_2_0)) {
            out.writeVLong(externalTotal);
            out.writeVLong(externalTotalTimeInMillis);
        }
        out.writeVInt(listeners);
        if (out.getTransportVersion().onOrAfter(TransportVersions.LAST_REFRESH_TIME_STATS)) {
            out.writeVLong(lastRefreshTime);
            out.writeVLong(lastExternalRefreshTime);
            out.writeBoolean(hasUnwrittenChanges);
        }
    }

    public RefreshStats(
        long total,
        long totalTimeInMillis,
        long externalTotal,
        long externalTotalTimeInMillis,
        int listeners,
        long lastRefreshTime,
        long lastExternalRefreshTime,
        boolean hasUnwrittenChanges
    ) {
        this.total = total;
        this.totalTimeInMillis = totalTimeInMillis;
        this.externalTotal = externalTotal;
        this.externalTotalTimeInMillis = externalTotalTimeInMillis;
        this.listeners = listeners;
        this.lastRefreshTime = lastRefreshTime;
        this.lastExternalRefreshTime = lastExternalRefreshTime;
        this.hasUnwrittenChanges = hasUnwrittenChanges;
    }

    public void add(RefreshStats refreshStats) {
        addTotals(refreshStats);
    }

    public void addTotals(RefreshStats refreshStats) {
        if (refreshStats == null) {
            return;
        }
        this.total += refreshStats.total;
        this.totalTimeInMillis += refreshStats.totalTimeInMillis;
        this.externalTotal += refreshStats.externalTotal;
        this.externalTotalTimeInMillis += refreshStats.externalTotalTimeInMillis;
        this.listeners += refreshStats.listeners;
        this.lastRefreshTime = Math.max(this.lastRefreshTime, refreshStats.lastRefreshTime);
        this.lastExternalRefreshTime = Math.max(this.lastExternalRefreshTime, refreshStats.lastExternalRefreshTime);
        this.hasUnwrittenChanges |= refreshStats.hasUnwrittenChanges;
    }

    /**
     * The total number of refresh executed.
     */
    public long getTotal() {
        return this.total;
    }

    /*
     * The total number of external refresh executed.
     */
    public long getExternalTotal() {
        return this.externalTotal;
    }

    /**
     * The total time spent executing refreshes (in milliseconds).
     */
    public long getTotalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time spent executing external refreshes (in milliseconds).
     */
    public long getExternalTotalTimeInMillis() {
        return this.externalTotalTimeInMillis;
    }

    /**
     * The total time refreshes have been executed.
     */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    /**
     * The total time external refreshes have been executed.
     */
    public TimeValue getExternalTotalTime() {
        return new TimeValue(externalTotalTimeInMillis);
    }

    /**
     * The number of waiting refresh listeners.
     */
    public int getListeners() {
        return listeners;
    }

    /**
     * Timestamp of the last refresh.
     */
    public long getLastRefreshTime() {
        return lastRefreshTime;
    }

    /**
     * Timestamp of the last external refresh.
     */
    public long getLastExternalRefreshTime() {
        return lastExternalRefreshTime;
    }

    /**
     * Whether there are changes that need to be written to disk or not.
     */
    public boolean getHasUnwrittenChanges() {
        return hasUnwrittenChanges;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("refresh");
        builder.field("total", total);
        builder.humanReadableField("total_time_in_millis", "total_time", getTotalTime());
        builder.field("external_total", externalTotal);
        builder.humanReadableField("external_total_time_in_millis", "external_total_time", getExternalTotalTime());
        builder.field("listeners", listeners);
        builder.field("last_refresh_timestamp", lastRefreshTime);
        builder.field("last_external_refresh_timestamp", lastExternalRefreshTime);
        builder.field("has_unwritten_changes", hasUnwrittenChanges);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != RefreshStats.class) {
            return false;
        }
        RefreshStats rhs = (RefreshStats) obj;
        return total == rhs.total
            && totalTimeInMillis == rhs.totalTimeInMillis
            && externalTotal == rhs.externalTotal
            && externalTotalTimeInMillis == rhs.externalTotalTimeInMillis
            && listeners == rhs.listeners
            && lastRefreshTime == rhs.lastRefreshTime
            && lastExternalRefreshTime == rhs.lastExternalRefreshTime
            && hasUnwrittenChanges == rhs.hasUnwrittenChanges;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            total,
            totalTimeInMillis,
            externalTotal,
            externalTotalTimeInMillis,
            listeners,
            lastRefreshTime,
            lastExternalRefreshTime,
            hasUnwrittenChanges
        );
    }
}
