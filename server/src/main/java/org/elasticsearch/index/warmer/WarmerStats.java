/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.warmer;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class WarmerStats implements Writeable, ToXContentFragment {

    private long current;

    private long total;

    private long totalTimeInMillis;

    public WarmerStats() {

    }

    public WarmerStats(StreamInput in) throws IOException {
        current = in.readVLong();
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
    }

    public WarmerStats(long current, long total, long totalTimeInMillis) {
        this.current = current;
        this.total = total;
        this.totalTimeInMillis = totalTimeInMillis;
    }

    public void add(long current, long total, long totalTimeInMillis) {
        this.current += current;
        this.total += total;
        this.totalTimeInMillis += totalTimeInMillis;
    }

    public void add(WarmerStats warmerStats) {
        if (warmerStats == null) {
            return;
        }
        this.current += warmerStats.current;
        this.total += warmerStats.total;
        this.totalTimeInMillis += warmerStats.totalTimeInMillis;
    }

    public long current() {
        return this.current;
    }

    /**
     * The total number of warmer executed.
     */
    public long total() {
        return this.total;
    }

    /**
     * The total time warmer have been executed (in milliseconds).
     */
    public long totalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time warmer have been executed.
     */
    public TimeValue totalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.WARMER);
        builder.field(Fields.CURRENT, current);
        builder.field(Fields.TOTAL, total);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, totalTime());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String WARMER = "warmer";
        static final String CURRENT = "current";
        static final String TOTAL = "total";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(current);
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WarmerStats that = (WarmerStats) o;
        return current == that.current && total == that.total && totalTimeInMillis == that.totalTimeInMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, total, totalTimeInMillis);
    }
}
