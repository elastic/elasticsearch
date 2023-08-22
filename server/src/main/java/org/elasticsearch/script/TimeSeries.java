/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A response class representing a snapshot of a {@link org.elasticsearch.script.TimeSeriesCounter} at a point in time.
 */
public class TimeSeries implements Writeable, ToXContentFragment {
    public final long fiveMinutes;
    public final long fifteenMinutes;
    public final long twentyFourHours;
    public final long total;

    public TimeSeries(long total) {
        this.fiveMinutes = 0;
        this.fifteenMinutes = 0;
        this.twentyFourHours = 0;
        this.total = total;
    }

    public TimeSeries(long fiveMinutes, long fifteenMinutes, long twentyFourHours, long total) {
        this.fiveMinutes = fiveMinutes;
        this.fifteenMinutes = fifteenMinutes;
        this.twentyFourHours = twentyFourHours;
        this.total = total;
    }

    TimeSeries withTotal(long total) {
        return new TimeSeries(fiveMinutes, fifteenMinutes, twentyFourHours, total);
    }

    public static TimeSeries merge(TimeSeries first, TimeSeries second) {
        return new TimeSeries(
            first.fiveMinutes + second.fiveMinutes,
            first.fifteenMinutes + second.fifteenMinutes,
            first.twentyFourHours + second.twentyFourHours,
            first.total + second.total
        );
    }

    public TimeSeries(StreamInput in) throws IOException {
        fiveMinutes = in.readVLong();
        fifteenMinutes = in.readVLong();
        twentyFourHours = in.readVLong();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
            total = in.readVLong();
        } else {
            total = 0;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // total is omitted from toXContent as it's written at a higher level by ScriptContextStats
        builder.field(ScriptContextStats.Fields.FIVE_MINUTES, fiveMinutes);
        builder.field(ScriptContextStats.Fields.FIFTEEN_MINUTES, fifteenMinutes);
        builder.field(ScriptContextStats.Fields.TWENTY_FOUR_HOURS, twentyFourHours);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fiveMinutes);
        out.writeVLong(fifteenMinutes);
        out.writeVLong(twentyFourHours);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
            out.writeVLong(total);
        }
    }

    public boolean areTimingsEmpty() {
        return fiveMinutes == 0 && fifteenMinutes == 0 && twentyFourHours == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeries that = (TimeSeries) o;
        return fiveMinutes == that.fiveMinutes
            && fifteenMinutes == that.fifteenMinutes
            && twentyFourHours == that.twentyFourHours
            && total == that.total;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fiveMinutes, fifteenMinutes, twentyFourHours, total);
    }

    @Override
    public String toString() {
        return "TimeSeries{"
            + "fiveMinutes="
            + fiveMinutes
            + ", fifteenMinutes="
            + fifteenMinutes
            + ", twentyFourHours="
            + twentyFourHours
            + ", total="
            + total
            + '}';
    }
}
