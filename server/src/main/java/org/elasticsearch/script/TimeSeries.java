/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TimeSeries implements Writeable, ToXContentFragment {
    public final long fiveMinutes;
    public final long fifteenMinutes;
    public final long twentyFourHours;

    public TimeSeries() {
        this.fiveMinutes = 0;
        this.fifteenMinutes = 0;
        this.twentyFourHours = 0;
    }

    public TimeSeries(long fiveMinutes, long fifteenMinutes, long twentyFourHours) {
        this.fiveMinutes = fiveMinutes;
        this.fifteenMinutes = fifteenMinutes;
        this.twentyFourHours = twentyFourHours;
    }

    public TimeSeries(StreamInput in) throws IOException {
        fiveMinutes = in.readVLong();
        fifteenMinutes = in.readVLong();
        twentyFourHours = in.readVLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
    }

    public boolean isEmpty() {
        return twentyFourHours == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeries that = (TimeSeries) o;
        return fiveMinutes == that.fiveMinutes && fifteenMinutes == that.fifteenMinutes && twentyFourHours == that.twentyFourHours;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fiveMinutes, fifteenMinutes, twentyFourHours);
    }
}
