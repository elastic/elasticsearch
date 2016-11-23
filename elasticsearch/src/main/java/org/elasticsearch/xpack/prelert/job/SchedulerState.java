/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import org.elasticsearch.common.xcontent.ObjectParser.ValueType;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class SchedulerState extends ToXContentToBytes implements Writeable {

    // NORELEASE: no camel casing:
    public static final ParseField TYPE_FIELD = new ParseField("schedulerState");
    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField START_TIME_MILLIS = new ParseField("start");
    public static final ParseField END_TIME_MILLIS = new ParseField("end");

    public static final ConstructingObjectParser<SchedulerState, ParseFieldMatcherSupplier> PARSER = new ConstructingObjectParser<>(
            TYPE_FIELD.getPreferredName(), a -> new SchedulerState((JobSchedulerStatus) a[0], (long) a[1], (Long) a[2]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> JobSchedulerStatus.fromString(p.text()), STATUS,
                ValueType.STRING);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), START_TIME_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), END_TIME_MILLIS);
    }

    private JobSchedulerStatus status;
    @Nullable
    private Long startTimeMillis;
    @Nullable
    private Long endTimeMillis;

    public SchedulerState(JobSchedulerStatus status, Long startTimeMillis, Long endTimeMillis) {
        this.status = status;
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
    }

    public SchedulerState(StreamInput in) throws IOException {
        status = JobSchedulerStatus.fromStream(in);
        startTimeMillis = in.readOptionalLong();
        endTimeMillis = in.readOptionalLong();
    }

    public JobSchedulerStatus getStatus() {
        return status;
    }

    public Long getStartTimeMillis() {
        return startTimeMillis;
    }

    /**
     * The end time as epoch milliseconds. An {@code null} end time indicates
     * real-time mode.
     *
     * @return The optional end time as epoch milliseconds.
     */
    @Nullable
    public Long getEndTimeMillis() {
        return endTimeMillis;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof SchedulerState == false) {
            return false;
        }

        SchedulerState that = (SchedulerState) other;

        return Objects.equals(this.status, that.status) && Objects.equals(this.startTimeMillis, that.startTimeMillis)
                && Objects.equals(this.endTimeMillis, that.endTimeMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, startTimeMillis, endTimeMillis);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        status.writeTo(out);
        out.writeOptionalLong(startTimeMillis);
        out.writeOptionalLong(endTimeMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATUS.getPreferredName(), status.name().toUpperCase(Locale.ROOT));
        if (startTimeMillis != null) {
            builder.field(START_TIME_MILLIS.getPreferredName(), startTimeMillis);
        }
        if (endTimeMillis != null) {
            builder.field(END_TIME_MILLIS.getPreferredName(), endTimeMillis);
        }
        builder.endObject();
        return builder;
    }
}
