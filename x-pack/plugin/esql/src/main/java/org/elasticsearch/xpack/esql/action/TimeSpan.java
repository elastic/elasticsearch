/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * THis class is used to capture a duration of some process, including start and stop point int time.
 */
public record TimeSpan(long startMillis, long startNanos, long stopMillis, long stopNanos) implements Writeable, ToXContentObject {

    public static TimeSpan readFrom(StreamInput in) throws IOException {
        return new TimeSpan(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(startMillis);
        out.writeVLong(startNanos);
        out.writeVLong(stopMillis);
        out.writeVLong(stopNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.timestampFieldsFromUnixEpochMillis("start_millis", "start", startMillis);
        builder.timestampFieldsFromUnixEpochMillis("stop_millis", "stop", stopMillis);
        if (builder.humanReadable()) {
            builder.field("took_time", toTimeValue());
        }
        builder.field("took_millis", durationInMillis());
        builder.field("took_nanos", durationInNanos());
        builder.endObject();
        return builder;
    }

    public TimeValue toTimeValue() {
        return timeValueNanos(stopNanos - startNanos);
    }

    public long durationInMillis() {
        return stopMillis - startMillis;
    }

    public long durationInNanos() {
        return stopNanos - startNanos;
    }

    public static Builder start() {
        return new Builder();
    }

    public static class Builder {

        private final long startMillis = System.currentTimeMillis();
        private final long startNanos = System.nanoTime();

        public TimeSpan stop() {
            return new TimeSpan(startMillis, startNanos, System.currentTimeMillis(), System.nanoTime());
        }
    }
}
