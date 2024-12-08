/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.execution;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class QueuedWatch implements Writeable, ToXContentObject {

    private String watchId;
    private String watchRecordId;
    private ZonedDateTime triggeredTime;
    private ZonedDateTime executionTime;

    public QueuedWatch(WatchExecutionContext ctx) {
        this.watchId = ctx.id().watchId();
        this.watchRecordId = ctx.id().value();
        this.triggeredTime = ctx.triggerEvent().triggeredTime();
        this.executionTime = ctx.executionTime();
    }

    public QueuedWatch(StreamInput in) throws IOException {
        watchId = in.readString();
        watchRecordId = in.readString();
        triggeredTime = Instant.ofEpochMilli(in.readVLong()).atZone(ZoneOffset.UTC);
        executionTime = Instant.ofEpochMilli(in.readVLong()).atZone(ZoneOffset.UTC);
    }

    public String watchId() {
        return watchId;
    }

    public ZonedDateTime triggeredTime() {
        return triggeredTime;
    }

    public void triggeredTime(ZonedDateTime triggeredTime) {
        this.triggeredTime = triggeredTime;
    }

    public ZonedDateTime executionTime() {
        return executionTime;
    }

    public void executionTime(ZonedDateTime executionTime) {
        this.executionTime = executionTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(watchId);
        out.writeString(watchRecordId);
        out.writeVLong(triggeredTime.toInstant().toEpochMilli());
        out.writeVLong(executionTime.toInstant().toEpochMilli());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("watch_id", watchId);
        builder.field("watch_record_id", watchRecordId);
        builder.timestampField("triggered_time", triggeredTime);
        builder.timestampField("execution_time", executionTime);
        builder.endObject();
        return builder;
    }

}
