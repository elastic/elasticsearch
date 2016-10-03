/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public class QueuedWatch implements Streamable, ToXContent {

    private String watchId;
    private String watchRecordId;
    private DateTime triggeredTime;
    private DateTime executionTime;

    public QueuedWatch(WatchExecutionContext ctx) {
        this.watchId = ctx.watch().id();
        this.watchRecordId = ctx.id().value();
        this.triggeredTime = ctx.triggerEvent().triggeredTime();
        this.executionTime = ctx.executionTime();
    }

    public QueuedWatch(StreamInput in) throws IOException {
        readFrom(in);
    }

    public String watchId() {
        return watchId;
    }

    public void WatchId(String watchId) {
        this.watchId = watchId;
    }

    public String watchRecordId() {
        return watchRecordId;
    }

    public void watchRecordId(String watchRecordId) {
        this.watchRecordId = watchRecordId;
    }

    public DateTime triggeredTime() {
        return triggeredTime;
    }

    public void triggeredTime(DateTime triggeredTime) {
        this.triggeredTime = triggeredTime;
    }

    public DateTime executionTime() {
        return executionTime;
    }

    public void executionTime(DateTime executionTime) {
        this.executionTime = executionTime;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        watchId = in.readString();
        watchRecordId = in.readString();
        triggeredTime = new DateTime(in.readVLong(), DateTimeZone.UTC);
        executionTime = new DateTime(in.readVLong(), DateTimeZone.UTC);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(watchId);
        out.writeString(watchRecordId);
        out.writeVLong(triggeredTime.getMillis());
        out.writeVLong(executionTime.getMillis());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("watch_id", watchId);
        builder.field("watch_record_id", watchRecordId);
        builder.field("triggered_time", triggeredTime);
        builder.field("execution_time", executionTime);
        builder.endObject();
        return builder;
    }

}
