/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ReindexDataStreamStatus(
    long persistentTaskStartTime,
    boolean complete,
    Exception exception,
    List<String> successes,
    List<String> inProgress,
    List<String> pending,
    Map<String, Exception> errors
) implements Task.Status {
    public ReindexDataStreamStatus {
        Objects.requireNonNull(successes);
        Objects.requireNonNull(inProgress);
        Objects.requireNonNull(pending);
        Objects.requireNonNull(errors);
    }

    public static final String NAME = "ReindexDataStreamStatus";

    public ReindexDataStreamStatus(StreamInput in) throws IOException {
        this(
            in.readLong(),
            in.readBoolean(),
            in.readException(),
            in.readCollectionAsList(StreamInput::readString),
            in.readCollectionAsList(StreamInput::readString),
            in.readCollectionAsList(StreamInput::readString),
            in.readMap(StreamInput::readException)
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(persistentTaskStartTime);
        out.writeBoolean(complete);
        out.writeException(exception);
        out.writeStringCollection(successes);
        out.writeStringCollection(inProgress);
        out.writeStringCollection(pending);
        out.writeMap(errors, StreamOutput::writeException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("start_time", persistentTaskStartTime);
        builder.field("complete", complete);
        addIndexList(builder, "successes", successes);
        addIndexList(builder, "in_progress", inProgress);
        addIndexList(builder, "pending", pending);
        builder.startObject("errors");
        builder.field("count", errors.size());
        builder.startObject("indices");
        for (Map.Entry<String, Exception> error : errors.entrySet()) {
            builder.field(error.getKey(), error.getValue().getMessage());
        }
        builder.endObject();
        builder.endObject();
        if (exception != null) {
            builder.field("exception", exception.getMessage());
        }
        builder.endObject();
        return builder;
    }

    private void addIndexList(XContentBuilder builder, String name, List<String> list) throws IOException {
        builder.startObject(name);
        builder.field("count", list.size());
        builder.field("indices", list);
        builder.endObject();
    }
}
