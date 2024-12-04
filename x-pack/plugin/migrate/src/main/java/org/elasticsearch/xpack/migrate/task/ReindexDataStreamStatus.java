/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record ReindexDataStreamStatus(
    long persistentTaskStartTime,
    int totalIndices,
    int totalIndicesToBeUpgraded,
    boolean complete,
    Exception exception,
    int inProgress,
    int pending,
    List<Tuple<String, Exception>> errors
) implements Task.Status {
    public ReindexDataStreamStatus {
        Objects.requireNonNull(errors);
    }

    public static final String NAME = "ReindexDataStreamStatus";

    public ReindexDataStreamStatus(StreamInput in) throws IOException {
        this(
            in.readLong(),
            in.readInt(),
            in.readInt(),
            in.readBoolean(),
            in.readException(),
            in.readInt(),
            in.readInt(),
            in.readCollectionAsList(in1 -> Tuple.tuple(in1.readString(), in1.readException()))
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(persistentTaskStartTime);
        out.writeInt(totalIndices);
        out.writeInt(totalIndicesToBeUpgraded);
        out.writeBoolean(complete);
        out.writeException(exception);
        out.writeInt(inProgress);
        out.writeInt(pending);
        out.writeCollection(errors, (out1, tuple) -> {
            out1.writeString(tuple.v1());
            out1.writeException(tuple.v2());
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("start_time", persistentTaskStartTime);
        builder.field("complete", complete);
        builder.field("total_indices", totalIndices);
        builder.field("total_indices_requiring_upgrade", totalIndicesToBeUpgraded);
        builder.field("successes", totalIndicesToBeUpgraded - (inProgress + pending + errors.size()));
        builder.field("in_progress", inProgress);
        builder.field("pending", pending);
        builder.startArray("errors");
        for (Tuple<String, Exception> error : errors) {
            builder.startObject();
            builder.field("index", error.v1());
            builder.field("message", error.v2().getMessage());
            builder.endObject();
        }
        builder.endArray();
        if (exception != null) {
            builder.field("exception", exception.getMessage());
        }
        builder.endObject();
        return builder;
    }
}
