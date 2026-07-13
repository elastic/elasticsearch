/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/*
 * This class represents information similar to that in ReindexDataStreamStatus, but enriched from other sources besides just the task
 * itself.
 */
public record ReindexDataStreamEnrichedStatus(
    long persistentTaskStartTime,
    int totalIndices,
    int totalIndicesToBeUpgraded,
    boolean complete,
    Exception exception,
    Map<String, Tuple<Long, Long>> inProgress,
    int pending,
    List<Tuple<String, Exception>> errors
) implements ToXContentObject, Writeable {
    public ReindexDataStreamEnrichedStatus {
        Objects.requireNonNull(inProgress);
        Objects.requireNonNull(errors);
    }

    public ReindexDataStreamEnrichedStatus(StreamInput in) throws IOException {
        this(
            in.readLong(),
            in.readInt(),
            in.readInt(),
            in.readBoolean(),
            in.readException(),
            in.readMap(StreamInput::readString, in2 -> Tuple.tuple(in2.readLong(), in2.readLong())),
            in.readInt(),
            in.readCollectionAsList(in1 -> Tuple.tuple(in1.readString(), in1.readException()))
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(persistentTaskStartTime);
        out.writeInt(totalIndices);
        out.writeInt(totalIndicesToBeUpgraded);
        out.writeBoolean(complete);
        out.writeException(exception);
        out.writeMap(inProgress, StreamOutput::writeString, (out2, tuple) -> {
            out2.writeLong(tuple.v1());
            out2.writeLong(tuple.v2());
        });
        out.writeInt(pending);
        out.writeCollection(errors, (out1, tuple) -> {
            out1.writeString(tuple.v1());
            out1.writeException(tuple.v2());
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.timestampFieldsFromUnixEpochMillis("start_time_millis", "start_time", persistentTaskStartTime);
        builder.field("complete", complete);
        builder.field("total_indices_in_data_stream", totalIndices);
        builder.field("total_indices_requiring_upgrade", totalIndicesToBeUpgraded);
        builder.field("successes", totalIndicesToBeUpgraded - (inProgress.size() + pending + errors.size()));
        builder.startArray("in_progress");
        for (Map.Entry<String, Tuple<Long, Long>> inProgressEntry : inProgress.entrySet()) {
            builder.startObject();
            builder.field("index", inProgressEntry.getKey());
            builder.field("total_doc_count", inProgressEntry.getValue().v1());
            builder.field("reindexed_doc_count", inProgressEntry.getValue().v2());
            builder.endObject();
        }
        builder.endArray();
        builder.field("pending", pending);
        builder.startArray("errors");
        for (Tuple<String, Exception> error : errors) {
            builder.startObject();
            builder.field("index", error.v1());
            builder.field("message", error.v2() == null ? "unknown" : error.v2().getMessage());
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
