/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record ReindexDataStreamPersistentTaskState(long completionTime) implements Task.Status, PersistentTaskState {
    public static final String NAME = ReindexDataStreamTask.TASK_NAME;
    private static final String COMPLETION_TIME_FIELD = "completion_time";
    private static final ConstructingObjectParser<ReindexDataStreamPersistentTaskState, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new ReindexDataStreamPersistentTaskState((long) args[0])
    );
    static {
        PARSER.declareLong(constructorArg(), new ParseField(COMPLETION_TIME_FIELD));
    }

    public ReindexDataStreamPersistentTaskState(StreamInput in) throws IOException {
        this(in.readLong());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(completionTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(COMPLETION_TIME_FIELD, completionTime);
        builder.endObject();
        return builder;
    }

    public static ReindexDataStreamPersistentTaskState fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

}
