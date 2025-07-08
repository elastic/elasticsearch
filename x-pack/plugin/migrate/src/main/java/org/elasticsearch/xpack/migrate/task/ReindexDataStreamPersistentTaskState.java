/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record ReindexDataStreamPersistentTaskState(
    @Nullable Integer totalIndices,
    @Nullable Integer totalIndicesToBeUpgraded,
    @Nullable Long completionTime
) implements Task.Status, PersistentTaskState {

    public static final String NAME = ReindexDataStreamTask.TASK_NAME;
    private static final String TOTAL_INDICES_FIELD = "total_indices_in_data_stream";
    private static final String TOTAL_INDICES_REQUIRING_UPGRADE_FIELD = "total_indices_requiring_upgrade";
    private static final String COMPLETION_TIME_FIELD = "completion_time";
    private static final ConstructingObjectParser<ReindexDataStreamPersistentTaskState, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new ReindexDataStreamPersistentTaskState((Integer) args[0], (Integer) args[1], (Long) args[2])
    );
    static {
        PARSER.declareInt(optionalConstructorArg(), new ParseField(TOTAL_INDICES_FIELD));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(TOTAL_INDICES_REQUIRING_UPGRADE_FIELD));
        PARSER.declareLong(optionalConstructorArg(), new ParseField(COMPLETION_TIME_FIELD));
    }

    public ReindexDataStreamPersistentTaskState(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalInt(), in.readOptionalLong());
    }

    public boolean isComplete() {
        return completionTime != null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(totalIndices);
        out.writeOptionalInt(totalIndicesToBeUpgraded);
        out.writeOptionalLong(completionTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (totalIndices != null) {
            builder.field(TOTAL_INDICES_FIELD, totalIndices);
        }
        if (totalIndicesToBeUpgraded != null) {
            builder.field(TOTAL_INDICES_REQUIRING_UPGRADE_FIELD, totalIndicesToBeUpgraded);
        }
        if (completionTime != null) {
            builder.field(COMPLETION_TIME_FIELD, completionTime);
        }
        builder.endObject();
        return builder;
    }

    public static ReindexDataStreamPersistentTaskState fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

}
