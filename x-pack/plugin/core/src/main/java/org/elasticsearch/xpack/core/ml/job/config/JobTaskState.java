/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class JobTaskState implements PersistentTaskState {

    public static final String NAME = OpenJobAction.TASK_NAME;

    private static ParseField STATE = new ParseField("state");
    private static ParseField ALLOCATION_ID = new ParseField("allocation_id");

    private static final ConstructingObjectParser<JobTaskState, Void> PARSER =
            new ConstructingObjectParser<>(NAME,
                    args -> new JobTaskState((JobState) args[0], (Long) args[1]));

    static {
        PARSER.declareField(constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return JobState.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, STATE, ObjectParser.ValueType.STRING);
        PARSER.declareLong(constructorArg(), ALLOCATION_ID);
    }

    public static JobTaskState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final JobState state;
    private final long allocationId;

    public JobTaskState(JobState state, long allocationId) {
        this.state = Objects.requireNonNull(state);
        this.allocationId = allocationId;
    }

    public JobTaskState(StreamInput in) throws IOException {
        state = JobState.fromStream(in);
        allocationId = in.readLong();
    }

    public JobState getState() {
        return state;
    }

    public boolean isStatusStale(PersistentTask<?> task) {
        return allocationId != task.getAllocationId();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
        out.writeLong(allocationId);
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.value());
        builder.field(ALLOCATION_ID.getPreferredName(), allocationId);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobTaskState that = (JobTaskState) o;
        return state == that.state &&
                Objects.equals(allocationId, that.allocationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, allocationId);
    }
}
