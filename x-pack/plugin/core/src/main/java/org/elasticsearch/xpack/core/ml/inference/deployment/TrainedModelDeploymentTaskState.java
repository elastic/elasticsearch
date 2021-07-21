/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.deployment;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;

import java.io.IOException;
import java.util.Objects;

public class TrainedModelDeploymentTaskState implements PersistentTaskState {

    public static final String NAME = MlTasks.TRAINED_MODEL_DEPLOYMENT_TASK_NAME;

    private static ParseField STATE = new ParseField("state");
    private static ParseField ALLOCATION_ID = new ParseField("allocation_id");
    private static ParseField REASON = new ParseField("reason");

    private final TrainedModelDeploymentState state;
    private final long allocationId;
    private final String reason;

    private static final ConstructingObjectParser<TrainedModelDeploymentTaskState, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true,
            a -> new TrainedModelDeploymentTaskState((TrainedModelDeploymentState) a[0], (long) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameAnalyticsState::fromString, STATE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), ALLOCATION_ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
    }

    public static TrainedModelDeploymentTaskState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TrainedModelDeploymentTaskState(TrainedModelDeploymentState state, long allocationId, @Nullable String reason) {
        this.state = Objects.requireNonNull(state);
        this.allocationId = allocationId;
        this.reason = reason;
    }

    public TrainedModelDeploymentTaskState(StreamInput in) throws IOException {
        this.state = TrainedModelDeploymentState.fromStream(in);
        this.allocationId = in.readLong();
        this.reason = in.readOptionalString();
    }

    public TrainedModelDeploymentState getState() {
        return state;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.toString());
        builder.field(ALLOCATION_ID.getPreferredName(), allocationId);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
        out.writeLong(allocationId);
        out.writeOptionalString(reason);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelDeploymentTaskState that = (TrainedModelDeploymentTaskState) o;
        return allocationId == that.allocationId &&
            state == that.state &&
            Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, allocationId, reason);
    }

    public boolean isStatusStale(PersistentTasksCustomMetadata.PersistentTask<?> task) {
        return allocationId != task.getAllocationId();
    }
}
