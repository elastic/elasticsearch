/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.ml.MlTasks;

import java.io.IOException;
import java.util.Objects;

public class DataFrameAnalyticsTaskState implements PersistentTaskState {

    private static ParseField STATE = new ParseField("state");
    private static ParseField ALLOCATION_ID = new ParseField("allocation_id");

    private final DataFrameAnalyticsState state;
    private final long allocationId;

    private static final ConstructingObjectParser<DataFrameAnalyticsTaskState, Void> PARSER =
            new ConstructingObjectParser<>(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, true,
                a -> new DataFrameAnalyticsTaskState((DataFrameAnalyticsState) a[0], (long) a[1]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
           if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
               return DataFrameAnalyticsState.fromString(p.text());
           }
           throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, STATE, ObjectParser.ValueType.STRING);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), ALLOCATION_ID);
    }

    public static DataFrameAnalyticsTaskState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DataFrameAnalyticsTaskState(DataFrameAnalyticsState state, long allocationId) {
        this.state = Objects.requireNonNull(state);
        this.allocationId = allocationId;
    }

    public DataFrameAnalyticsState getState() {
        return state;
    }

    public boolean isStatusStale(PersistentTasksCustomMetaData.PersistentTask<?> task) {
        return allocationId != task.getAllocationId();
    }

    @Override
    public String getWriteableName() {
        return MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
        out.writeLong(allocationId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.toString());
        builder.field(ALLOCATION_ID.getPreferredName(), allocationId);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFrameAnalyticsTaskState that = (DataFrameAnalyticsTaskState) o;
        return allocationId == that.allocationId &&
            state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, allocationId);
    }
}
