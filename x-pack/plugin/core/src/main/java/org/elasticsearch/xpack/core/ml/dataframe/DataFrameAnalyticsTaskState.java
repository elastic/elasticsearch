/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.utils.MlTaskState;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameAnalyticsTaskState implements PersistentTaskState, MlTaskState {

    public static final String NAME = MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME;

    private static final ParseField STATE = new ParseField("state");
    private static final ParseField ALLOCATION_ID = new ParseField("allocation_id");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField LAST_STATE_CHANGE_TIME = new ParseField("last_state_change_time");

    private final DataFrameAnalyticsState state;
    private final long allocationId;
    private final String reason;
    private final Instant lastStateChangeTime;

    private static final ConstructingObjectParser<DataFrameAnalyticsTaskState, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new DataFrameAnalyticsTaskState((DataFrameAnalyticsState) a[0], (long) a[1], (String) a[2], (Instant) a[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameAnalyticsState::fromString, STATE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), ALLOCATION_ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
        PARSER.declareField(
            optionalConstructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, LAST_STATE_CHANGE_TIME.getPreferredName()),
            LAST_STATE_CHANGE_TIME,
            ObjectParser.ValueType.VALUE
        );
    }

    public static DataFrameAnalyticsTaskState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DataFrameAnalyticsTaskState(
        DataFrameAnalyticsState state,
        long allocationId,
        @Nullable String reason,
        @Nullable Instant lastStateChangeTime
    ) {
        this.state = Objects.requireNonNull(state);
        this.allocationId = allocationId;
        this.reason = reason;
        // Round to millisecond to avoid serialization round trip differences
        this.lastStateChangeTime = (lastStateChangeTime != null) ? Instant.ofEpochMilli(lastStateChangeTime.toEpochMilli()) : null;
    }

    public DataFrameAnalyticsTaskState(StreamInput in) throws IOException {
        this.state = DataFrameAnalyticsState.fromStream(in);
        this.allocationId = in.readLong();
        this.reason = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            lastStateChangeTime = in.readOptionalInstant();
        } else {
            lastStateChangeTime = null;
        }
    }

    public DataFrameAnalyticsState getState() {
        return state;
    }

    public long getAllocationId() {
        return allocationId;
    }

    @Nullable
    public String getReason() {
        return reason;
    }

    @Override
    @Nullable
    public Instant getLastStateChangeTime() {
        return lastStateChangeTime;
    }

    @Override
    public boolean isFailed() {
        return DataFrameAnalyticsState.FAILED.equals(state);
    }

    public boolean isStatusStale(PersistentTasksCustomMetadata.PersistentTask<?> task) {
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
        out.writeOptionalString(reason);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalInstant(lastStateChangeTime);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.toString());
        builder.field(ALLOCATION_ID.getPreferredName(), allocationId);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        if (lastStateChangeTime != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                LAST_STATE_CHANGE_TIME.getPreferredName(),
                LAST_STATE_CHANGE_TIME.getPreferredName() + "_string",
                lastStateChangeTime.toEpochMilli()
            );
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFrameAnalyticsTaskState that = (DataFrameAnalyticsTaskState) o;
        return allocationId == that.allocationId
            && state == that.state
            && Objects.equals(reason, that.reason)
            && Objects.equals(lastStateChangeTime, that.lastStateChangeTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, allocationId, reason, lastStateChangeTime);
    }
}
