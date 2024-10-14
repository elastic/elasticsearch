/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class JobTaskState implements PersistentTaskState, MlTaskState {

    public static final String NAME = MlTasks.JOB_TASK_NAME;

    private static final ParseField STATE = new ParseField("state");
    private static final ParseField ALLOCATION_ID = new ParseField("allocation_id");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField LAST_STATE_CHANGE_TIME = new ParseField("last_state_change_time");

    private static final ConstructingObjectParser<JobTaskState, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new JobTaskState((JobState) args[0], (Long) args[1], (String) args[2], (Instant) args[3])
    );

    static {
        PARSER.declareString(constructorArg(), JobState::fromString, STATE);
        PARSER.declareLong(constructorArg(), ALLOCATION_ID);
        PARSER.declareString(optionalConstructorArg(), REASON);
        PARSER.declareField(
            optionalConstructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, LAST_STATE_CHANGE_TIME.getPreferredName()),
            LAST_STATE_CHANGE_TIME,
            ObjectParser.ValueType.VALUE
        );
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
    private final String reason;
    private final Instant lastStateChangeTime;

    public JobTaskState(JobState state, long allocationId, @Nullable String reason, @Nullable Instant lastStateChangeTime) {
        this.state = Objects.requireNonNull(state);
        this.allocationId = allocationId;
        this.reason = reason;
        // Round to millisecond to avoid serialization round trip differences
        this.lastStateChangeTime = (lastStateChangeTime != null) ? Instant.ofEpochMilli(lastStateChangeTime.toEpochMilli()) : null;
    }

    public JobTaskState(StreamInput in) throws IOException {
        state = JobState.fromStream(in);
        allocationId = in.readLong();
        reason = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            lastStateChangeTime = in.readOptionalInstant();
        } else {
            lastStateChangeTime = null;
        }
    }

    public JobState getState() {
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
        return JobState.FAILED.equals(state);
    }

    /**
     * The job state stores the allocation ID at the time it was last set.
     * This method compares the allocation ID in the state with the allocation
     * ID in the task.  If the two are different then the task has been relocated
     * to a different node after the last time the state was set.  This in turn
     * means that the state is not necessarily correct.  For example, a job that
     * has a state of OPENED but is stale must be considered to be OPENING, because
     * it won't yet have a corresponding autodetect process.
     * @param task The job task to check.
     * @return Has the task been relocated to another node and not had its status set since then?
     */
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
        out.writeOptionalString(reason);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalInstant(lastStateChangeTime);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.value());
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
        JobTaskState that = (JobTaskState) o;
        return state == that.state
            && Objects.equals(allocationId, that.allocationId)
            && Objects.equals(reason, that.reason)
            && Objects.equals(lastStateChangeTime, that.lastStateChangeTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, allocationId, reason, lastStateChangeTime);
    }
}
