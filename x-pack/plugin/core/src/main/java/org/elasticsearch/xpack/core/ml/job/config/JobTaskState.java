/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.core.ml.MlTasks;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class JobTaskState implements PersistentTaskState {

    public static final String NAME = MlTasks.JOB_TASK_NAME;

    private static ParseField STATE = new ParseField("state");
    private static ParseField ALLOCATION_ID = new ParseField("allocation_id");
    private static ParseField REASON = new ParseField("reason");

    private static final ConstructingObjectParser<JobTaskState, Void> PARSER =
            new ConstructingObjectParser<>(NAME, true,
                    args -> new JobTaskState((JobState) args[0], (Long) args[1], (String) args[2]));

    static {
        PARSER.declareField(constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return JobState.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, STATE, ObjectParser.ValueType.STRING);
        PARSER.declareLong(constructorArg(), ALLOCATION_ID);
        PARSER.declareString(optionalConstructorArg(), REASON);
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

    public JobTaskState(JobState state, long allocationId, @Nullable String reason) {
        this.state = Objects.requireNonNull(state);
        this.allocationId = allocationId;
        this.reason = reason;
    }

    public JobTaskState(StreamInput in) throws IOException {
        state = JobState.fromStream(in);
        allocationId = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            reason = in.readOptionalString();
        } else {
            reason = null;
        }
    }

    public JobState getState() {
        return state;
    }

    @Nullable
    public String getReason() {
        return reason;
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
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeOptionalString(reason);
        }
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
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobTaskState that = (JobTaskState) o;
        return state == that.state &&
                Objects.equals(allocationId, that.allocationId) &&
                Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, allocationId, reason);
    }
}
