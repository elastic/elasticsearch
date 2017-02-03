/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;

import java.io.IOException;
import java.util.Objects;

public class Allocation extends AbstractDiffable<Allocation> implements ToXContent {

    private static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    private static final ParseField JOB_ID_FIELD = new ParseField("job_id");
    private static final ParseField IGNORE_DOWNTIME_FIELD = new ParseField("ignore_downtime");
    public static final ParseField STATE = new ParseField("state");
    public static final ParseField STATE_REASON = new ParseField("state_reason");

    static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("allocation", Builder::new);

    static {
        PARSER.declareString(Builder::setNodeId, NODE_ID_FIELD);
        PARSER.declareString(Builder::setJobId, JOB_ID_FIELD);
        PARSER.declareBoolean(Builder::setIgnoreDowntime, IGNORE_DOWNTIME_FIELD);
        PARSER.declareField(Builder::setState, (p, c) -> JobState.fromString(p.text()), STATE, ObjectParser.ValueType.STRING);
        PARSER.declareString(Builder::setStateReason, STATE_REASON);
    }

    private final String nodeId;
    private final String jobId;
    private final boolean ignoreDowntime;
    private final JobState state;
    private final String stateReason;

    public Allocation(String nodeId, String jobId, boolean ignoreDowntime, JobState state, String stateReason) {
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.ignoreDowntime = ignoreDowntime;
        this.state = state;
        this.stateReason = stateReason;
    }

    public Allocation(StreamInput in) throws IOException {
        this.nodeId = in.readOptionalString();
        this.jobId = in.readString();
        this.ignoreDowntime = in.readBoolean();
        this.state = JobState.fromStream(in);
        this.stateReason = in.readOptionalString();
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getJobId() {
        return jobId;
    }

    /**
     * @return Whether to ignore downtime at startup.
     *
     * When the job state is set to STARTED, to ignoreDowntime will be set to false.
     */
    public boolean isIgnoreDowntime() {
        return ignoreDowntime;
    }

    public JobState getState() {
        return state;
    }

    public String getStateReason() {
        return stateReason;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(nodeId);
        out.writeString(jobId);
        out.writeBoolean(ignoreDowntime);
        state.writeTo(out);
        out.writeOptionalString(stateReason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (nodeId != null) {
            builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
        }
        builder.field(JOB_ID_FIELD.getPreferredName(), jobId);
        builder.field(IGNORE_DOWNTIME_FIELD.getPreferredName(), ignoreDowntime);
        builder.field(STATE.getPreferredName(), state);
        if (stateReason != null) {
            builder.field(STATE_REASON.getPreferredName(), stateReason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Allocation that = (Allocation) o;
        return Objects.equals(nodeId, that.nodeId) &&
                Objects.equals(jobId, that.jobId) &&
                Objects.equals(ignoreDowntime, that.ignoreDowntime) &&
                Objects.equals(state, that.state) &&
                Objects.equals(stateReason, that.stateReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, jobId, ignoreDowntime, state, stateReason);
    }

    // Class already extends from AbstractDiffable, so copied from ToXContentToBytes#toString()
    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String nodeId;
        private String jobId;
        private boolean ignoreDowntime;
        private JobState state;
        private String stateReason;

        public Builder() {
        }

        public Builder(Job job) {
            this.jobId = job.getId();
        }

        public Builder(Allocation allocation) {
            this.nodeId = allocation.nodeId;
            this.jobId = allocation.jobId;
            this.ignoreDowntime  = allocation.ignoreDowntime;
            this.state = allocation.state;
            this.stateReason = allocation.stateReason;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public void setIgnoreDowntime(boolean ignoreDownTime) {
            this.ignoreDowntime = ignoreDownTime;
        }

        @SuppressWarnings("incomplete-switch")
        public void setState(JobState newState) {
            if (this.state != null) {
                switch (newState) {
                    case CLOSING:
                        if (this.state != JobState.OPENED) {
                            throw new IllegalArgumentException("[" + jobId + "] expected state [" + JobState.OPENED
                                    + "], but got [" + state +"]");
                        }
                        break;
                    case OPENING:
                        if (this.state.isAnyOf(JobState.CLOSED, JobState.FAILED) == false) {
                            throw new IllegalArgumentException("[" + jobId + "] expected state [" + JobState.CLOSED
                                    + "] or [" + JobState.FAILED + "], but got [" + state +"]");
                        }
                        break;
                    case OPENED:
                        ignoreDowntime = false;
                        break;
                }
            }

            this.state = newState;
        }

        public void setStateReason(String stateReason) {
            this.stateReason = stateReason;
        }

        public Allocation build() {
            return new Allocation(nodeId, jobId, ignoreDowntime, state, stateReason);
        }

    }
}
