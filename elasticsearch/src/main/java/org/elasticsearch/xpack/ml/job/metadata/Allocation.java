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
import org.elasticsearch.xpack.ml.job.config.JobStatus;

import java.io.IOException;
import java.util.Objects;

public class Allocation extends AbstractDiffable<Allocation> implements ToXContent {

    private static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    private static final ParseField JOB_ID_FIELD = new ParseField("job_id");
    private static final ParseField IGNORE_DOWNTIME_FIELD = new ParseField("ignore_downtime");
    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField STATUS_REASON = new ParseField("status_reason");

    static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("allocation", Builder::new);

    static {
        PARSER.declareString(Builder::setNodeId, NODE_ID_FIELD);
        PARSER.declareString(Builder::setJobId, JOB_ID_FIELD);
        PARSER.declareBoolean(Builder::setIgnoreDowntime, IGNORE_DOWNTIME_FIELD);
        PARSER.declareField(Builder::setStatus, (p, c) -> JobStatus.fromString(p.text()), STATUS, ObjectParser.ValueType.STRING);
        PARSER.declareString(Builder::setStatusReason, STATUS_REASON);
    }

    private final String nodeId;
    private final String jobId;
    private final boolean ignoreDowntime;
    private final JobStatus status;
    private final String statusReason;

    public Allocation(String nodeId, String jobId, boolean ignoreDowntime, JobStatus status, String statusReason) {
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.ignoreDowntime = ignoreDowntime;
        this.status = status;
        this.statusReason = statusReason;
    }

    public Allocation(StreamInput in) throws IOException {
        this.nodeId = in.readOptionalString();
        this.jobId = in.readString();
        this.ignoreDowntime = in.readBoolean();
        this.status = JobStatus.fromStream(in);
        this.statusReason = in.readOptionalString();
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
     * When the job status is set to STARTED, to ignoreDowntime will be set to false.
     */
    public boolean isIgnoreDowntime() {
        return ignoreDowntime;
    }

    public JobStatus getStatus() {
        return status;
    }

    public String getStatusReason() {
        return statusReason;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(nodeId);
        out.writeString(jobId);
        out.writeBoolean(ignoreDowntime);
        status.writeTo(out);
        out.writeOptionalString(statusReason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (nodeId != null) {
            builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
        }
        builder.field(JOB_ID_FIELD.getPreferredName(), jobId);
        builder.field(IGNORE_DOWNTIME_FIELD.getPreferredName(), ignoreDowntime);
        builder.field(STATUS.getPreferredName(), status);
        if (statusReason != null) {
            builder.field(STATUS_REASON.getPreferredName(), statusReason);
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
                Objects.equals(status, that.status) &&
                Objects.equals(statusReason, that.statusReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, jobId, ignoreDowntime, status, statusReason);
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
        private JobStatus status;
        private String statusReason;

        public Builder() {
        }

        public Builder(Job job) {
            this.jobId = job.getId();
        }

        public Builder(Allocation allocation) {
            this.nodeId = allocation.nodeId;
            this.jobId = allocation.jobId;
            this.ignoreDowntime  = allocation.ignoreDowntime;
            this.status = allocation.status;
            this.statusReason = allocation.statusReason;
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
        public void setStatus(JobStatus newStatus) {
            if (this.status != null) {
                switch (newStatus) {
                    case CLOSING:
                        if (this.status != JobStatus.OPENED) {
                            throw new IllegalArgumentException("[" + jobId + "] expected status [" + JobStatus.OPENED
                                    + "], but got [" + status +"]");
                        }
                        break;
                    case OPENING:
                        if (this.status.isAnyOf(JobStatus.CLOSED, JobStatus.FAILED) == false) {
                            throw new IllegalArgumentException("[" + jobId + "] expected status [" + JobStatus.CLOSED
                                    + "] or [" + JobStatus.FAILED + "], but got [" + status +"]");
                        }
                        break;
                    case OPENED:
                        ignoreDowntime = false;
                        break;
                }
            }

            this.status = newStatus;
        }

        public void setStatusReason(String statusReason) {
            this.statusReason = statusReason;
        }

        public Allocation build() {
            return new Allocation(nodeId, jobId, ignoreDowntime, status, statusReason);
        }

    }
}
