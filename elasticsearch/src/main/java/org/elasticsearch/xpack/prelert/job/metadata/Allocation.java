/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class Allocation extends AbstractDiffable<Allocation> implements ToXContent {

    private static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    private static final ParseField JOB_ID_FIELD = new ParseField("job_id");
    private static final ParseField IGNORE_DOWNTIME_FIELD = new ParseField("ignore_downtime");
    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField STATUS_REASON = new ParseField("status_reason");
    public static final ParseField SCHEDULER_STATE = new ParseField("scheduler_state");

    static final Allocation PROTO = new Allocation(null, null, false, null, null, null);

    static final ObjectParser<Builder, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("allocation", Builder::new);

    static {
        PARSER.declareString(Builder::setNodeId, NODE_ID_FIELD);
        PARSER.declareString(Builder::setJobId, JOB_ID_FIELD);
        PARSER.declareBoolean(Builder::setIgnoreDowntime, IGNORE_DOWNTIME_FIELD);
        PARSER.declareField(Builder::setStatus, (p, c) -> JobStatus.fromString(p.text()), STATUS, ObjectParser.ValueType.STRING);
        PARSER.declareString(Builder::setStatusReason, STATUS_REASON);
        PARSER.declareObject(Builder::setSchedulerState, SchedulerState.PARSER, SCHEDULER_STATE);
    }

    private final String nodeId;
    private final String jobId;
    private final boolean ignoreDowntime;
    private final JobStatus status;
    private final String statusReason;
    private final SchedulerState schedulerState;

    public Allocation(String nodeId, String jobId, boolean ignoreDowntime, JobStatus status, String statusReason,
                      SchedulerState schedulerState) {
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.ignoreDowntime = ignoreDowntime;
        this.status = status;
        this.statusReason = statusReason;
        this.schedulerState = schedulerState;
    }

    public Allocation(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.jobId = in.readString();
        this.ignoreDowntime = in.readBoolean();
        this.status = JobStatus.fromStream(in);
        this.statusReason = in.readOptionalString();
        this.schedulerState = in.readOptionalWriteable(SchedulerState::new);
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

    public SchedulerState getSchedulerState() {
        return schedulerState;
    }

    @Override
    public Allocation readFrom(StreamInput in) throws IOException {
        return new Allocation(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeString(jobId);
        out.writeBoolean(ignoreDowntime);
        status.writeTo(out);
        out.writeOptionalString(statusReason);
        out.writeOptionalWriteable(schedulerState);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
        builder.field(JOB_ID_FIELD.getPreferredName(), jobId);
        builder.field(IGNORE_DOWNTIME_FIELD.getPreferredName(), ignoreDowntime);
        builder.field(STATUS.getPreferredName(), status);
        if (statusReason != null) {
            builder.field(STATUS_REASON.getPreferredName(), statusReason);
        }
        if (schedulerState != null) {
            builder.field(SCHEDULER_STATE.getPreferredName(), schedulerState);
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
                Objects.equals(statusReason, that.statusReason) &&
                Objects.equals(schedulerState, that.schedulerState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, jobId, ignoreDowntime, status, statusReason, schedulerState);
    }

    // Class alreadt extends from AbstractDiffable, so copied from ToXContentToBytes#toString()
    @SuppressWarnings("deprecation")
    @Override
    public final String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            // So we have a stack trace logged somewhere
            return "{ \"error\" : \"" + org.elasticsearch.ExceptionsHelper.detailedMessage(e) + "\"}";
        }
    }

    public static class Builder {

        private String nodeId;
        private String jobId;
        private boolean ignoreDowntime;
        private JobStatus status;
        private String statusReason;
        private SchedulerState schedulerState;

        public Builder() {
        }

        public Builder(Allocation allocation) {
            this.nodeId = allocation.nodeId;
            this.jobId = allocation.jobId;
            this.ignoreDowntime  = allocation.ignoreDowntime;
            this.status = allocation.status;
            this.statusReason = allocation.statusReason;
            this.schedulerState = allocation.schedulerState;
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
                        if (this.status.isAnyOf(JobStatus.CLOSED, JobStatus.FAILED)) {
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

        public void setSchedulerState(SchedulerState schedulerState) {
            JobSchedulerStatus currentSchedulerStatus = this.schedulerState == null ?
                    JobSchedulerStatus.STOPPED : this.schedulerState.getStatus();
            JobSchedulerStatus newSchedulerStatus = schedulerState.getStatus();
            switch (newSchedulerStatus) {
            case STARTING:
                if (currentSchedulerStatus != JobSchedulerStatus.STOPPED) {
                    String msg = Messages.getMessage(Messages.JOB_SCHEDULER_CANNOT_START, jobId, newSchedulerStatus);
                    throw ExceptionsHelper.conflictStatusException(msg);
                }
                break;
            case STARTED:
                if (currentSchedulerStatus != JobSchedulerStatus.STARTING) {
                    String msg = Messages.getMessage(Messages.JOB_SCHEDULER_CANNOT_START, jobId, newSchedulerStatus);
                    throw ExceptionsHelper.conflictStatusException(msg);
                }
                break;
            case STOPPING:
                if (currentSchedulerStatus != JobSchedulerStatus.STARTED) {
                    String msg = Messages.getMessage(Messages.JOB_SCHEDULER_CANNOT_STOP_IN_CURRENT_STATE, jobId, newSchedulerStatus);
                    throw ExceptionsHelper.conflictStatusException(msg);
                }
                break;
            case STOPPED:
                if ((currentSchedulerStatus != JobSchedulerStatus.STOPPED ||
                currentSchedulerStatus != JobSchedulerStatus.STOPPING) == false) {
                    String msg = Messages.getMessage(Messages.JOB_SCHEDULER_CANNOT_STOP_IN_CURRENT_STATE, jobId, newSchedulerStatus);
                    throw ExceptionsHelper.conflictStatusException(msg);
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid requested job scheduler status: " + newSchedulerStatus);
            }

            this.schedulerState = schedulerState;
        }

        public Allocation build() {
            return new Allocation(nodeId, jobId, ignoreDowntime, status, statusReason, schedulerState);
        }

    }
}
