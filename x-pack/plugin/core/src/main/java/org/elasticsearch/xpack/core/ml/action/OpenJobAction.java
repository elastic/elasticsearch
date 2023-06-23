/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;

import java.io.IOException;
import java.util.Objects;

public class OpenJobAction extends ActionType<NodeAcknowledgedResponse> {

    public static final OpenJobAction INSTANCE = new OpenJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/open";

    private OpenJobAction() {
        super(NAME, NodeAcknowledgedResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            JobParams jobParams = JobParams.PARSER.apply(parser, null);
            if (jobId != null) {
                jobParams.jobId = jobId;
            }
            return new Request(jobParams);
        }

        private JobParams jobParams;

        public Request(JobParams jobParams) {
            this.jobParams = Objects.requireNonNull(jobParams);
        }

        public Request(String jobId) {
            this.jobParams = new JobParams(jobId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobParams = new JobParams(in);
        }

        public JobParams getJobParams() {
            return jobParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            jobParams.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            jobParams.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobParams);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            OpenJobAction.Request other = (OpenJobAction.Request) obj;
            return Objects.equals(jobParams, other.jobParams);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class JobParams implements PersistentTaskParams, MlTaskParams {

        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField JOB = new ParseField("job");

        public static final ObjectParser<JobParams, Void> PARSER = new ObjectParser<>(MlTasks.JOB_TASK_NAME, true, JobParams::new);
        static {
            PARSER.declareString(JobParams::setJobId, Job.ID);
            PARSER.declareString((params, val) -> params.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareObject(JobParams::setJob, (p, c) -> Job.LENIENT_PARSER.apply(p, c).build(), JOB);
        }

        public static JobParams fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static JobParams parseRequest(String jobId, XContentParser parser) {
            JobParams params = PARSER.apply(parser, null);
            if (jobId != null) {
                params.jobId = jobId;
            }
            return params;
        }

        private String jobId;
        // A big state can take a while to restore. For symmetry with the _close endpoint any
        // changes here should be reflected there too.
        private TimeValue timeout = MachineLearningField.STATE_PERSIST_RESTORE_TIMEOUT;
        private Job job;

        JobParams() {}

        public JobParams(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public JobParams(StreamInput in) throws IOException {
            jobId = in.readString();
            timeout = TimeValue.timeValueMillis(in.readVLong());
            job = in.readOptionalWriteable(Job::new);
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        @Nullable
        public Job getJob() {
            return job;
        }

        public void setJob(Job job) {
            this.job = job;
        }

        @Override
        public String getWriteableName() {
            return MlTasks.JOB_TASK_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(jobId);
            out.writeVLong(timeout.millis());
            out.writeOptionalWriteable(job);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            if (job != null) {
                builder.field("job", job);
            }
            builder.endObject();
            // The job field is streamed but not persisted
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, timeout, job);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            OpenJobAction.JobParams other = (OpenJobAction.JobParams) obj;
            return Objects.equals(jobId, other.jobId) && Objects.equals(timeout, other.timeout) && Objects.equals(job, other.job);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.MINIMUM_COMPATIBLE;
        }

        @Override
        public String getMlId() {
            return jobId;
        }
    }

    public interface JobTaskMatcher {

        static boolean match(Task task, String expectedJobId) {
            if (task instanceof JobTaskMatcher) {
                if (Strings.isAllOrWildcard(expectedJobId)) {
                    return true;
                }
                String expectedDescription = "job-" + expectedJobId;
                return expectedDescription.equals(task.getDescription());
            }
            return false;
        }
    }
}
