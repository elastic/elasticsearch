/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Objects;

public class CloseJobAction extends ActionType<CloseJobAction.Response> {

    public static final CloseJobAction INSTANCE = new CloseJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/close";

    private CloseJobAction() {
        super(NAME, CloseJobAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField FORCE = new ParseField("force");
        @Deprecated
        public static final String ALLOW_NO_JOBS = "allow_no_jobs";
        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match", ALLOW_NO_JOBS);
        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setJobId, Job.ID);
            PARSER.declareString((request, val) ->
                    request.setCloseTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareBoolean(Request::setForce, FORCE);
            PARSER.declareBoolean(Request::setAllowNoMatch, ALLOW_NO_MATCH);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.setJobId(jobId);
            }
            return request;
        }

        private String jobId;
        private boolean force = false;
        private boolean allowNoMatch = true;
        // A big state can take a while to persist.  For symmetry with the _open endpoint any
        // changes here should be reflected there too.
        private TimeValue timeout = MachineLearningField.STATE_PERSIST_RESTORE_TIMEOUT;

        private String[] openJobIds;

        private boolean local;

        public Request() {
            openJobIds = new String[] {};
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            timeout = in.readTimeValue();
            force = in.readBoolean();
            openJobIds = in.readStringArray();
            local = in.readBoolean();
            allowNoMatch = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeTimeValue(timeout);
            out.writeBoolean(force);
            out.writeStringArray(openJobIds);
            out.writeBoolean(local);
            out.writeBoolean(allowNoMatch);
        }

        public Request(String jobId) {
            this();
            this.jobId = jobId;
        }

        public String getJobId() {
            return jobId;
        }

        public Request setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public TimeValue getCloseTimeout() {
            return timeout;
        }

        public Request setCloseTimeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public boolean isForce() {
            return force;
        }

        public Request setForce(boolean force) {
            this.force = force;
            return this;
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        public Request setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
            return this;
        }

        public boolean isLocal() { return local; }

        public Request setLocal(boolean local) {
            this.local = local;
            return this;
        }

        public String[] getOpenJobIds() { return openJobIds; }

        public Request setOpenJobIds(String[] openJobIds) {
            this.openJobIds = openJobIds;
            return this;
        }

        @Override
        public boolean match(Task task) {
            for (String id : openJobIds) {
                if (OpenJobAction.JobTaskMatcher.match(task, id)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // openJobIds are excluded
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.field(FORCE.getPreferredName(), force);
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            // openJobIds are excluded
            return Objects.hash(jobId, timeout, force, allowNoMatch);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            // openJobIds are excluded
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(timeout, other.timeout) &&
                    Objects.equals(force, other.force) &&
                    Objects.equals(allowNoMatch, other.allowNoMatch);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean closed;

        public Response(boolean closed) {
            super(null, null);
            this.closed = closed;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            closed = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(closed);
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("closed", closed);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return closed == response.closed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(closed);
        }
    }

}

