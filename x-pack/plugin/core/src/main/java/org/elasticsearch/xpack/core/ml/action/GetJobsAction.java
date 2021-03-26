/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class GetJobsAction extends ActionType<GetJobsAction.Response> {

    public static final GetJobsAction INSTANCE = new GetJobsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/get";

    private GetJobsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        @Deprecated
        public static final String ALLOW_NO_JOBS = "allow_no_jobs";
        public static final String ALLOW_NO_MATCH = "allow_no_match";

        private String jobId;
        private boolean allowNoMatch = true;

        public Request(String jobId) {
            this();
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Request() {
            local(true);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            allowNoMatch = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(allowNoMatch);
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        public String getJobId() {
            return jobId;
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, allowNoMatch);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(jobId, other.jobId) && Objects.equals(allowNoMatch, other.allowNoMatch);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<Job> implements ToXContentObject {

        public Response(QueryPage<Job> jobs) {
            super(jobs);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public QueryPage<Job> getResponse() {
            return getResources();
        }

        @Override
        protected Reader<Job> getReader() {
            return Job::new;
        }
    }


}
