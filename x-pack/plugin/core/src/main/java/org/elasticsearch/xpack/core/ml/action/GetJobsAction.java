/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
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

        public static final ParseField ALLOW_NO_JOBS = new ParseField("allow_no_jobs");

        private String jobId;
        private boolean allowNoJobs = true;

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
            allowNoJobs = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(allowNoJobs);
        }

        public void setAllowNoJobs(boolean allowNoJobs) {
            this.allowNoJobs = allowNoJobs;
        }

        public String getJobId() {
            return jobId;
        }

        public boolean allowNoJobs() {
            return allowNoJobs;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, allowNoJobs);
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
            return Objects.equals(jobId, other.jobId) && Objects.equals(allowNoJobs, other.allowNoJobs);
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetJobsAction action) {
            super(client, action, new Request());
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
