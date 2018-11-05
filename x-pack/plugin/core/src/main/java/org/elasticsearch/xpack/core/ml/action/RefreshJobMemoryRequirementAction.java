/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Objects;

public class RefreshJobMemoryRequirementAction extends Action<RefreshJobMemoryRequirementAction.Request, AcknowledgedResponse,
    RefreshJobMemoryRequirementAction.RequestBuilder> {

    public static final RefreshJobMemoryRequirementAction INSTANCE = new RefreshJobMemoryRequirementAction();
    public static final String NAME = "cluster:internal/xpack/ml/job/refresh_memory_requirement";

    private RefreshJobMemoryRequirementAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setJobId, Job.ID);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.setJobId(jobId);
            }
            return request;
        }

        private String jobId;

        public Request(String jobId) {

            this.jobId = jobId;
        }

        public Request() {
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request request = (Request) o;
            return Objects.equals(jobId, request.jobId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, RefreshJobMemoryRequirementAction action) {
            super(client, action, new Request());
        }
    }
}
