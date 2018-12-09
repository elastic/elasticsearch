/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PutJobAction extends Action<PutJobAction.Request, PutJobAction.Response, PutJobAction.RequestBuilder> {

    public static final PutJobAction INSTANCE = new PutJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/put";

    private PutJobAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static Request parseRequest(String jobId, XContentParser parser) {
            Job.Builder jobBuilder = Job.STRICT_PARSER.apply(parser, null);
            if (jobBuilder.getId() == null) {
                jobBuilder.setId(jobId);
            } else if (!Strings.isNullOrEmpty(jobId) && !jobId.equals(jobBuilder.getId())) {
                // If we have both URI and body jobBuilder ID, they must be identical
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, Job.ID.getPreferredName(),
                        jobBuilder.getId(), jobId));
            }

            return new Request(jobBuilder);
        }

        private Job.Builder jobBuilder;

        public Request(Job.Builder jobBuilder) {
            // Validate the jobBuilder immediately so that errors can be detected prior to transportation.
            jobBuilder.validateInputFields();

            // Some fields cannot be set at create time
            List<String> invalidJobCreationSettings = jobBuilder.invalidCreateTimeSettings();
            if (invalidJobCreationSettings.isEmpty() == false) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_INVALID_CREATE_SETTINGS,
                        String.join(",", invalidJobCreationSettings)));
            }

            this.jobBuilder = jobBuilder;
        }

        public Request() {
        }

        public Job.Builder getJobBuilder() {
            return jobBuilder;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobBuilder = new Job.Builder(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            jobBuilder.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            jobBuilder.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(jobBuilder, request.jobBuilder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobBuilder);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, PutJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Job job;

        public Response(Job job) {
            this.job = job;
        }

        public Response() {
        }

        public Job getResponse() {
            return job;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.getVersion().before(Version.V_6_3_0)) {
                //the acknowledged flag was removed
                in.readBoolean();
            }
            job = new Job(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().before(Version.V_6_3_0)) {
                //the acknowledged flag is no longer supported
                out.writeBoolean(true);
            }
            job.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            job.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(job, response.job);
        }

        @Override
        public int hashCode() {
            return Objects.hash(job);
        }
    }
}
