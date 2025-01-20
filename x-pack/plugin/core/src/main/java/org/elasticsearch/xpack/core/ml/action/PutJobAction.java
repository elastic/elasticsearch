/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Objects;

public class PutJobAction extends ActionType<PutJobAction.Response> {

    public static final PutJobAction INSTANCE = new PutJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/put";

    private PutJobAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static Request parseRequest(String jobId, XContentParser parser, IndicesOptions indicesOptions) {
            Job.Builder jobBuilder = Job.REST_REQUEST_PARSER.apply(parser, null);
            if (jobBuilder.getId() == null) {
                jobBuilder.setId(jobId);
            } else if (Strings.isNullOrEmpty(jobId) == false && jobId.equals(jobBuilder.getId()) == false) {
                // If we have both URI and body jobBuilder ID, they must be identical
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, Job.ID.getPreferredName(), jobBuilder.getId(), jobId)
                );
            }
            jobBuilder.setDatafeedIndicesOptionsIfRequired(indicesOptions);
            return new Request(jobBuilder);
        }

        private final Job.Builder jobBuilder;

        public Request(Job.Builder jobBuilder) {
            // Validate the jobBuilder immediately so that errors can be detected prior to transportation.
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            jobBuilder.validateInputFields();
            // Validate that detector configs are unique.
            // This validation logically belongs to validateInputFields call but we perform it only for PUT action to avoid BWC issues which
            // would occur when parsing an old job config that already had duplicate detectors.
            jobBuilder.validateDetectorsAreUnique();

            this.jobBuilder = jobBuilder;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobBuilder = new Job.Builder(in);
        }

        public Job.Builder getJobBuilder() {
            return jobBuilder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            jobBuilder.writeTo(out);
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
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Job job;

        public Response(Job job) {
            this.job = job;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            job = new Job(in);
        }

        public Job getResponse() {
            return job;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
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
