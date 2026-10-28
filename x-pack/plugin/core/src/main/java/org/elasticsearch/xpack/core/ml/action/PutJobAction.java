/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;

import java.io.IOException;
import java.util.Objects;

public class PutJobAction extends ActionType<PutJobAction.Response> {

    public static final PutJobAction INSTANCE = new PutJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/put";

    private PutJobAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements Releasable {

        /**
         * Carries the caller's cloud credential from the coordinating node to the master for the embedded {@code datafeed_config}.
         * Dedicated version: {@code datafeed_cloud_internal_credential} predates this field on {@link PutJobAction.Request}.
         */
        public static final TransportVersion ML_PUT_JOB_CLOUD_CREDENTIAL = TransportVersion.fromName("ml_put_job_cloud_credential");

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

        // Caller's cloud credential for the embedded datafeed, carried on the request so it survives coordinator -> master transport.
        @Nullable
        private CloudCredential cloudCredential;

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
            if (in.getTransportVersion().supports(ML_PUT_JOB_CLOUD_CREDENTIAL)) {
                cloudCredential = in.readOptionalWriteable(CloudCredential::new);
            } else {
                cloudCredential = null;
            }
        }

        public Job.Builder getJobBuilder() {
            return jobBuilder;
        }

        @Nullable
        public CloudCredential getCloudCredential() {
            return cloudCredential;
        }

        public void setCloudCredential(@Nullable CloudCredential cloudCredential) {
            // Zero a previously carried credential rather than orphaning it; re-setting the same instance must not close it.
            if (this.cloudCredential != null && this.cloudCredential != cloudCredential) {
                IOUtils.closeWhileHandlingException(this.cloudCredential);
            }
            this.cloudCredential = cloudCredential;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            jobBuilder.writeTo(out);
            if (out.getTransportVersion().supports(ML_PUT_JOB_CLOUD_CREDENTIAL)) {
                out.writeOptionalWriteable(cloudCredential);
            }
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(cloudCredential);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            // cloudCredential is intentionally excluded: request-scoped secret carrier, not logical identity.
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
