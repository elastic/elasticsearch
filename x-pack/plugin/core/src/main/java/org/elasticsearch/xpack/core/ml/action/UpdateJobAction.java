/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class UpdateJobAction extends ActionType<PutJobAction.Response> {
    public static final UpdateJobAction INSTANCE = new UpdateJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/update";

    private UpdateJobAction() {
        super(NAME, PutJobAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<UpdateJobAction.Request> implements ToXContentObject {

        public static UpdateJobAction.Request parseRequest(String jobId, XContentParser parser) {
            JobUpdate update = JobUpdate.EXTERNAL_PARSER.apply(parser, null).setJobId(jobId).build();
            return new UpdateJobAction.Request(jobId, update);
        }

        private String jobId;
        private JobUpdate update;

        /** Indicates an update that was not triggered by a user */
        private boolean isInternal;

        public Request(String jobId, JobUpdate update) {
            this(jobId, update, false);
        }

        private Request(String jobId, JobUpdate update, boolean isInternal) {
            this.jobId = jobId;
            this.update = update;
            this.isInternal = isInternal;
            if (Metadata.ALL.equals(jobId)) {
                throw ExceptionsHelper.badRequestException("Cannot update more than 1 job at a time");
            }
        }

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            update = new JobUpdate(in);
            isInternal = in.readBoolean();
        }

        public static Request internal(String jobId, JobUpdate update) {
            return new Request(jobId, update, true);
        }

        public String getJobId() {
            return jobId;
        }

        public JobUpdate getJobUpdate() {
            return update;
        }

        public boolean isInternal() {
            return isInternal;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            update.writeTo(out);
            out.writeBoolean(isInternal);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // only serialize the update, as the job id is specified as part of the url
            update.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UpdateJobAction.Request that = (UpdateJobAction.Request) o;
            return Objects.equals(jobId, that.jobId) &&
                    Objects.equals(update, that.update) &&
                    isInternal == that.isInternal;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, update, isInternal);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, PutJobAction.Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, UpdateJobAction action) {
            super(client, action, new Request());
        }
    }

}
