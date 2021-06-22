/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Objects;

public class DeleteExpiredDataAction extends ActionType<DeleteExpiredDataAction.Response> {

    public static final DeleteExpiredDataAction INSTANCE = new DeleteExpiredDataAction();
    public static final String NAME = "cluster:admin/xpack/ml/delete_expired_data";

    private DeleteExpiredDataAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField REQUESTS_PER_SECOND = new ParseField("requests_per_second");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(
            "delete_expired_data_request",
            false,
            Request::new);

        static {
            PARSER.declareFloat(Request::setRequestsPerSecond, REQUESTS_PER_SECOND);
            PARSER.declareString((obj, value) -> obj.setTimeout(TimeValue.parseTimeValue(value, TIMEOUT.getPreferredName())),
                TIMEOUT);
            PARSER.declareString(Request::setJobId, Job.ID);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private Float requestsPerSecond;
        private TimeValue timeout;
        private String jobId;
        private String [] expandedJobIds;

        public Request() {}

        public Request(Float requestsPerSecond, TimeValue timeValue) {
            this.requestsPerSecond = requestsPerSecond;
            this.timeout = timeValue;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.requestsPerSecond = in.readOptionalFloat();
            this.timeout = in.readOptionalTimeValue();
            this.jobId = in.readOptionalString();
        }

        public Float getRequestsPerSecond() {
            return requestsPerSecond;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public String getJobId() {
            return jobId;
        }

        public Request setRequestsPerSecond(Float requestsPerSecond) {
            this.requestsPerSecond = requestsPerSecond;
            return this;
        }

        public Request setTimeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Request setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Not serialized, the expanded job Ids should only be used
         * on the executing node.
         * @return The expanded Ids in the case where {@code jobId} is not `_all`
         * otherwise null.
         */
        public String [] getExpandedJobIds() {
            return expandedJobIds;
        }

        public void setExpandedJobIds(String [] expandedJobIds) {
            this.expandedJobIds = expandedJobIds;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (this.requestsPerSecond != null && this.requestsPerSecond != -1.0f && this.requestsPerSecond <= 0) {
                ActionRequestValidationException requestValidationException = new ActionRequestValidationException();
                requestValidationException.addValidationError("[requests_per_second] must either be -1 or greater than 0");
                return requestValidationException;
            }
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(requestsPerSecond, request.requestsPerSecond)
                && Objects.equals(jobId, request.jobId)
                && Objects.equals(expandedJobIds, request.expandedJobIds)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestsPerSecond, timeout, jobId, expandedJobIds);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalFloat(requestsPerSecond);
            out.writeOptionalTimeValue(timeout);
            out.writeOptionalString(jobId);
            // expandedJobIds are set on the node and not part of serialisation
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private static final ParseField DELETED = new ParseField("deleted");

        private final boolean deleted;

        public Response(boolean deleted) {
            this.deleted = deleted;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            deleted = in.readBoolean();
        }

        public boolean isDeleted() {
            return deleted;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(deleted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DELETED.getPreferredName(), deleted);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(deleted, response.deleted);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deleted);
        }
    }

}
