/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
        }

        private Float requestsPerSecond;
        private TimeValue timeout;

        public Request() {}

        public Request(Float requestsPerSecond, TimeValue timeValue) {
            this.requestsPerSecond = requestsPerSecond;
            this.timeout = timeValue;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
                this.requestsPerSecond = in.readOptionalFloat();
                this.timeout = in.readOptionalTimeValue();
            } else {
                this.requestsPerSecond = null;
                this.timeout = null;
            }
        }

        public Float getRequestsPerSecond() {
            return requestsPerSecond;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public Request setRequestsPerSecond(Float requestsPerSecond) {
            this.requestsPerSecond = requestsPerSecond;
            return this;
        }

        public Request setTimeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
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
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestsPerSecond, timeout);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
                out.writeOptionalFloat(requestsPerSecond);
                out.writeOptionalTimeValue(timeout);
            }
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response> {
        RequestBuilder(ElasticsearchClient client, DeleteExpiredDataAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private static final ParseField DELETED = new ParseField("deleted");

        private boolean deleted;

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
