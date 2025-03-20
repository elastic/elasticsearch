/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobSearchResult;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class GetConnectorSyncJobAction {

    public static final String NAME = "cluster:admin/xpack/connector/sync_job/get";
    public static final ActionType<GetConnectorSyncJobAction.Response> INSTANCE = new ActionType<>(NAME);

    private GetConnectorSyncJobAction() {/* no instances */}

    public static class Request extends ConnectorSyncJobActionRequest implements ToXContentObject {
        private final String connectorSyncJobId;

        private static final ParseField CONNECTOR_ID_FIELD = new ParseField("connector_id");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorSyncJobId = in.readString();
        }

        public Request(String connectorSyncJobId) {
            this.connectorSyncJobId = connectorSyncJobId;
        }

        public String getConnectorSyncJobId() {
            return connectorSyncJobId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorSyncJobId)) {
                validationException = addValidationError(
                    ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE,
                    validationException
                );
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorSyncJobId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorSyncJobId, request.connectorSyncJobId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorSyncJobId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONNECTOR_ID_FIELD.getPreferredName(), connectorSyncJobId);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "get_connector_sync_job_request",
            false,
            (args) -> new Request((String) args[0])
        );

        static {
            PARSER.declareString(constructorArg(), CONNECTOR_ID_FIELD);
        }

        public static Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final ConnectorSyncJobSearchResult connectorSyncJob;

        public Response(ConnectorSyncJobSearchResult connectorSyncJob) {
            this.connectorSyncJob = connectorSyncJob;
        }

        public Response(StreamInput in) throws IOException {
            this.connectorSyncJob = new ConnectorSyncJobSearchResult(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            connectorSyncJob.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return connectorSyncJob.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(connectorSyncJob, response.connectorSyncJob);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorSyncJob);
        }
    }
}
