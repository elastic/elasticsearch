/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.action.ConnectorUpdateActionResponse;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class UpdateConnectorSyncJobErrorAction {

    public static final String NAME = "cluster:admin/xpack/connector/sync_job/update_error";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorSyncJobErrorAction() {/* no instances */}

    public static final String ERROR_EMPTY_MESSAGE = "[error] of the connector sync job cannot be null or empty";

    public static class Request extends ConnectorSyncJobActionRequest implements ToXContentObject {
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "connector_sync_job_error_request",
            false,
            ((args, connectorSyncJobId) -> new Request(connectorSyncJobId, (String) args[0]))
        );

        static {
            PARSER.declareString(constructorArg(), ConnectorSyncJob.ERROR_FIELD);
        }

        private final String connectorSyncJobId;

        private final String error;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorSyncJobId = in.readString();
            this.error = in.readString();
        }

        public Request(String connectorSyncJobId, String error) {
            this.connectorSyncJobId = connectorSyncJobId;
            this.error = error;
        }

        public static UpdateConnectorSyncJobErrorAction.Request fromXContent(XContentParser parser, String connectorSyncJobId)
            throws IOException {
            return PARSER.parse(parser, connectorSyncJobId);
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

            if (Strings.isNullOrEmpty(error)) {
                validationException = addValidationError(ERROR_EMPTY_MESSAGE, validationException);
            }

            return validationException;
        }

        public String getConnectorSyncJobId() {
            return connectorSyncJobId;
        }

        public String getError() {
            return error;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorSyncJobId);
            out.writeString(error);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorSyncJobId, request.connectorSyncJobId) && Objects.equals(error, request.error);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorSyncJobId, error);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(ConnectorSyncJob.ERROR_FIELD.getPreferredName(), error);
            }
            builder.endObject();
            return builder;
        }

    }

}
