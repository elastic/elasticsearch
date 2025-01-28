/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class DeleteConnectorSyncJobAction {

    public static final String NAME = "cluster:admin/xpack/connector/sync_job/delete";
    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>(NAME);

    private DeleteConnectorSyncJobAction() {/* no instances */}

    public static class Request extends ConnectorSyncJobActionRequest implements ToXContentObject {
        public static final ParseField CONNECTOR_SYNC_JOB_ID_FIELD = new ParseField("connector_sync_job_id");

        private final String connectorSyncJobId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorSyncJobId = in.readString();
        }

        public Request(String connectorSyncJobId) {
            this.connectorSyncJobId = connectorSyncJobId;
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

        public String getConnectorSyncJobId() {
            return connectorSyncJobId;
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
            builder.field(CONNECTOR_SYNC_JOB_ID_FIELD.getPreferredName(), connectorSyncJobId);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<DeleteConnectorSyncJobAction.Request, Void> PARSER = new ConstructingObjectParser<>(
            "delete_connector_sync_job_request",
            false,
            (args) -> new Request((String) args[0])
        );

        static {
            PARSER.declareString(constructorArg(), CONNECTOR_SYNC_JOB_ID_FIELD);

        }

        public static DeleteConnectorSyncJobAction.Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

}
