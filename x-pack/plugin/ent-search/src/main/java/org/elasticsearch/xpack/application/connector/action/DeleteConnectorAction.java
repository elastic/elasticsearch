/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class DeleteConnectorAction {

    public static final String NAME = "cluster:admin/xpack/connector/delete";
    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>(NAME);

    private DeleteConnectorAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final boolean hardDelete;
        private final boolean deleteSyncJobs;

        private static final ParseField CONNECTOR_ID_FIELD = new ParseField("connector_id");
        private static final ParseField HARD_DELETE_FIELD = new ParseField("hard");
        private static final ParseField DELETE_SYNC_JOB_FIELD = new ParseField("delete_sync_jobs");

        public Request(String connectorId, boolean hardDelete, boolean deleteSyncJobs) {
            this.connectorId = connectorId;
            this.hardDelete = hardDelete;
            this.deleteSyncJobs = deleteSyncJobs;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("connector_id missing", validationException);
            }

            return validationException;
        }

        public String getConnectorId() {
            return connectorId;
        }

        public boolean isHardDelete() {
            return hardDelete;
        }

        public boolean shouldDeleteSyncJobs() {
            return deleteSyncJobs;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return hardDelete == request.hardDelete
                && deleteSyncJobs == request.deleteSyncJobs
                && Objects.equals(connectorId, request.connectorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, hardDelete, deleteSyncJobs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONNECTOR_ID_FIELD.getPreferredName(), connectorId);
            builder.field(HARD_DELETE_FIELD.getPreferredName(), hardDelete);
            builder.field(DELETE_SYNC_JOB_FIELD.getPreferredName(), deleteSyncJobs);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<DeleteConnectorAction.Request, Void> PARSER = new ConstructingObjectParser<>(
            "delete_connector_request",
            false,
            (p) -> new Request((String) p[0], (boolean) p[1], (boolean) p[2])
        );
        static {
            PARSER.declareString(constructorArg(), CONNECTOR_ID_FIELD);
            PARSER.declareBoolean(constructorArg(), HARD_DELETE_FIELD);
            PARSER.declareBoolean(constructorArg(), DELETE_SYNC_JOB_FIELD);
        }

        public static DeleteConnectorAction.Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

    }
}
