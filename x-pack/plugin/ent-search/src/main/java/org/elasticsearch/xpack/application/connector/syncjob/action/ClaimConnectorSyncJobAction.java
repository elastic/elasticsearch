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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.action.ConnectorUpdateActionResponse;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ClaimConnectorSyncJobAction {
    public static final ParseField CONNECTOR_SYNC_JOB_ID_FIELD = new ParseField("connector_sync_job_id");
    public static final String NAME = "cluster:admin/xpack/connector/sync_job/claim";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private ClaimConnectorSyncJobAction() {/* no instances */}

    public static class Request extends ConnectorSyncJobActionRequest implements ToXContentObject {

        private final String connectorSyncJobId;
        private final String workerHostname;
        private final Object syncCursor;

        public String getConnectorSyncJobId() {
            return connectorSyncJobId;
        }

        public String getWorkerHostname() {
            return workerHostname;
        }

        public Object getSyncCursor() {
            return syncCursor;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorSyncJobId = in.readString();
            this.workerHostname = in.readString();
            this.syncCursor = in.readGenericValue();
        }

        public Request(String connectorSyncJobId, String workerHostname, Object syncCursor) {
            this.connectorSyncJobId = connectorSyncJobId;
            this.workerHostname = workerHostname;
            this.syncCursor = syncCursor;
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "claim_connector_sync_job",
            false,
            (args, connectorSyncJobId) -> {
                String workerHostname = (String) args[0];
                Object syncCursor = args[1];

                return new Request(connectorSyncJobId, workerHostname, syncCursor);
            }
        );

        static {
            PARSER.declareString(constructorArg(), ConnectorSyncJob.WORKER_HOSTNAME_FIELD);
            PARSER.declareObject(optionalConstructorArg(), (parser, context) -> parser.map(), Connector.SYNC_CURSOR_FIELD);
        }

        public static Request fromXContent(XContentParser parser, String connectorSyncJobId) throws IOException {
            return PARSER.parse(parser, connectorSyncJobId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(ConnectorSyncJob.WORKER_HOSTNAME_FIELD.getPreferredName(), workerHostname);
                if (syncCursor != null) {
                    builder.field(Connector.SYNC_CURSOR_FIELD.getPreferredName(), syncCursor);
                }
            }
            builder.endObject();
            return builder;
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

            if (workerHostname == null) {
                validationException = addValidationError(
                    ConnectorSyncJobConstants.EMPTY_WORKER_HOSTNAME_ERROR_MESSAGE,
                    validationException
                );
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorSyncJobId);
            out.writeString(workerHostname);
            out.writeGenericValue(syncCursor);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorSyncJobId, request.connectorSyncJobId)
                && Objects.equals(workerHostname, request.workerHostname)
                && Objects.equals(syncCursor, request.syncCursor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorSyncJobId, workerHostname, syncCursor);
        }
    }
}
