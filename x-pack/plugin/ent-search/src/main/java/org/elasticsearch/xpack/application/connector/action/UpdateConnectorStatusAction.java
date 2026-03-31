/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorStatus;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateConnectorStatusAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_status";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    public UpdateConnectorStatusAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final ConnectorStatus status;

        public Request(String connectorId, ConnectorStatus status) {
            this.connectorId = connectorId;
            this.status = status;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.status = in.readEnum(ConnectorStatus.class);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public ConnectorStatus getStatus() {
            return status;
        }

        private static final ConstructingObjectParser<UpdateConnectorStatusAction.Request, String> PARSER = new ConstructingObjectParser<>(
            "connector_update_status_request",
            false,
            ((args, connectorId) -> new UpdateConnectorStatusAction.Request(connectorId, (ConnectorStatus) args[0]))
        );

        static {
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> ConnectorStatus.connectorStatus(p.text()),
                Connector.STATUS_FIELD,
                ObjectParser.ValueType.STRING
            );
        }

        public static UpdateConnectorStatusAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.STATUS_FIELD.getPreferredName(), status);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            if (status == null) {
                validationException = addValidationError("[status] cannot be [null].", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeEnum(status);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && status == request.status;
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, status);
        }
    }
}
