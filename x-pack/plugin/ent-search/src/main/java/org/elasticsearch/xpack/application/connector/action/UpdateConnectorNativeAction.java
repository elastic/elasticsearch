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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class UpdateConnectorNativeAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_native";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorNativeAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final boolean isNative;

        public Request(String connectorId, boolean indexName) {
            this.connectorId = connectorId;
            this.isNative = indexName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.isNative = in.readBoolean();
        }

        public String getConnectorId() {
            return connectorId;
        }

        public boolean isNative() {
            return isNative;
        }

        private static final ConstructingObjectParser<UpdateConnectorNativeAction.Request, String> PARSER = new ConstructingObjectParser<>(
            "connector_update_native_request",
            false,
            ((args, connectorId) -> new UpdateConnectorNativeAction.Request(connectorId, (boolean) args[0]))
        );

        static {
            PARSER.declareBoolean(constructorArg(), Connector.IS_NATIVE_FIELD);
        }

        public static UpdateConnectorNativeAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.IS_NATIVE_FIELD.getPreferredName(), isNative);
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

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeBoolean(isNative);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return isNative == request.isNative && Objects.equals(connectorId, request.connectorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, isNative);
        }
    }
}
