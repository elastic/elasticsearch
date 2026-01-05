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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateConnectorApiKeyIdAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_api_key_id";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorApiKeyIdAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;

        @Nullable
        private final String apiKeyId;

        @Nullable
        private final String apiKeySecretId;

        public Request(String connectorId, String apiKeyId, String apiKeySecretId) {
            this.connectorId = connectorId;
            this.apiKeyId = apiKeyId;
            this.apiKeySecretId = apiKeySecretId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.apiKeyId = in.readOptionalString();
            this.apiKeySecretId = in.readOptionalString();
        }

        public String getConnectorId() {
            return connectorId;
        }

        public String getApiKeyId() {
            return apiKeyId;
        }

        public String getApiKeySecretId() {
            return apiKeySecretId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }
            if (apiKeyId == null && apiKeySecretId == null) {
                validationException = addValidationError(
                    "[api_key_id] and [api_key_secret_id] cannot both be [null]. Please provide a value for at least one of them.",
                    validationException
                );
            }

            return validationException;
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "connector_update_api_key_id_request",
            false,
            ((args, connectorId) -> new UpdateConnectorApiKeyIdAction.Request(connectorId, (String) args[0], (String) args[1]))
        );

        static {
            PARSER.declareStringOrNull(optionalConstructorArg(), Connector.API_KEY_ID_FIELD);
            PARSER.declareStringOrNull(optionalConstructorArg(), Connector.API_KEY_SECRET_ID_FIELD);
        }

        public static UpdateConnectorApiKeyIdAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                if (apiKeyId != null) {
                    builder.field(Connector.API_KEY_ID_FIELD.getPreferredName(), apiKeyId);
                }
                if (apiKeySecretId != null) {
                    builder.field(Connector.API_KEY_SECRET_ID_FIELD.getPreferredName(), apiKeySecretId);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeOptionalString(apiKeyId);
            out.writeOptionalString(apiKeySecretId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId)
                && Objects.equals(apiKeyId, request.apiKeyId)
                && Objects.equals(apiKeySecretId, request.apiKeySecretId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, apiKeyId, apiKeySecretId);
        }
    }
}
