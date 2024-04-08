/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.Connector;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateConnectorNameAction {

    public static final String NAME = "indices:data/write/xpack/connector/update_name";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorNameAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;

        @Nullable
        private final String name;

        @Nullable
        private final String description;

        public Request(String connectorId, String name, String description) {
            this.connectorId = connectorId;
            this.name = name;
            this.description = description;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.name = in.readOptionalString();
            this.description = in.readOptionalString();
        }

        public String getConnectorId() {
            return connectorId;
        }

        public String getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }
            if (name == null && description == null) {
                validationException = addValidationError(
                    "[name] and [description] cannot both be [null]. Please provide a value for at least one of them.",
                    validationException
                );
            }

            return validationException;
        }

        private static final ConstructingObjectParser<UpdateConnectorNameAction.Request, String> PARSER = new ConstructingObjectParser<>(
            "connector_update_name_request",
            false,
            ((args, connectorId) -> new UpdateConnectorNameAction.Request(connectorId, (String) args[0], (String) args[1]))
        );

        static {
            PARSER.declareStringOrNull(optionalConstructorArg(), Connector.NAME_FIELD);
            PARSER.declareStringOrNull(optionalConstructorArg(), Connector.DESCRIPTION_FIELD);
        }

        public static UpdateConnectorNameAction.Request fromXContentBytes(
            String connectorId,
            BytesReference source,
            XContentType xContentType
        ) {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
                return UpdateConnectorNameAction.Request.fromXContent(parser, connectorId);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
            }
        }

        public static UpdateConnectorNameAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                if (name != null) {
                    builder.field(Connector.NAME_FIELD.getPreferredName(), name);
                }
                if (description != null) {
                    builder.field(Connector.DESCRIPTION_FIELD.getPreferredName(), description);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeOptionalString(name);
            out.writeOptionalString(description);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId)
                && Objects.equals(name, request.name)
                && Objects.equals(description, request.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, name, description);
        }
    }
}
