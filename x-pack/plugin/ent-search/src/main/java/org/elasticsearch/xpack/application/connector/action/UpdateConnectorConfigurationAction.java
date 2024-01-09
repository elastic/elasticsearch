/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateConnectorConfigurationAction extends ActionType<ConnectorUpdateActionResponse> {

    public static final UpdateConnectorConfigurationAction INSTANCE = new UpdateConnectorConfigurationAction();
    public static final String NAME = "cluster:admin/xpack/connector/update_configuration";

    public UpdateConnectorConfigurationAction() {
        super(NAME, ConnectorUpdateActionResponse::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private final String connectorId;
        private final Map<String, ConnectorConfiguration> configuration;

        public Request(String connectorId, Map<String, ConnectorConfiguration> configuration) {
            this.connectorId = connectorId;
            this.configuration = configuration;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.configuration = in.readMap(ConnectorConfiguration::new);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public Map<String, ConnectorConfiguration> getConfiguration() {
            return configuration;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be null or empty.", validationException);
            }

            if (Objects.isNull(configuration)) {
                validationException = addValidationError("[configuration] cannot be null.", validationException);
            }

            return validationException;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<UpdateConnectorConfigurationAction.Request, String> PARSER =
            new ConstructingObjectParser<>(
                "connector_update_configuration_request",
                false,
                ((args, connectorId) -> new UpdateConnectorConfigurationAction.Request(
                    connectorId,
                    (Map<String, ConnectorConfiguration>) args[0]
                ))
            );

        static {
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> p.map(HashMap::new, ConnectorConfiguration::fromXContent),
                Connector.CONFIGURATION_FIELD,
                ObjectParser.ValueType.OBJECT
            );
        }

        public static UpdateConnectorConfigurationAction.Request fromXContentBytes(
            String connectorId,
            BytesReference source,
            XContentType xContentType
        ) {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
                return UpdateConnectorConfigurationAction.Request.fromXContent(parser, connectorId);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse connector configuration.", e);
            }
        }

        public static UpdateConnectorConfigurationAction.Request fromXContent(XContentParser parser, String connectorId)
            throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.CONFIGURATION_FIELD.getPreferredName(), configuration);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeMap(configuration, StreamOutput::writeWriteable);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && Objects.equals(configuration, request.configuration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, configuration);
        }
    }
}
