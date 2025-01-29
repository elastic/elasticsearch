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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateConnectorConfigurationAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_configuration";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorConfigurationAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final Map<String, ConnectorConfiguration> configuration;
        private final Map<String, Object> configurationValues;

        private static final ParseField VALUES_FIELD = new ParseField("values");

        public Request(String connectorId, Map<String, ConnectorConfiguration> configuration, Map<String, Object> configurationValues) {
            this.connectorId = connectorId;
            this.configuration = configuration;
            this.configurationValues = configurationValues;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.configuration = in.readMap(ConnectorConfiguration::new);
            this.configurationValues = in.readGenericMap();
        }

        public String getConnectorId() {
            return connectorId;
        }

        public Map<String, ConnectorConfiguration> getConfiguration() {
            return configuration;
        }

        public Map<String, Map<String, Object>> getConfigurationAsMap() {
            return configuration.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toMap()));
        }

        public Map<String, Object> getConfigurationValues() {
            return configurationValues;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            if (configuration == null && configurationValues == null) {
                validationException = addValidationError("[configuration] and [values] cannot both be null.", validationException);
            }

            if (configuration != null && configurationValues != null) {
                validationException = addValidationError(
                    "[configuration] and [values] cannot both be provided in the same request.",
                    validationException
                );
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
                    (Map<String, ConnectorConfiguration>) args[0],
                    (Map<String, Object>) args[1]
                ))
            );

        static {
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> p.map(HashMap::new, ConnectorConfiguration::fromXContent),
                Connector.CONFIGURATION_FIELD,
                ObjectParser.ValueType.OBJECT
            );
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.map(), VALUES_FIELD, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
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
                builder.field(VALUES_FIELD.getPreferredName(), configurationValues);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeMap(configuration, StreamOutput::writeWriteable);
            out.writeGenericMap(configurationValues);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId)
                && Objects.equals(configuration, request.configuration)
                && Objects.equals(configurationValues, request.configurationValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, configuration, configurationValues);
        }
    }
}
