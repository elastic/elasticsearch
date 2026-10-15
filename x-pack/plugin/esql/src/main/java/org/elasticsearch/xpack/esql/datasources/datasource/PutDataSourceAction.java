/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;
import org.elasticsearch.xpack.core.security.audit.SecurityConfigChangeRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.esql.datasources.DataSourceLimits.MAX_DESCRIPTION_LENGTH;

/** Create or replace an ES|QL data source. */
public class PutDataSourceAction extends ActionType<AcknowledgedResponse> {

    public static final PutDataSourceAction INSTANCE = new PutDataSourceAction();
    public static final String NAME = EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME;
    public static final TransportVersion FEDERATED_DATA_SECRET_KEYS = TransportVersion.fromName("federated_data_secret_keys");

    private PutDataSourceAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements DataSourceRequestInfo, SecurityConfigChangeRequest {
        private static final ParseField TYPE = new ParseField("type");
        private static final ParseField DESCRIPTION = new ParseField("description");
        private static final ParseField SETTINGS = new ParseField("settings");

        public record ParseContext(String name, TimeValue masterNodeTimeout, TimeValue ackTimeout, Set<String> secretSettingNames) {}

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, ParseContext> PARSER = new ConstructingObjectParser<>(
            "esql_put_data_source",
            false,
            (args, ctx) -> new Request(
                ctx.masterNodeTimeout(),
                ctx.ackTimeout(),
                ctx.name(),
                (String) args[0],
                (String) args[1],
                (Map<String, Object>) args[2],
                ctx.secretSettingNames()
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), SETTINGS);
        }

        public static Request fromXContent(
            XContentParser parser,
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            String name,
            Set<String> secretSettingNames
        ) throws IOException {
            return PARSER.parse(parser, new ParseContext(name, masterNodeTimeout, ackTimeout, secretSettingNames));
        }

        private final String name;
        private final String type;
        @Nullable
        private final String description;
        private final Map<String, Object> rawSettings;
        private final Set<String> secretSettingNames;

        public Request(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            String name,
            String type,
            @Nullable String description,
            Map<String, Object> rawSettings,
            Set<String> secretSettingNames
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
            this.type = type;
            this.description = description;
            this.rawSettings = rawSettings == null ? Map.of() : rawSettings;
            this.secretSettingNames = secretSettingNames;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.type = in.readString();
            this.description = in.readOptionalString();
            this.rawSettings = in.readGenericMap();
            if (in.getTransportVersion().supports(FEDERATED_DATA_SECRET_KEYS)) {
                this.secretSettingNames = in.readOptional(i -> i.readCollectionAsSet(StreamInput::readString));
            } else {
                this.secretSettingNames = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(type);
            out.writeOptionalString(description);
            out.writeGenericMap(rawSettings);
            if (out.getTransportVersion().supports(FEDERATED_DATA_SECRET_KEYS)) {
                out.writeOptionalStringCollection(secretSettingNames);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(name) == false) {
                return addValidationError("data source name is missing", null);
            }
            try {
                MetadataCreateIndexService.validateIndexOrAliasName(
                    name,
                    (dataSourceName, error) -> new IllegalArgumentException("invalid data source name [" + dataSourceName + "], " + error)
                );
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(e.getMessage(), validationException);
            }
            if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
                validationException = addValidationError("invalid data source name [" + name + "], must be lowercase", validationException);
            }
            if (Strings.hasText(type) == false) {
                validationException = addValidationError("data source type is missing or empty", validationException);
            } else if (type.toLowerCase(Locale.ROOT).equals(type) == false) {
                validationException = addValidationError("invalid data source type [" + type + "], must be lowercase", validationException);
            } else if (type.chars().anyMatch(Character::isWhitespace)) {
                validationException = addValidationError(
                    "invalid data source type [" + type + "], must not contain whitespace",
                    validationException
                );
            }
            if (description != null && description.length() > MAX_DESCRIPTION_LENGTH) {
                validationException = addValidationError(
                    "data source description is too large: "
                        + description.length()
                        + " characters, the maximum allowed is "
                        + MAX_DESCRIPTION_LENGTH,
                    validationException
                );
            }
            return validationException;
        }

        public String name() {
            return name;
        }

        @Override
        public String[] dataSourceNames() {
            return new String[] { name };
        }

        @Override
        public String dataSourceClusterActionName() {
            return NAME;
        }

        public String type() {
            return type;
        }

        @Nullable
        public String description() {
            return description;
        }

        public Map<String, Object> rawSettings() {
            return rawSettings;
        }

        /* package private */ Set<String> secretSettingNames() {
            return secretSettingNames;
        }

        @Override
        public Map<String, Object> auditFields() {
            var fields = new HashMap<String, Object>();
            fields.put("type", type);
            fields.put("name", name);
            if (description != null) {
                fields.put("description", description);
            }
            if (secretSettingNames != null) {
                for (var entry : rawSettings.entrySet()) {
                    if (secretSettingNames.contains(entry.getKey()) == false) {
                        fields.put("settings." + entry.getKey(), entry.getValue());
                    } else {
                        fields.put("settings." + entry.getKey(), "::es_redacted::");
                    }
                }
            }
            return fields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name)
                && Objects.equals(type, request.type)
                && Objects.equals(description, request.description)
                && Objects.equals(rawSettings, request.rawSettings)
                && Objects.equals(secretSettingNames, request.secretSettingNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, description, rawSettings, secretSettingNames);
        }
    }
}
