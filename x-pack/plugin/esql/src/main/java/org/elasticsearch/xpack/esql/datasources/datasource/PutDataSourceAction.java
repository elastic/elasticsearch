/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

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
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/** Create or replace an ES|QL data source. */
public class PutDataSourceAction extends ActionType<AcknowledgedResponse> {

    public static final PutDataSourceAction INSTANCE = new PutDataSourceAction();
    public static final String NAME = EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME;

    private PutDataSourceAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private static final ParseField TYPE = new ParseField("type");
        private static final ParseField DESCRIPTION = new ParseField("description");
        private static final ParseField SETTINGS = new ParseField("settings");

        public record ParseContext(String name, TimeValue masterNodeTimeout, TimeValue ackTimeout) {}

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
                (Map<String, Object>) args[2]
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), SETTINGS);
        }

        public static Request fromXContent(XContentParser parser, TimeValue masterNodeTimeout, TimeValue ackTimeout, String name)
            throws IOException {
            return PARSER.parse(parser, new ParseContext(name, masterNodeTimeout, ackTimeout));
        }

        private final String name;
        private final String type;
        @Nullable
        private final String description;
        private final Map<String, Object> rawSettings;

        public Request(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            String name,
            String type,
            @Nullable String description,
            Map<String, Object> rawSettings
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
            this.type = type;
            this.description = description;
            this.rawSettings = rawSettings == null ? Map.of() : rawSettings;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.type = in.readString();
            this.description = in.readOptionalString();
            this.rawSettings = in.readGenericMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(type);
            out.writeOptionalString(description);
            out.writeGenericMap(rawSettings);
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
            return validationException;
        }

        public String name() {
            return name;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name)
                && Objects.equals(type, request.type)
                && Objects.equals(description, request.description)
                && Objects.equals(rawSettings, request.rawSettings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, description, rawSettings);
        }
    }
}
