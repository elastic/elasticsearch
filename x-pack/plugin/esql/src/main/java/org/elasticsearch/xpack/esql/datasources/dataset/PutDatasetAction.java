/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
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
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/** Create or replace an ES|QL dataset. */
public class PutDatasetAction extends ActionType<AcknowledgedResponse> {

    public static final PutDatasetAction INSTANCE = new PutDatasetAction();
    public static final String NAME = EsqlDatasetActionNames.ESQL_PUT_DATASET_ACTION_NAME;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).build())
        .build();

    private PutDatasetAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {
        private static final ParseField DATA_SOURCE = new ParseField("data_source");
        private static final ParseField RESOURCE = new ParseField("resource");
        private static final ParseField DESCRIPTION = new ParseField("description");
        private static final ParseField SETTINGS = new ParseField("settings");

        public record ParseContext(String name, TimeValue masterNodeTimeout, TimeValue ackTimeout) {}

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, ParseContext> PARSER = new ConstructingObjectParser<>(
            "esql_put_dataset",
            false,
            (args, ctx) -> new Request(
                ctx.masterNodeTimeout(),
                ctx.ackTimeout(),
                ctx.name(),
                (String) args[0],
                (String) args[1],
                (String) args[2],
                (Map<String, Object>) args[3]
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), DATA_SOURCE);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), RESOURCE);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), SETTINGS);
        }

        public static Request fromXContent(XContentParser parser, TimeValue masterNodeTimeout, TimeValue ackTimeout, String name)
            throws IOException {
            return PARSER.parse(parser, new ParseContext(name, masterNodeTimeout, ackTimeout));
        }

        private final String name;
        private final String dataSource;
        private final String resource;
        @Nullable
        private final String description;
        private final Map<String, Object> rawSettings;

        public Request(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            String name,
            String dataSource,
            String resource,
            @Nullable String description,
            Map<String, Object> rawSettings
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
            this.dataSource = dataSource;
            this.resource = resource;
            this.description = description;
            this.rawSettings = rawSettings == null ? Map.of() : rawSettings;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.dataSource = in.readString();
            this.resource = in.readString();
            this.description = in.readOptionalString();
            this.rawSettings = in.readGenericMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(dataSource);
            out.writeString(resource);
            out.writeOptionalString(description);
            out.writeGenericMap(rawSettings);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(name) == false) {
                return addValidationError("dataset name is missing", null);
            }
            // Dataset names participate in the index namespace (Type.DATASET) so they must follow index/alias name rules.
            try {
                MetadataCreateIndexService.validateIndexOrAliasName(
                    name,
                    (datasetName, error) -> new IllegalArgumentException("invalid dataset name [" + datasetName + "], " + error)
                );
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(e.getMessage(), validationException);
            }
            if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
                validationException = addValidationError("invalid dataset name [" + name + "], must be lowercase", validationException);
            }
            if (Strings.hasText(dataSource) == false) {
                validationException = addValidationError("dataset data_source is missing or empty", validationException);
            }
            if (Strings.hasText(resource) == false) {
                validationException = addValidationError("dataset resource is missing or empty", validationException);
            }
            return validationException;
        }

        public String name() {
            return name;
        }

        public String dataSource() {
            return dataSource;
        }

        public String resource() {
            return resource;
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
                && Objects.equals(dataSource, request.dataSource)
                && Objects.equals(resource, request.resource)
                && Objects.equals(description, request.description)
                && Objects.equals(rawSettings, request.rawSettings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, dataSource, resource, description, rawSettings);
        }

        @Override
        public String[] indices() {
            return new String[] { name };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return DEFAULT_INDICES_OPTIONS;
        }
    }
}
