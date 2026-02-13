/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.metadata.EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Handles parsing the v2 authorization response from the Elastic Inference Service.
 *
 * Note: This class does not really need to be {@link InferenceServiceResults}. We do this so that we can leverage the existing
 * {@link org.elasticsearch.xpack.inference.external.http.sender.Sender} framework.
 *
 * Because of this, we don't need to register this class as a named writeable in the NamedWriteableRegistry. It will never be
 * sent over the wire between nodes.
 */
public class ElasticInferenceServiceAuthorizationResponseEntity implements InferenceServiceResults {

    public static final String NAME = "elastic_inference_service_auth_results_v2";

    private static final String INFERENCE_ENDPOINTS = "inference_endpoints";

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ElasticInferenceServiceAuthorizationResponseEntity, Void> PARSER =
        new ConstructingObjectParser<>(
            ElasticInferenceServiceAuthorizationResponseEntity.class.getSimpleName(),
            true,
            args -> new ElasticInferenceServiceAuthorizationResponseEntity((List<AuthorizedEndpoint>) args[0])
        );

    static {
        PARSER.declareObjectArray(
            constructorArg(),
            AuthorizedEndpoint.AUTHORIZED_ENDPOINT_PARSER::apply,
            new ParseField(INFERENCE_ENDPOINTS)
        );
    }

    public record AuthorizedEndpoint(
        String id,
        String modelName,
        TaskTypeObject taskType,
        String status,
        @Nullable List<String> properties,
        String releaseDate,
        @Nullable String endOfLifeDate,
        @Nullable Configuration configuration,
        @Nullable String displayName,
        @Nullable String fingerprint
    ) implements Writeable, ToXContentObject {

        public static final String RELEASE_DATE = "release_date";
        public static final String END_OF_LIFE_DATE = "end_of_life_date";

        private static final String ID = "id";
        private static final String MODEL_NAME = "model_name";
        private static final String TASK_TYPE = "task_types";
        private static final String STATUS = "status";
        private static final String PROPERTIES = "properties";
        private static final String CONFIGURATION = "configuration";
        private static final String DISPLAY_NAME = "display_name";
        private static final String FINGERPRINT = "fingerprint";

        @SuppressWarnings("unchecked")
        public static ConstructingObjectParser<AuthorizedEndpoint, Void> AUTHORIZED_ENDPOINT_PARSER = new ConstructingObjectParser<>(
            AuthorizedEndpoint.class.getSimpleName(),
            true,
            args -> new AuthorizedEndpoint(
                (String) args[0],
                (String) args[1],
                (TaskTypeObject) args[2],
                (String) args[3],
                (List<String>) args[4],
                (String) args[5],
                (String) args[6],
                (Configuration) args[7],
                (String) args[8],
                (String) args[9]
            )
        );

        static {
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(ID));
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(MODEL_NAME));
            AUTHORIZED_ENDPOINT_PARSER.declareObject(constructorArg(), TaskTypeObject.PARSER::apply, new ParseField(TASK_TYPE));
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(STATUS));
            AUTHORIZED_ENDPOINT_PARSER.declareStringArray(optionalConstructorArg(), new ParseField(PROPERTIES));
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(RELEASE_DATE));
            AUTHORIZED_ENDPOINT_PARSER.declareString(optionalConstructorArg(), new ParseField(END_OF_LIFE_DATE));
            AUTHORIZED_ENDPOINT_PARSER.declareObject(optionalConstructorArg(), Configuration.PARSER::apply, new ParseField(CONFIGURATION));
            AUTHORIZED_ENDPOINT_PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField(DISPLAY_NAME));
            AUTHORIZED_ENDPOINT_PARSER.declareString(optionalConstructorArg(), new ParseField(FINGERPRINT));
        }

        public AuthorizedEndpoint(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readString(),
                new TaskTypeObject(in),
                in.readString(),
                in.readOptionalCollectionAsList(StreamInput::readString),
                in.readString(),
                in.readOptionalString(),
                in.readOptionalWriteable(Configuration::new),
                in.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED) ? in.readOptionalString() : null,
                in.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED) ? in.readOptionalString() : null
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeString(modelName);
            taskType.writeTo(out);
            out.writeString(status);
            out.writeOptionalCollection(properties, StreamOutput::writeString);
            out.writeString(releaseDate);
            out.writeOptionalString(endOfLifeDate);
            out.writeOptionalWriteable(configuration);

            if (out.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED)) {
                out.writeOptionalString(displayName);
                out.writeOptionalString(fingerprint);
            }
        }

        @Override
        public String toString() {
            return Strings.format(
                "AuthorizedEndpoint{id='%s', modelName='%s', taskType='%s', status='%s', "
                    + "properties=%s, releaseDate='%s', endOfLifeDate='%s', configuration=%s, displayName='%s', fingerprint='%s'}",
                id,
                modelName,
                taskType,
                status,
                properties,
                releaseDate,
                endOfLifeDate,
                configuration,
                displayName,
                fingerprint
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(ID, id);
            builder.field(MODEL_NAME, modelName);
            builder.field(TASK_TYPE, taskType);
            builder.field(STATUS, status);
            if (properties != null) {
                builder.field(PROPERTIES, properties);
            }
            builder.field(RELEASE_DATE, releaseDate);
            if (endOfLifeDate != null) {
                builder.field(END_OF_LIFE_DATE, endOfLifeDate);
            }
            if (configuration != null) {
                builder.field(CONFIGURATION, configuration);
            }

            if (displayName != null) {
                builder.field(DISPLAY_NAME, displayName);
            }

            if (fingerprint != null) {
                builder.field(FINGERPRINT, fingerprint);
            }

            builder.endObject();

            return builder;
        }
    }

    public record TaskTypeObject(String eisTaskType, String elasticsearchTaskType) implements Writeable, ToXContentObject {

        private static final String EIS_TASK_TYPE_FIELD = "eis";
        private static final String ELASTICSEARCH_TASK_TYPE_FIELD = "elasticsearch";

        private static final ConstructingObjectParser<TaskTypeObject, Void> PARSER = new ConstructingObjectParser<>(
            TaskTypeObject.class.getSimpleName(),
            true,
            args -> new TaskTypeObject((String) args[0], (String) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField(EIS_TASK_TYPE_FIELD));
            PARSER.declareString(constructorArg(), new ParseField(ELASTICSEARCH_TASK_TYPE_FIELD));
        }

        public TaskTypeObject(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Override
        public String toString() {
            return Strings.format("TaskTypeObject{eisTaskType='%s', elasticsearchTaskType='%s'}", eisTaskType, elasticsearchTaskType);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(eisTaskType);
            out.writeString(elasticsearchTaskType);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(EIS_TASK_TYPE_FIELD, eisTaskType);
            builder.field(ELASTICSEARCH_TASK_TYPE_FIELD, elasticsearchTaskType);
            builder.endObject();
            return builder;
        }
    }

    public record Configuration(
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable String elementType,
        @Nullable Map<String, Object> chunkingSettings
    ) implements Writeable, ToXContentObject {

        public static final Configuration EMPTY = new Configuration(null, null, null, null);

        public static final String SIMILARITY = "similarity";
        public static final String DIMENSIONS = "dimensions";
        public static final String ELEMENT_TYPE = "element_type";
        public static final String CHUNKING_SETTINGS = "chunking_settings";

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Configuration, Void> PARSER = new ConstructingObjectParser<>(
            Configuration.class.getSimpleName(),
            true,
            args -> new Configuration((String) args[0], (Integer) args[1], (String) args[2], (Map<String, Object>) args[3])
        );

        static {
            PARSER.declareString(optionalConstructorArg(), new ParseField(SIMILARITY));
            PARSER.declareInt(optionalConstructorArg(), new ParseField(DIMENSIONS));
            PARSER.declareString(optionalConstructorArg(), new ParseField(ELEMENT_TYPE));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), new ParseField(CHUNKING_SETTINGS));
        }

        public Configuration(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalVInt(), in.readOptionalString(), in.readGenericMap());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(similarity);
            out.writeOptionalVInt(dimensions);
            out.writeOptionalString(elementType);
            out.writeGenericMap(chunkingSettings);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (similarity != null) {
                builder.field(SIMILARITY, similarity);
            }

            if (dimensions != null) {
                builder.field(DIMENSIONS, dimensions);
            }

            if (elementType != null) {
                builder.field(ELEMENT_TYPE, elementType);
            }

            if (chunkingSettings != null) {
                builder.field(CHUNKING_SETTINGS, chunkingSettings);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.format(
                "Configuration{similarity='%s', dimensions=%s, elementType='%s', chunkingSettings=%s}",
                similarity,
                dimensions,
                elementType,
                chunkingSettings
            );
        }
    }

    private final List<AuthorizedEndpoint> authorizedEndpoints;

    public ElasticInferenceServiceAuthorizationResponseEntity(List<AuthorizedEndpoint> authorizedEndpoints) {
        this.authorizedEndpoints = Objects.requireNonNull(authorizedEndpoints);
    }

    public ElasticInferenceServiceAuthorizationResponseEntity(StreamInput in) throws IOException {
        this(in.readCollectionAsList(AuthorizedEndpoint::new));
    }

    public static ElasticInferenceServiceAuthorizationResponseEntity fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return PARSER.apply(jsonParser, null);
        }
    }

    public List<AuthorizedEndpoint> getAuthorizedEndpoints() {
        return authorizedEndpoints;
    }

    @Override
    public String toString() {
        return authorizedEndpoints.stream().map(AuthorizedEndpoint::toString).collect(Collectors.joining(", "));
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(authorizedEndpoints);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Map<String, Object> asMap() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceAuthorizationResponseEntity that = (ElasticInferenceServiceAuthorizationResponseEntity) o;
        return Objects.equals(authorizedEndpoints, that.authorizedEndpoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorizedEndpoints);
    }
}
