/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Handles parsing the v2 authorization response from the Elastic Inference Service.
 * <p>
 * Note: This class does not really need to be {@link InferenceServiceResults}. We do this so that we can leverage the existing
 * {@link Sender} framework.
 * <p>
 * Because of this, we don't need to register this class as a named writeable in the NamedWriteableRegistry. It will never be
 * sent over the wire between nodes.
 */
public record ElasticInferenceServiceAuthorizationResponseEntity(List<AuthorizedEndpoint> authorizedEndpoints)
    implements
        InferenceServiceResults {

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
    ) {

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
    }

    public record TaskTypeObject(String eisTaskType, String elasticsearchTaskType) {

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
    }

    public record Configuration(
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable String elementType,
        @Nullable Map<String, Object> chunkingSettings
    ) {

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
    }

    public ElasticInferenceServiceAuthorizationResponseEntity(List<AuthorizedEndpoint> authorizedEndpoints) {
        this.authorizedEndpoints = Objects.requireNonNull(authorizedEndpoints);
    }

    public static ElasticInferenceServiceAuthorizationResponseEntity fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return PARSER.apply(jsonParser, null);
        }
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
        throw new UnsupportedOperationException("EIS authorization entity does not support serialization");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("EIS authorization entity does not support serialization");
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Map<String, Object> asMap() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
