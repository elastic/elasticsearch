/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.apache.http.auth.AUTH;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/*

{
  "inference_endpoints": [
    {
      "id": ".rainbow-sprinkles-elastic",
      "model_name": "rainbow-sprinkles",
      "task_type": "chat",
      "status": "ga",
      "properties": [
        "multilingual"
      ],
      "release_date": "2024-05-01",
      "end_of_life_date": "2025-12-31"
    },
    {
      "id": ".elastic-elser-v2",
      "model_name": "elser_model_2",
      "task_type": "embed/text/sparse",
      "status": "preview",
      "properties": [
        "english"
      ],
      "release_date": "2024-05-01",
      "configuration": {
        "chunking_settings": {
          "strategy": "sentence",
          "max_chunk_size": 250,
          "sentence_overlap": 1
        }
      }
    },
    {
      "id": ".jina-embeddings-v3",
      "model_name": "jina-embeddings-v3",
      "task_type": "embed/text/dense",
      "status": "beta",
      "properties": [
        "multilingual",
        "open-weights"
      ],
      "release_date": "2024-05-01",
      "configuration": {
        "similarity": "cosine",
        "dimension": 1024,
        "element_type": "float",
        "chunking_settings": {
          "strategy": "sentence",
          "max_chunk_size": 250,
          "sentence_overlap": 1
        }
      }
    }
  ]
}
 */
public class ElasticInferenceServiceAuthorizationResponseEntityV2 implements InferenceServiceResults {

    public static final String NAME = "elastic_inference_service_auth_results_v2";

    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceAuthorizationResponseEntityV2.class);
    private static final String AUTH_FIELD_NAME = "authorized_models";
    private static final Map<String, TaskType> ELASTIC_INFERENCE_SERVICE_TASK_TYPE_MAPPING = Map.of(
        "embed/text/sparse",
        TaskType.SPARSE_EMBEDDING,
        "chat",
        TaskType.CHAT_COMPLETION,
        "embed/text/dense",
        TaskType.TEXT_EMBEDDING,
        "rerank/text/text-similarity",
        TaskType.RERANK
    );

    private static final String INFERENCE_ENDPOINTS = "inference_endpoints";

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ElasticInferenceServiceAuthorizationResponseEntityV2, Void> PARSER =
        new ConstructingObjectParser<>(
            ElasticInferenceServiceAuthorizationResponseEntityV2.class.getSimpleName(),
            true,
            args -> new ElasticInferenceServiceAuthorizationResponseEntityV2((List<AuthorizedEndpoint>) args[0])
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
        String taskType,
        String status,
        @Nullable List<String> properties,
        String releaseDate,
        @Nullable Configuration configuration
    ) implements Writeable, ToXContentObject {

        private static final String ID = "id";
        private static final String MODEL_NAME = "model_name";
        private static final String TASK_TYPE = "task_type";
        private static final String STATUS = "status";
        private static final String PROPERTIES = "properties";
        private static final String RELEASE_DATE = "release_date";
        private static final String CONFIGURATION = "configuration";

        @SuppressWarnings("unchecked")
        public static ConstructingObjectParser<AuthorizedEndpoint, Void> AUTHORIZED_ENDPOINT_PARSER = new ConstructingObjectParser<>(
            AuthorizedEndpoint.class.getSimpleName(),
            true,
            args -> new AuthorizedEndpoint(
                (String) args[0],
                (String) args[1],
                (String) args[2],
                (String) args[3],
                (List<String>) args[4],
                (String) args[5],
                (Configuration) args[6]
            )
        );

        static {
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(ID));
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(MODEL_NAME));
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(TASK_TYPE));
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(STATUS));
            AUTHORIZED_ENDPOINT_PARSER.declareStringArray(optionalConstructorArg(), new ParseField(PROPERTIES));
            AUTHORIZED_ENDPOINT_PARSER.declareString(constructorArg(), new ParseField(RELEASE_DATE));
            AUTHORIZED_ENDPOINT_PARSER.declareObject(
                optionalConstructorArg(),
                Configuration.CONFIGURATION_PARSER::apply,
                new ParseField(CONFIGURATION)
            );
        }

        private static EnumSet<TaskType> toTaskTypes(List<String> stringTaskTypes) {
            var taskTypes = EnumSet.noneOf(TaskType.class);
            for (String taskType : stringTaskTypes) {
                var mappedTaskType = ELASTIC_INFERENCE_SERVICE_TASK_TYPE_MAPPING.get(taskType);
                if (mappedTaskType != null) {
                    taskTypes.add(mappedTaskType);
                }
            }

            return taskTypes;
        }

        public AuthorizedEndpoint(StreamInput in) throws IOException {
            this(in.readString(), in.readEnumSet(TaskType.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(modelName);
            out.writeEnumSet(taskTypes);
        }

    @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(ID, )

            builder.field("model_name", modelName);
            builder.field("task_types", taskTypes.stream().map(TaskType::toString).collect(Collectors.toList()));

            builder.endObject();

            return builder;
        }

        @Override
        public String toString() {
            return Strings.format("{modelName='%s', taskTypes='%s'}", modelName, taskTypes);
        }
    }

    public record Configuration(
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable String elementType,
        @Nullable Map<String, Object> chunkingSettings
    ) implements Writeable, ToXContentObject {

        private static final String SIMILARITY = "similarity";
        private static final String DIMENSIONS = "dimensions";
        private static final String ELEMENT_TYPE = "element_type";
        private static final String CHUNKING_SETTINGS = "chunking_settings";

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Configuration, Void> CONFIGURATION_PARSER = new ConstructingObjectParser<>(
            Configuration.class.getSimpleName(),
            true,
            args -> new Configuration((String) args[0], (Integer) args[1], (String) args[2], (Map<String, Object>) args[3])
        );

        static {
            CONFIGURATION_PARSER.declareString(optionalConstructorArg(), new ParseField(SIMILARITY));
            CONFIGURATION_PARSER.declareInt(optionalConstructorArg(), new ParseField(DIMENSIONS));
            CONFIGURATION_PARSER.declareString(optionalConstructorArg(), new ParseField(ELEMENT_TYPE));
            CONFIGURATION_PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), new ParseField(CHUNKING_SETTINGS));
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
    }

    private final List<AuthorizedEndpoint> authorizedModels;

    public ElasticInferenceServiceAuthorizationResponseEntityV2(List<AuthorizedEndpoint> authorizedModels) {
        this.authorizedModels = Objects.requireNonNull(authorizedModels);
    }

    /**
     * Create an empty response
     */
    public ElasticInferenceServiceAuthorizationResponseEntityV2() {
        this(List.of());
    }

    public ElasticInferenceServiceAuthorizationResponseEntityV2(StreamInput in) throws IOException {
        this(in.readCollectionAsList(AuthorizedEndpoint::new));
    }

    public static ElasticInferenceServiceAuthorizationResponseEntityV2 fromResponse(Request request, HttpResult response)
        throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return PARSER.apply(jsonParser, null);
        }
    }

    public List<AuthorizedEndpoint> getAuthorizedModels() {
        return authorizedModels;
    }

    @Override
    public String toString() {
        return authorizedModels.stream().map(AuthorizedEndpoint::toString).collect(Collectors.joining(", "));
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(authorizedModels);
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
        ElasticInferenceServiceAuthorizationResponseEntityV2 that = (ElasticInferenceServiceAuthorizationResponseEntityV2) o;
        return Objects.equals(authorizedModels, that.authorizedModels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorizedModels);
    }
}
