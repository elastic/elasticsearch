/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
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

public class ElasticInferenceServiceAuthorizationResponseEntity implements InferenceServiceResults {

    public static final String NAME = "elastic_inference_service_auth_results";
    private static final Map<String, TaskType> ELASTIC_INFERENCE_SERVICE_TASK_TYPE_MAPPING = Map.of(
        "embed/text/sparse",
        TaskType.SPARSE_EMBEDDING,
        "chat",
        TaskType.CHAT_COMPLETION
    );

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ElasticInferenceServiceAuthorizationResponseEntity, Void> PARSER =
        new ConstructingObjectParser<>(
            ElasticInferenceServiceAuthorizationResponseEntity.class.getSimpleName(),
            args -> new ElasticInferenceServiceAuthorizationResponseEntity((List<AuthorizedModel>) args[0])
        );

    static {
        PARSER.declareObjectArray(constructorArg(), AuthorizedModel.AUTHORIZED_MODEL_PARSER::apply, new ParseField("models"));
    }

    public record AuthorizedModel(String modelName, EnumSet<TaskType> taskTypes) implements Writeable, ToXContentObject {

        @SuppressWarnings("unchecked")
        public static ConstructingObjectParser<AuthorizedModel, Void> AUTHORIZED_MODEL_PARSER = new ConstructingObjectParser<>(
            AuthorizedModel.class.getSimpleName(),
            args -> new AuthorizedModel((String) args[0], toTaskTypes((List<String>) args[1]))
        );

        static {
            AUTHORIZED_MODEL_PARSER.declareString(constructorArg(), new ParseField("model_name"));
            AUTHORIZED_MODEL_PARSER.declareStringArray(constructorArg(), new ParseField("task_types"));
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

        public AuthorizedModel(StreamInput in) throws IOException {
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

            builder.field("model_name", modelName);
            builder.field("task_types", taskTypes.stream().map(TaskType::toString).collect(Collectors.toList()));

            builder.endObject();

            return builder;
        }
    }

    private final List<AuthorizedModel> authorizedModels;

    public ElasticInferenceServiceAuthorizationResponseEntity(List<AuthorizedModel> authorizedModels) {
        this.authorizedModels = Objects.requireNonNull(authorizedModels);
    }

    /**
     * Create an empty response
     */
    public ElasticInferenceServiceAuthorizationResponseEntity() {
        this(List.of());
    }

    public ElasticInferenceServiceAuthorizationResponseEntity(StreamInput in) throws IOException {
        this(in.readCollectionAsList(AuthorizedModel::new));
    }

    public static ElasticInferenceServiceAuthorizationResponseEntity fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return PARSER.apply(jsonParser, null);
        }
    }

    public List<AuthorizedModel> getAuthorizedModels() {
        return authorizedModels;
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
        ElasticInferenceServiceAuthorizationResponseEntity that = (ElasticInferenceServiceAuthorizationResponseEntity) o;
        return Objects.equals(authorizedModels, that.authorizedModels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorizedModels);
    }
}
