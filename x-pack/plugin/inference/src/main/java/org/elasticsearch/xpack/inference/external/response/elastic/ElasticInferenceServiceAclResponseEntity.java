/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.elastic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
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
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ElasticInferenceServiceAclResponseEntity implements InferenceServiceResults {

    public static final String NAME = "elastic_inference_service_acl_results";
    public static final String COMPLETION = TaskType.COMPLETION.name().toLowerCase(Locale.ROOT);

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ElasticInferenceServiceAclResponseEntity, Void> PARSER = new ConstructingObjectParser<>(
        ElasticInferenceServiceAclResponseEntity.class.getSimpleName(),
        args -> new ElasticInferenceServiceAclResponseEntity((List<AllowedModel>) args[0])
    );

    static {
        PARSER.declareObjectArray(constructorArg(), AllowedModel.ALLOWED_MODEL_PARSER::apply, new ParseField("allowed_models"));
    }

    public record AllowedModel(String modelName, EnumSet<TaskType> taskTypes) implements Writeable, ToXContentObject {

        @SuppressWarnings("unchecked")
        public static ConstructingObjectParser<AllowedModel, Void> ALLOWED_MODEL_PARSER = new ConstructingObjectParser<>(
            AllowedModel.class.getSimpleName(),
            args -> new AllowedModel((String) args[0], toTaskTypes((List<String>) args[1]))
        );

        static {
            ALLOWED_MODEL_PARSER.declareString(constructorArg(), new ParseField("model_name"));
            ALLOWED_MODEL_PARSER.declareStringArray(constructorArg(), new ParseField("task_types"));
        }

        private static EnumSet<TaskType> toTaskTypes(List<String> stringTaskTypes) {
            var taskTypes = EnumSet.noneOf(TaskType.class);
            for (String taskType : stringTaskTypes) {
                taskTypes.add(TaskType.fromStringOrStatusException(taskType));
            }

            return taskTypes;
        }

        public AllowedModel(StreamInput in) throws IOException {
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

    private final List<AllowedModel> allowedModels;

    public ElasticInferenceServiceAclResponseEntity(List<AllowedModel> allowedModels) {
        this.allowedModels = Objects.requireNonNull(allowedModels);
    }

    public ElasticInferenceServiceAclResponseEntity(StreamInput in) throws IOException {
        this(in.readCollectionAsList(AllowedModel::new));
    }

    public static ElasticInferenceServiceAclResponseEntity fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return PARSER.apply(jsonParser, null);
        }
    }

    public List<AllowedModel> getAllowedModels() {
        return allowedModels;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(allowedModels);
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
    public List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Map<String, Object> asMap() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
