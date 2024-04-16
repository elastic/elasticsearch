/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A {@link ToXContentObject} that is used to represent the transformation of the semantic text field's inputs.
 * The resulting object preserves the original input under the {@link SemanticTextField#TEXT_FIELD} and exposes
 * the inference results under the {@link SemanticTextField#INFERENCE_FIELD}.
 *
 * @param fieldName The original field name.
 * @param originalValues The original values associated with the field name.
 * @param inference The inference result.
 * @param contentType The {@link XContentType} used to store the embeddings chunks.
 */
public record SemanticTextField(String fieldName, List<String> originalValues, InferenceResult inference, XContentType contentType)
    implements
        ToXContentObject {

    static final ParseField TEXT_FIELD = new ParseField("text");
    static final ParseField INFERENCE_FIELD = new ParseField("inference");
    static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    static final ParseField CHUNKS_FIELD = new ParseField("chunks");
    static final ParseField CHUNKED_EMBEDDINGS_FIELD = new ParseField("embeddings");
    static final ParseField CHUNKED_TEXT_FIELD = new ParseField("text");
    static final ParseField MODEL_SETTINGS_FIELD = new ParseField("model_settings");
    static final ParseField TASK_TYPE_FIELD = new ParseField("task_type");
    static final ParseField DIMENSIONS_FIELD = new ParseField("dimensions");
    static final ParseField SIMILARITY_FIELD = new ParseField("similarity");

    public record InferenceResult(String inferenceId, ModelSettings modelSettings, List<Chunk> chunks) {}

    public record Chunk(String text, BytesReference rawEmbeddings) {}

    public record ModelSettings(TaskType taskType, Integer dimensions, SimilarityMeasure similarity) implements ToXContentObject {
        public ModelSettings(Model model) {
            this(model.getTaskType(), model.getServiceSettings().dimensions(), model.getServiceSettings().similarity());
        }

        public ModelSettings(TaskType taskType, Integer dimensions, SimilarityMeasure similarity) {
            this.taskType = Objects.requireNonNull(taskType, "task type must not be null");
            this.dimensions = dimensions;
            this.similarity = similarity;
            validate();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TASK_TYPE_FIELD.getPreferredName(), taskType.toString());
            if (dimensions != null) {
                builder.field(DIMENSIONS_FIELD.getPreferredName(), dimensions);
            }
            if (similarity != null) {
                builder.field(SIMILARITY_FIELD.getPreferredName(), similarity);
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("task_type=").append(taskType);
            if (dimensions != null) {
                sb.append(", dimensions=").append(dimensions);
            }
            if (similarity != null) {
                sb.append(", similarity=").append(similarity);
            }
            return sb.toString();
        }

        private void validate() {
            switch (taskType) {
                case TEXT_EMBEDDING:
                    if (dimensions == null) {
                        throw new IllegalArgumentException(
                            "required [" + DIMENSIONS_FIELD + "] field is missing for task_type [" + taskType.name() + "]"
                        );
                    }
                    if (similarity == null) {
                        throw new IllegalArgumentException(
                            "required [" + SIMILARITY_FIELD + "] field is missing for task_type [" + taskType.name() + "]"
                        );
                    }
                    break;
                case SPARSE_EMBEDDING:
                    break;

                default:
                    throw new IllegalArgumentException(
                        "Wrong ["
                            + TASK_TYPE_FIELD.getPreferredName()
                            + "], expected "
                            + TEXT_EMBEDDING
                            + " or "
                            + SPARSE_EMBEDDING
                            + ", got "
                            + taskType.name()
                    );
            }
        }
    }

    public static String getOriginalTextFieldName(String fieldName) {
        return fieldName + "." + TEXT_FIELD.getPreferredName();
    }

    public static String getInferenceFieldName(String fieldName) {
        return fieldName + "." + INFERENCE_FIELD.getPreferredName();
    }

    public static String getChunksFieldName(String fieldName) {
        return getInferenceFieldName(fieldName) + "." + CHUNKS_FIELD.getPreferredName();
    }

    public static String getEmbeddingsFieldName(String fieldName) {
        return getChunksFieldName(fieldName) + "." + CHUNKED_EMBEDDINGS_FIELD.getPreferredName();
    }

    static SemanticTextField parse(XContentParser parser, Tuple<String, XContentType> context) throws IOException {
        return SEMANTIC_TEXT_FIELD_PARSER.parse(parser, context);
    }

    static ModelSettings parseModelSettings(XContentParser parser) throws IOException {
        return MODEL_SETTINGS_PARSER.parse(parser, null);
    }

    static ModelSettings parseModelSettingsFromMap(Object node) {
        if (node == null) {
            return null;
        }
        try {
            Map<String, Object> map = XContentMapValues.nodeMapValue(node, MODEL_SETTINGS_FIELD.getPreferredName());
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                map,
                XContentType.JSON
            );
            return parseModelSettings(parser);
        } catch (Exception exc) {
            throw new ElasticsearchException(exc);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (originalValues.isEmpty() == false) {
            builder.field(TEXT_FIELD.getPreferredName(), originalValues.size() == 1 ? originalValues.get(0) : originalValues);
        }
        builder.startObject(INFERENCE_FIELD.getPreferredName());
        builder.field(INFERENCE_ID_FIELD.getPreferredName(), inference.inferenceId);
        builder.field(MODEL_SETTINGS_FIELD.getPreferredName(), inference.modelSettings);
        builder.startArray(CHUNKS_FIELD.getPreferredName());
        for (var chunk : inference.chunks) {
            builder.startObject();
            builder.field(CHUNKED_TEXT_FIELD.getPreferredName(), chunk.text);
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                chunk.rawEmbeddings,
                contentType
            );
            builder.field(CHUNKED_EMBEDDINGS_FIELD.getPreferredName()).copyCurrentStructure(parser);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SemanticTextField, Tuple<String, XContentType>> SEMANTIC_TEXT_FIELD_PARSER =
        new ConstructingObjectParser<>(
            SemanticTextFieldMapper.CONTENT_TYPE,
            true,
            (args, context) -> new SemanticTextField(
                context.v1(),
                (List<String>) (args[0] == null ? List.of() : args[0]),
                (InferenceResult) args[1],
                context.v2()
            )
        );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceResult, Void> INFERENCE_RESULT_PARSER = new ConstructingObjectParser<>(
        INFERENCE_FIELD.getPreferredName(),
        true,
        args -> new InferenceResult((String) args[0], (ModelSettings) args[1], (List<Chunk>) args[2])
    );

    private static final ConstructingObjectParser<Chunk, Void> CHUNKS_PARSER = new ConstructingObjectParser<>(
        CHUNKS_FIELD.getPreferredName(),
        true,
        args -> new Chunk((String) args[0], (BytesReference) args[1])
    );

    private static final ConstructingObjectParser<ModelSettings, Void> MODEL_SETTINGS_PARSER = new ConstructingObjectParser<>(
        MODEL_SETTINGS_FIELD.getPreferredName(),
        true,
        args -> {
            TaskType taskType = TaskType.fromString((String) args[0]);
            Integer dimensions = (Integer) args[1];
            SimilarityMeasure similarity = args[2] == null ? null : SimilarityMeasure.fromString((String) args[2]);
            return new ModelSettings(taskType, dimensions, similarity);
        }
    );

    static {
        SEMANTIC_TEXT_FIELD_PARSER.declareStringArray(optionalConstructorArg(), TEXT_FIELD);
        SEMANTIC_TEXT_FIELD_PARSER.declareObject(constructorArg(), (p, c) -> INFERENCE_RESULT_PARSER.parse(p, null), INFERENCE_FIELD);

        INFERENCE_RESULT_PARSER.declareString(constructorArg(), INFERENCE_ID_FIELD);
        INFERENCE_RESULT_PARSER.declareObject(constructorArg(), (p, c) -> MODEL_SETTINGS_PARSER.parse(p, c), MODEL_SETTINGS_FIELD);
        INFERENCE_RESULT_PARSER.declareObjectArray(constructorArg(), (p, c) -> CHUNKS_PARSER.parse(p, c), CHUNKS_FIELD);

        CHUNKS_PARSER.declareString(constructorArg(), CHUNKED_TEXT_FIELD);
        CHUNKS_PARSER.declareField(constructorArg(), (p, c) -> {
            XContentBuilder b = XContentBuilder.builder(p.contentType().xContent());
            b.copyCurrentStructure(p);
            return BytesReference.bytes(b);
        }, CHUNKED_EMBEDDINGS_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);

        MODEL_SETTINGS_PARSER.declareString(ConstructingObjectParser.constructorArg(), TASK_TYPE_FIELD);
        MODEL_SETTINGS_PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), DIMENSIONS_FIELD);
        MODEL_SETTINGS_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SIMILARITY_FIELD);
    }
}
