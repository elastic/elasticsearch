/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;

/**
 * Defines the base settings required to configure an inference endpoint.
 *
 * These settings are immutable and describe the input and output types that the endpoint will handle.
 * They capture the essential properties of an inference model, ensuring the endpoint is correctly configured.
 *
 * Key properties include:
 * <ul>
 *   <li>{@code taskType} - Specifies the type of task the model performs, such as classification or text embeddings.</li>
 *   <li>{@code dimensions}, {@code similarity}, and {@code elementType} - These settings are applicable only when
 *       the {@code taskType} is {@link TaskType#TEXT_EMBEDDING}. They define the structure and behavior of embeddings.</li>
 * </ul>
 *
 * @param taskType the type of task the inference model performs.
 * @param dimensions the number of dimensions for the embeddings, applicable only for {@link TaskType#TEXT_EMBEDDING} (nullable).
 * @param similarity the similarity measure used for embeddings, applicable only for {@link TaskType#TEXT_EMBEDDING} (nullable).
 * @param elementType the type of elements in the embeddings, applicable only for {@link TaskType#TEXT_EMBEDDING} (nullable).
 */
public record MinimalServiceSettings(
    TaskType taskType,
    @Nullable Integer dimensions,
    @Nullable SimilarityMeasure similarity,
    @Nullable ElementType elementType
) implements ToXContentObject {

    public static final String TASK_TYPE_FIELD = "task_type";
    static final String DIMENSIONS_FIELD = "dimensions";
    static final String SIMILARITY_FIELD = "similarity";
    static final String ELEMENT_TYPE_FIELD = "element_type";

    private static final ConstructingObjectParser<MinimalServiceSettings, Void> PARSER = new ConstructingObjectParser<>(
        "model_settings",
        true,
        args -> {
            TaskType taskType = TaskType.fromString((String) args[0]);
            Integer dimensions = (Integer) args[1];
            SimilarityMeasure similarity = args[2] == null ? null : SimilarityMeasure.fromString((String) args[2]);
            DenseVectorFieldMapper.ElementType elementType = args[3] == null
                ? null
                : DenseVectorFieldMapper.ElementType.fromString((String) args[3]);
            return new MinimalServiceSettings(taskType, dimensions, similarity, elementType);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(TASK_TYPE_FIELD));
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(DIMENSIONS_FIELD));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(SIMILARITY_FIELD));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ELEMENT_TYPE_FIELD));
    }

    public static MinimalServiceSettings parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static MinimalServiceSettings textEmbedding(int dimensions, SimilarityMeasure similarity, ElementType elementType) {
        return new MinimalServiceSettings(TEXT_EMBEDDING, dimensions, similarity, elementType);
    }

    public static MinimalServiceSettings sparseEmbedding() {
        return new MinimalServiceSettings(SPARSE_EMBEDDING, null, null, null);
    }

    public static MinimalServiceSettings rerank() {
        return new MinimalServiceSettings(RERANK, null, null, null);
    }

    public static MinimalServiceSettings completion() {
        return new MinimalServiceSettings(COMPLETION, null, null, null);
    }

    public static MinimalServiceSettings chatCompletion() {
        return new MinimalServiceSettings(CHAT_COMPLETION, null, null, null);
    }

    public MinimalServiceSettings(Model model) {
        this(
            model.getTaskType(),
            model.getServiceSettings().dimensions(),
            model.getServiceSettings().similarity(),
            model.getServiceSettings().elementType()
        );
    }

    public MinimalServiceSettings(
        TaskType taskType,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable ElementType elementType
    ) {
        this.taskType = Objects.requireNonNull(taskType, "task type must not be null");
        this.dimensions = dimensions;
        this.similarity = similarity;
        this.elementType = elementType;
        validate();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_TYPE_FIELD, taskType.toString());
        if (dimensions != null) {
            builder.field(DIMENSIONS_FIELD, dimensions);
        }
        if (similarity != null) {
            builder.field(SIMILARITY_FIELD, similarity);
        }
        if (elementType != null) {
            builder.field(ELEMENT_TYPE_FIELD, elementType);
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
        if (elementType != null) {
            sb.append(", element_type=").append(elementType);
        }
        return sb.toString();
    }

    private void validate() {
        switch (taskType) {
            case TEXT_EMBEDDING:
                validateFieldPresent(DIMENSIONS_FIELD, dimensions);
                validateFieldPresent(SIMILARITY_FIELD, similarity);
                validateFieldPresent(ELEMENT_TYPE_FIELD, elementType);
                break;

            default:
                validateFieldNotPresent(DIMENSIONS_FIELD, dimensions);
                validateFieldNotPresent(SIMILARITY_FIELD, similarity);
                validateFieldNotPresent(ELEMENT_TYPE_FIELD, elementType);
                break;
        }
    }

    private void validateFieldPresent(String field, Object fieldValue) {
        if (fieldValue == null) {
            throw new IllegalArgumentException("required [" + field + "] field is missing for task_type [" + taskType.name() + "]");
        }
    }

    private void validateFieldNotPresent(String field, Object fieldValue) {
        if (fieldValue != null) {
            throw new IllegalArgumentException("[" + field + "] is not allowed for task_type [" + taskType.name() + "]");
        }
    }
}
