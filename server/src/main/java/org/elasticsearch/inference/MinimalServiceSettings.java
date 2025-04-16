/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.INFERENCE_MODEL_REGISTRY_METADATA;
import static org.elasticsearch.TransportVersions.INFERENCE_MODEL_REGISTRY_METADATA_8_19;
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
    @Nullable String service,
    TaskType taskType,
    @Nullable Integer dimensions,
    @Nullable SimilarityMeasure similarity,
    @Nullable ElementType elementType
) implements ServiceSettings, SimpleDiffable<MinimalServiceSettings> {

    public static final String NAME = "minimal_service_settings";

    public static final String SERVICE_FIELD = "service";
    public static final String TASK_TYPE_FIELD = "task_type";
    static final String DIMENSIONS_FIELD = "dimensions";
    static final String SIMILARITY_FIELD = "similarity";
    static final String ELEMENT_TYPE_FIELD = "element_type";

    private static final ConstructingObjectParser<MinimalServiceSettings, Void> PARSER = new ConstructingObjectParser<>(
        "model_settings",
        true,
        args -> {
            String service = (String) args[0];
            TaskType taskType = TaskType.fromString((String) args[1]);
            Integer dimensions = (Integer) args[2];
            SimilarityMeasure similarity = args[3] == null ? null : SimilarityMeasure.fromString((String) args[3]);
            DenseVectorFieldMapper.ElementType elementType = args[4] == null
                ? null
                : DenseVectorFieldMapper.ElementType.fromString((String) args[4]);
            return new MinimalServiceSettings(service, taskType, dimensions, similarity, elementType);
        }
    );
    private static final String UNKNOWN_SERVICE = "_unknown_";

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(SERVICE_FIELD));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(TASK_TYPE_FIELD));
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(DIMENSIONS_FIELD));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(SIMILARITY_FIELD));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ELEMENT_TYPE_FIELD));
    }

    public static MinimalServiceSettings parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static MinimalServiceSettings textEmbedding(
        String serviceName,
        int dimensions,
        SimilarityMeasure similarity,
        ElementType elementType
    ) {
        return new MinimalServiceSettings(serviceName, TEXT_EMBEDDING, dimensions, similarity, elementType);
    }

    public static MinimalServiceSettings sparseEmbedding(String serviceName) {
        return new MinimalServiceSettings(serviceName, SPARSE_EMBEDDING, null, null, null);
    }

    public static MinimalServiceSettings rerank(String serviceName) {
        return new MinimalServiceSettings(serviceName, RERANK, null, null, null);
    }

    public static MinimalServiceSettings completion(String serviceName) {
        return new MinimalServiceSettings(serviceName, COMPLETION, null, null, null);
    }

    public static MinimalServiceSettings chatCompletion(String serviceName) {
        return new MinimalServiceSettings(serviceName, CHAT_COMPLETION, null, null, null);
    }

    public MinimalServiceSettings {
        Objects.requireNonNull(taskType, "task type must not be null");
        validate(taskType, dimensions, similarity, elementType);
    }

    public MinimalServiceSettings(Model model) {
        this(
            model.getConfigurations().getService(),
            model.getTaskType(),
            model.getServiceSettings().dimensions(),
            model.getServiceSettings().similarity(),
            model.getServiceSettings().elementType()
        );
    }

    public MinimalServiceSettings(StreamInput in) throws IOException {
        this(
            in.readOptionalString(),
            TaskType.fromStream(in),
            in.readOptionalInt(),
            in.readOptionalEnum(SimilarityMeasure.class),
            in.readOptionalEnum(ElementType.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(service);
        taskType.writeTo(out);
        out.writeOptionalInt(dimensions);
        out.writeOptionalEnum(similarity);
        out.writeOptionalEnum(elementType);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.INFERENCE_MODEL_REGISTRY_METADATA_8_19;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.isPatchFrom(INFERENCE_MODEL_REGISTRY_METADATA_8_19) || version.onOrAfter(INFERENCE_MODEL_REGISTRY_METADATA);
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this::toXContent;
    }

    @Override
    public String modelId() {
        return null;
    }

    public static Diff<MinimalServiceSettings> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(MinimalServiceSettings::new, in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (service != null) {
            builder.field(SERVICE_FIELD, service);
        }
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
        sb.append("service=").append(service);
        sb.append(", task_type=").append(taskType);
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

    private static void validate(TaskType taskType, Integer dimensions, SimilarityMeasure similarity, ElementType elementType) {
        switch (taskType) {
            case TEXT_EMBEDDING:
                validateFieldPresent(DIMENSIONS_FIELD, dimensions, taskType);
                validateFieldPresent(SIMILARITY_FIELD, similarity, taskType);
                validateFieldPresent(ELEMENT_TYPE_FIELD, elementType, taskType);
                break;

            default:
                validateFieldNotPresent(DIMENSIONS_FIELD, dimensions, taskType);
                validateFieldNotPresent(SIMILARITY_FIELD, similarity, taskType);
                validateFieldNotPresent(ELEMENT_TYPE_FIELD, elementType, taskType);
                break;
        }
    }

    private static void validateFieldPresent(String field, Object fieldValue, TaskType taskType) {
        if (fieldValue == null) {
            throw new IllegalArgumentException("required [" + field + "] field is missing for task_type [" + taskType.name() + "]");
        }
    }

    private static void validateFieldNotPresent(String field, Object fieldValue, TaskType taskType) {
        if (fieldValue != null) {
            throw new IllegalArgumentException("[" + field + "] is not allowed for task_type [" + taskType.name() + "]");
        }
    }

    /**
     * Checks if the given {@link MinimalServiceSettings} is equivalent to the current definition.
     */
    public boolean canMergeWith(MinimalServiceSettings other) {
        return taskType == other.taskType
            && Objects.equals(dimensions, other.dimensions)
            && similarity == other.similarity
            && elementType == other.elementType;
    }
}
