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
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.inference.metadata.EndpointMetadataClusterState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.inference.metadata.EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED;
import static org.elasticsearch.inference.metadata.EndpointMetadata.METADATA_FIELD_NAME;

/**
 * Defines the base settings required to configure an inference endpoint.
 * <p>
 * These settings are immutable and describe the input and output types that the endpoint will handle.
 * They capture the essential properties of an inference model, ensuring the endpoint is correctly configured.
 * <p>
 * Key properties include:
 * <ul>
 *   <li>{@code taskType} - Specifies the type of task the model performs, such as classification or text embeddings.</li>
 *   <li>{@code dimensions}, {@code similarity}, and {@code elementType} - These settings are applicable only when
 *       the {@code taskType} is {@link TaskType#TEXT_EMBEDDING}. They define the structure and behavior of embeddings.</li>
 * </ul>
 *
 * @param taskType         the type of task the inference model performs.
 * @param dimensions       the number of dimensions for the embeddings,
 *                         applicable only for {@link TaskType#TEXT_EMBEDDING} and {@link TaskType#EMBEDDING} (nullable).
 * @param similarity       the similarity measure used for embeddings,
 *                         applicable only for {@link TaskType#TEXT_EMBEDDING} and {@link TaskType#EMBEDDING} (nullable).
 * @param elementType      the type of elements in the embeddings,
 *                         applicable only for {@link TaskType#TEXT_EMBEDDING} and {@link TaskType#EMBEDDING} (nullable).
 * @param endpointMetadata the subset of endpoint metadata stored in cluster state (heuristics and internal only).
 */
public record EndpointClusterState(
    @Nullable String service,
    TaskType taskType,
    @Nullable Integer dimensions,
    @Nullable SimilarityMeasure similarity,
    @Nullable ElementType elementType,
    EndpointMetadataClusterState endpointMetadata
) implements ToXContentObject, SimpleDiffable<EndpointClusterState> {

    public static final String SERVICE_FIELD = "service";
    public static final String TASK_TYPE_FIELD = "task_type";
    static final String DIMENSIONS_FIELD = "dimensions";
    static final String SIMILARITY_FIELD = "similarity";
    static final String ELEMENT_TYPE_FIELD = "element_type";
    private static final String INCLUDE_ENDPOINT_METADATA_PARAM_NAME = "include_endpoint_metadata";
    private static final ConstructingObjectParser<EndpointClusterState, Void> PARSER = new ConstructingObjectParser<>(
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
            var metadata = args[5] == null ? EndpointMetadataClusterState.EMPTY_INSTANCE : (EndpointMetadataClusterState) args[5];
            return new EndpointClusterState(service, taskType, dimensions, similarity, elementType, metadata);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(SERVICE_FIELD));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(TASK_TYPE_FIELD));
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(DIMENSIONS_FIELD));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(SIMILARITY_FIELD));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ELEMENT_TYPE_FIELD));
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> EndpointMetadataClusterState.parse(p),
            new ParseField(METADATA_FIELD_NAME)
        );
    }

    public static EndpointClusterState parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * This class used to be a named writeable and needed a transport version for that. The class is no longer a named writeable but this
     * transport definition cannot be deleted.
     */
    @SuppressWarnings("unused")
    private static final TransportVersion INFERENCE_MODEL_REGISTRY_METADATA = TransportVersion.fromName(
        "inference_model_registry_metadata"
    );

    /**
     * Transport version at which cluster state stores only {@link EndpointMetadataClusterState}
     * which is a subset of {@link EndpointMetadata}.
     * Peers older than this expect the full {@link EndpointMetadata} layout on the wire.
     */
    private static final TransportVersion INFERENCE_ENDPOINT_METADATA_CLUSTER_STATE_ADDED = TransportVersion.fromName(
        "inference_endpoint_metadata_cluster_state_added"
    );

    public static EndpointClusterState textEmbedding(
        String serviceName,
        int dimensions,
        SimilarityMeasure similarity,
        ElementType elementType
    ) {
        return new EndpointClusterState(serviceName, TEXT_EMBEDDING, dimensions, similarity, elementType);
    }

    public static EndpointClusterState sparseEmbedding(String serviceName) {
        return new EndpointClusterState(serviceName, SPARSE_EMBEDDING, null, null, null);
    }

    public static EndpointClusterState rerank(String serviceName) {
        return new EndpointClusterState(serviceName, RERANK, null, null, null);
    }

    public static EndpointClusterState completion(String serviceName) {
        return new EndpointClusterState(serviceName, COMPLETION, null, null, null);
    }

    public static EndpointClusterState chatCompletion(String serviceName) {
        return new EndpointClusterState(serviceName, CHAT_COMPLETION, null, null, null);
    }

    public static Params withoutEndpointMetadata() {
        return withoutEndpointMetadata(ToXContentObject.EMPTY_PARAMS);
    }

    public static Params withoutEndpointMetadata(Params params) {
        Map<String, String> entries = Map.of(INCLUDE_ENDPOINT_METADATA_PARAM_NAME, Boolean.FALSE.toString());
        return new DelegatingMapParams(entries, params);
    }

    public EndpointClusterState {
        Objects.requireNonNull(taskType, "task type must not be null");
        validate(taskType, dimensions, similarity, elementType);
    }

    public EndpointClusterState(
        @Nullable String service,
        TaskType taskType,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable ElementType elementType
    ) {
        this(service, taskType, dimensions, similarity, elementType, EndpointMetadataClusterState.EMPTY_INSTANCE);
    }

    public EndpointClusterState(Model model) {
        this(
            model.getConfigurations().getService(),
            model.getTaskType(),
            model.getServiceSettings().dimensions(),
            model.getServiceSettings().similarity(),
            model.getServiceSettings().elementType(),
            EndpointMetadataClusterState.from(model.getConfigurations().getEndpointMetadataOrEmpty())
        );
    }

    public EndpointClusterState(StreamInput in) throws IOException {
        this(
            in.readOptionalString(),
            TaskType.fromStream(in),
            in.readOptionalInt(),
            in.readOptionalEnum(SimilarityMeasure.class),
            in.readOptionalEnum(ElementType.class),
            readEndpointMetadataClusterState(in)
        );
    }

    /**
     * Reads the cluster-state metadata subset. Nodes older than {@link #INFERENCE_ENDPOINT_METADATA_CLUSTER_STATE_ADDED} wrote the full
     * {@link EndpointMetadata} layout, so it is read with that class's own reader — which owns the layout and its internal version
     * gates — and then narrowed to the subset. The dropped fields are authoritative in the {@code .inference} system index and are
     * never read back from cluster state.
     */
    private static EndpointMetadataClusterState readEndpointMetadataClusterState(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED) == false) {
            return EndpointMetadataClusterState.EMPTY_INSTANCE;
        }
        if (in.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_CLUSTER_STATE_ADDED) == false) {
            return EndpointMetadataClusterState.from(new EndpointMetadata(in));
        }
        return new EndpointMetadataClusterState(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(service);
        taskType.writeTo(out);
        out.writeOptionalInt(dimensions);
        out.writeOptionalEnum(similarity);
        out.writeOptionalEnum(elementType);
        writeEndpointMetadataClusterState(out);
    }

    /**
     * Writes the cluster-state metadata subset. Peers older than {@link #INFERENCE_ENDPOINT_METADATA_CLUSTER_STATE_ADDED} expect the full
     * {@link EndpointMetadata} layout, so the subset is expanded with empty values for the dropped fields to keep the stream aligned
     * for the peer's map-level reader in {@code ModelRegistryClusterStateMetadata}.
     */
    private void writeEndpointMetadataClusterState(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED) == false) {
            return;
        }
        if (out.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_CLUSTER_STATE_ADDED) == false) {
            endpointMetadata.writeAsFullEndpointMetadata(out);
            return;
        }
        endpointMetadata.writeTo(out);
    }

    public static Diff<EndpointClusterState> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(EndpointClusterState::new, in);
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
        if (params.paramAsBoolean(INCLUDE_ENDPOINT_METADATA_PARAM_NAME, true) && endpointMetadata.isEmpty() == false) {
            builder.field(METADATA_FIELD_NAME, endpointMetadata);
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
        sb.append(", metadata=").append(endpointMetadata);
        return sb.toString();
    }

    private static void validate(TaskType taskType, Integer dimensions, SimilarityMeasure similarity, ElementType elementType) {
        switch (taskType) {
            case TEXT_EMBEDDING, EMBEDDING:
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
     * Checks if the given {@link EndpointClusterState} is equivalent to the current definition.
     */
    public boolean canMergeWith(EndpointClusterState other) {
        return taskType == other.taskType
            && Objects.equals(dimensions, other.dimensions)
            && similarity == other.similarity
            && elementType == other.elementType;
    }
}
