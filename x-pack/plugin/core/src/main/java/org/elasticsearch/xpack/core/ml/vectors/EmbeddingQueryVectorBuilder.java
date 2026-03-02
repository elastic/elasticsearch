/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.vectors;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class EmbeddingQueryVectorBuilder implements QueryVectorBuilder {
    public static final String NAME = "embedding";

    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField FORMAT_FIELD = new ParseField("format");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    public static final ConstructingObjectParser<EmbeddingQueryVectorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new EmbeddingQueryVectorBuilder((String) args[0], (DataType) args[1], (DataFormat) args[2], (String) args[3])
    );

    static {
        PARSER.declareString(optionalConstructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareString(constructorArg(), DataType::fromString, TYPE_FIELD);
        PARSER.declareString(optionalConstructorArg(), DataFormat::fromString, FORMAT_FIELD);
        PARSER.declareString(constructorArg(), VALUE_FIELD);
    }

    public static EmbeddingQueryVectorBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static final TransportVersion EMBEDDING_QUERY_VECTOR_BUILDER = TransportVersion.fromName("embedding_query_vector_builder");

    private final String inferenceId;
    private final DataType type;
    private final DataFormat format;
    private final String value;

    public EmbeddingQueryVectorBuilder(DataType type, String value) {
        this(null, type, null, value);
    }

    public EmbeddingQueryVectorBuilder(@Nullable String inferenceId, DataType type, @Nullable DataFormat format, String value) {
        this.inferenceId = inferenceId;
        this.type = Objects.requireNonNull(type);
        this.format = format;
        this.value = Objects.requireNonNull(value);
    }

    public EmbeddingQueryVectorBuilder(StreamInput in) throws IOException {
        this.inferenceId = in.readOptionalString();
        this.type = in.readEnum(DataType.class);
        this.format = in.readOptionalEnum(DataFormat.class);
        this.value = in.readString();
    }

    @Override
    public void buildVector(Client client, ActionListener<float[]> listener) {
        if (inferenceId == null) {
            listener.onFailure(new IllegalArgumentException("[inference_id] must be specified"));
            return;
        }

        var getModelRequest = new GetInferenceModelAction.Request(inferenceId, TaskType.ANY);
        executeAsyncWithOrigin(
            client,
            INFERENCE_ORIGIN,
            GetInferenceModelAction.INSTANCE,
            getModelRequest,
            listener.delegateFailureAndWrap((l, getModelResponse) -> handleGetModelResponse(client, l, getModelResponse))
        );
    }

    private void handleGetModelResponse(
        Client client,
        ActionListener<float[]> listener,
        GetInferenceModelAction.Response getModelResponse
    ) {
        if (getModelResponse.getEndpoints().isEmpty()) {
            listener.onFailure(new ResourceNotFoundException("inference endpoint [" + inferenceId + "] not found"));
            return;
        }

        var taskType = getModelResponse.getEndpoints().get(0).getTaskType();
        if (taskType != TaskType.EMBEDDING) {
            listener.onFailure(
                new IllegalArgumentException(
                    "inference endpoint [" + inferenceId + "] has task type [" + taskType + "], expected [" + TaskType.EMBEDDING + "]"
                )
            );
            return;
        }

        var inferenceString = format != null ? new InferenceString(type, format, value) : new InferenceString(type, value);
        var embeddingRequest = EmbeddingRequest.of(List.of(new InferenceStringGroup(inferenceString)));
        // TODO: Don't hard-code timeout
        var request = new EmbeddingAction.Request(inferenceId, TaskType.EMBEDDING, embeddingRequest, TimeValue.THIRTY_SECONDS);
        executeAsyncWithOrigin(
            client,
            INFERENCE_ORIGIN,
            EmbeddingAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap(EmbeddingQueryVectorBuilder::handleEmbeddingResponse)
        );
    }

    private static void handleEmbeddingResponse(ActionListener<float[]> listener, InferenceAction.Response response) {
        if (response.getResults() instanceof EmbeddingFloatResults results) {
            if (results.embeddings().isEmpty()) {
                listener.onFailure(new IllegalStateException("embedding inference response contains no results"));
                return;
            }
            listener.onResponse(results.embeddings().getFirst().values());
        } else {
            listener.onFailure(
                new IllegalStateException(
                    "expected a result of type [" + EmbeddingFloatResults.class + "], received [" + response.getResults().getClass() + "]"
                )
            );
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return EMBEDDING_QUERY_VECTOR_BUILDER;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(inferenceId);
        out.writeEnum(type);
        out.writeOptionalEnum(format);
        out.writeString(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inferenceId != null) {
            builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        }
        builder.field(TYPE_FIELD.getPreferredName(), type);
        if (format != null) {
            builder.field(FORMAT_FIELD.getPreferredName(), format);
        }
        builder.field(VALUE_FIELD.getPreferredName(), value);
        builder.endObject();
        return builder;
    }
}
