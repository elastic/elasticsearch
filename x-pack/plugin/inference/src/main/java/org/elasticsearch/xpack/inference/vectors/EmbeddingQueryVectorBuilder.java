/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xcontent.ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING;
import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class EmbeddingQueryVectorBuilder implements QueryVectorBuilder {
    public static final String NAME = "embedding";

    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField INPUT_FIELD = new ParseField("input");
    public static final ParseField TIMEOUT_FIELD = new ParseField("timeout");

    public static final TimeValue DEFAULT_TIMEOUT = InferenceAction.Request.DEFAULT_TIMEOUT;

    public static final NodeFeature EMBEDDING_QUERY_VECTOR_BUILDER_FEATURE = new NodeFeature("embedding_query_vector_builder");

    public static final ConstructingObjectParser<EmbeddingQueryVectorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new EmbeddingQueryVectorBuilder(
            (String) args[0],
            (InferenceStringGroup) args[1],
            args[2] == null ? null : TimeValue.parseTimeValue((String) args[2], TIMEOUT_FIELD.getPreferredName())
        )
    );

    static {
        PARSER.declareString(optionalConstructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareField(constructorArg(), (p, c) -> parseInput(p), INPUT_FIELD, OBJECT_ARRAY_OR_STRING);
        PARSER.declareString(optionalConstructorArg(), TIMEOUT_FIELD);
    }

    public static EmbeddingQueryVectorBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static InferenceStringGroup parseInput(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new InferenceStringGroup(parser.text());
        } else if (token == XContentParser.Token.START_OBJECT) {
            return new InferenceStringGroup(InferenceString.PARSER.apply(parser, null));
        } else if (token == XContentParser.Token.START_ARRAY) {
            List<InferenceString> inferenceStrings = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                inferenceStrings.add(InferenceString.PARSER.apply(parser, null));
            }

            if (inferenceStrings.isEmpty()) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "Must provide at least one value in " + INPUT_FIELD.getPreferredName()
                );
            }

            return new InferenceStringGroup(inferenceStrings);
        }
        throw new XContentParseException(parser.getTokenLocation(), "Unsupported token [" + token + "]");
    }

    private static final TransportVersion EMBEDDING_QUERY_VECTOR_BUILDER_TV = TransportVersion.fromName("embedding_query_vector_builder");

    private final String inferenceId;
    private final InferenceStringGroup input;
    private final TimeValue timeout;

    public EmbeddingQueryVectorBuilder(@Nullable String inferenceId, InferenceStringGroup input, @Nullable TimeValue timeout) {
        this.inferenceId = inferenceId;
        this.input = Objects.requireNonNull(input);
        this.timeout = timeout;
    }

    public EmbeddingQueryVectorBuilder(StreamInput in) throws IOException {
        this.inferenceId = in.readOptionalString();
        this.input = new InferenceStringGroup(in);
        this.timeout = in.readOptionalTimeValue();
    }

    @Override
    public void buildVector(Client client, ActionListener<float[]> listener) {
        if (inferenceId == null) {
            listener.onFailure(new IllegalArgumentException("[inference_id] must be specified"));
            return;
        }

        var embeddingRequest = new EmbeddingRequest(List.of(input), InputType.SEARCH, null);
        var actualTimeout = Objects.requireNonNullElse(timeout, DEFAULT_TIMEOUT);
        var request = new EmbeddingAction.Request(inferenceId, TaskType.ANY, embeddingRequest, actualTimeout);
        executeAsyncWithOrigin(
            client,
            INFERENCE_ORIGIN,
            EmbeddingAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap(EmbeddingQueryVectorBuilder::handleEmbeddingResponse)
        );
    }

    private static void handleEmbeddingResponse(ActionListener<float[]> listener, InferenceAction.Response response) {
        List<? extends InferenceResults> inferenceResults = response.getResults().transformToCoordinationFormat();
        if (inferenceResults.isEmpty()) {
            listener.onFailure(new IllegalStateException("the query embedding response contains no results"));
        } else if (inferenceResults.size() > 1) {
            listener.onFailure(new IllegalStateException("the query embedding response contains " + inferenceResults.size() + " results"));
        } else {
            InferenceResults inferenceResult = inferenceResults.getFirst();
            if (inferenceResult instanceof MlDenseEmbeddingResults mlDenseEmbeddingResults) {
                listener.onResponse(mlDenseEmbeddingResults.getInferenceAsFloat());
            } else {
                listener.onFailure(
                    new IllegalStateException(
                        "expected inference results to be of type ["
                            + MlDenseEmbeddingResults.NAME
                            + "], received ["
                            + inferenceResult.getWriteableName()
                            + "]"
                    )
                );
            }
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return EMBEDDING_QUERY_VECTOR_BUILDER_TV;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(inferenceId);
        out.writeWriteable(input);
        out.writeOptionalTimeValue(timeout);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inferenceId != null) {
            builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        }
        builder.xContentList(INPUT_FIELD.getPreferredName(), input.inferenceStrings());
        if (timeout != null) {
            builder.field(TIMEOUT_FIELD.getPreferredName(), timeout.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    public String getInferenceId() {
        return inferenceId;
    }

    public InferenceStringGroup getInput() {
        return input;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmbeddingQueryVectorBuilder that = (EmbeddingQueryVectorBuilder) o;
        return Objects.equals(inferenceId, that.inferenceId) && Objects.equals(input, that.input) && Objects.equals(timeout, that.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceId, input, timeout);
    }
}
