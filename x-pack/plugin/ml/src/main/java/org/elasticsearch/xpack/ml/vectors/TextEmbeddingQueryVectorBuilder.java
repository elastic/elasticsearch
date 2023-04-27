/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TextEmbeddingQueryVectorBuilder implements QueryVectorBuilder {

    public static final String NAME = "text_embedding";

    public static final ParseField MODEL_TEXT = new ParseField("model_text");

    public static final ConstructingObjectParser<TextEmbeddingQueryVectorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new TextEmbeddingQueryVectorBuilder((String) args[0], (String) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), TrainedModelConfig.MODEL_ID);
        PARSER.declareString(constructorArg(), MODEL_TEXT);
    }

    public static TextEmbeddingQueryVectorBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String modelId;
    private final String modelText;

    public TextEmbeddingQueryVectorBuilder(String modelId, String modelText) {
        this.modelId = modelId;
        this.modelText = modelText;
    }

    public TextEmbeddingQueryVectorBuilder(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.modelText = in.readString();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_7_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(modelText);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
        builder.field(MODEL_TEXT.getPreferredName(), modelText);
        builder.endObject();
        return builder;
    }

    @Override
    public void buildVector(Client client, ActionListener<float[]> listener) {
        InferModelAction.Request inferRequest = InferModelAction.Request.forTextInput(
            modelId,
            TextEmbeddingConfigUpdate.EMPTY_INSTANCE,
            List.of(modelText)
        );
        inferRequest.setHighPriority(true);

        executeAsyncWithOrigin(client, ML_ORIGIN, InferModelAction.INSTANCE, inferRequest, ActionListener.wrap(response -> {
            if (response.getInferenceResults().isEmpty()) {
                listener.onFailure(new IllegalStateException("text embedding inference response contain no results"));
                return;
            }

            if (response.getInferenceResults().get(0) instanceof TextEmbeddingResults textEmbeddingResults) {
                listener.onResponse(textEmbeddingResults.getInferenceAsFloat());
            } else if (response.getInferenceResults().get(0) instanceof WarningInferenceResults warning) {
                listener.onFailure(new IllegalStateException(warning.getWarning()));
            } else {
                throw new IllegalStateException(
                    "expected a result of type ["
                        + TextEmbeddingResults.NAME
                        + "] received ["
                        + response.getInferenceResults().get(0).getWriteableName()
                        + "]. Is ["
                        + modelId
                        + "] a text embedding model?"
                );
            }
        }, listener::onFailure));
    }

    public String getModelText() {
        return modelText;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextEmbeddingQueryVectorBuilder that = (TextEmbeddingQueryVectorBuilder) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(modelText, that.modelText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, modelText);
    }
}
