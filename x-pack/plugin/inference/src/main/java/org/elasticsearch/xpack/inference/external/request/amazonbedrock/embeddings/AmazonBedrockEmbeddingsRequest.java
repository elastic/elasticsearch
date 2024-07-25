/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import com.amazonaws.services.bedrockruntime.model.InvokeModelRequest;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockBaseClient;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockJsonBuilder;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.embeddings.AmazonBedrockEmbeddingsResponseListener;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class AmazonBedrockEmbeddingsRequest extends AmazonBedrockRequest {
    private final AmazonBedrockEmbeddingsModel embeddingsModel;
    private final ToXContent requestEntity;
    private final Truncator truncator;
    private final Truncator.TruncationResult truncationResult;
    private final AmazonBedrockProvider provider;
    private ActionListener<InvokeModelResult> listener = null;

    public AmazonBedrockEmbeddingsRequest(
        Truncator truncator,
        Truncator.TruncationResult input,
        AmazonBedrockEmbeddingsModel model,
        ToXContent requestEntity,
        @Nullable TimeValue timeout
    ) {
        super(model, timeout);
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.requestEntity = Objects.requireNonNull(requestEntity);
        this.embeddingsModel = model;
        this.provider = model.provider();
    }

    public AmazonBedrockProvider provider() {
        return provider;
    }

    @Override
    protected void executeRequest(AmazonBedrockBaseClient client) {
        try {
            var jsonBuilder = new AmazonBedrockJsonBuilder(requestEntity);
            var bodyAsString = jsonBuilder.getStringContent();

            var charset = StandardCharsets.UTF_8;
            var bodyBuffer = charset.encode(bodyAsString);

            var invokeModelRequest = new InvokeModelRequest().withModelId(embeddingsModel.model()).withBody(bodyBuffer);

            SocketAccess.doPrivileged(() -> client.invokeModel(invokeModelRequest, listener));
        } catch (IOException e) {
            listener.onFailure(new RuntimeException(e));
        }
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new AmazonBedrockEmbeddingsRequest(truncator, truncatedInput, embeddingsModel, requestEntity, timeout);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    @Override
    public TaskType taskType() {
        return TaskType.TEXT_EMBEDDING;
    }

    public void executeEmbeddingsRequest(
        AmazonBedrockBaseClient awsBedrockClient,
        AmazonBedrockEmbeddingsResponseListener embeddingsResponseListener
    ) {
        this.listener = embeddingsResponseListener;
        this.executeRequest(awsBedrockClient);
    }
}
