/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.completion.AmazonBedrockChatCompletionResponseListener;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.embeddings.AmazonBedrockEmbeddingsResponseListener;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.util.Objects;
import java.util.function.Supplier;

public class AmazonBedrockExecutor implements Runnable {
    protected final AmazonBedrockModel baseModel;
    protected final AmazonBedrockResponseHandler responseHandler;
    protected final Logger logger;
    protected final AmazonBedrockRequest request;
    protected final Supplier<Boolean> hasRequestCompletedFunction;
    protected final ActionListener<InferenceServiceResults> inferenceResultsListener;
    private final AmazonBedrockClientCache clientCache;

    public AmazonBedrockExecutor(
        AmazonBedrockModel model,
        AmazonBedrockRequest request,
        AmazonBedrockResponseHandler responseHandler,
        Logger logger,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> inferenceResultsListener,
        AmazonBedrockClientCache clientCache
    ) {
        this.baseModel = Objects.requireNonNull(model);
        this.responseHandler = Objects.requireNonNull(responseHandler);
        this.logger = Objects.requireNonNull(logger);
        this.request = Objects.requireNonNull(request);
        this.hasRequestCompletedFunction = Objects.requireNonNull(hasRequestCompletedFunction);
        this.inferenceResultsListener = Objects.requireNonNull(inferenceResultsListener);
        this.clientCache = Objects.requireNonNull(clientCache);
    }

    @Override
    public void run() {
        if (hasRequestCompletedFunction.get()) {
            // has already been run
            return;
        }

        var inferenceEntityId = baseModel.getInferenceEntityId();

        try {
            var awsBedrockClient = clientCache.getOrCreateClient(baseModel, request.timeout());
            executeClientRequest(awsBedrockClient);
        } catch (Exception e) {
            var errorMessage = Strings.format("Failed to send request from inference entity id [%s]", inferenceEntityId);
            logger.warn(errorMessage, e);
            inferenceResultsListener.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }

    private void executeClientRequest(AmazonBedrockBaseClient awsBedrockClient) {
        if (request instanceof AmazonBedrockChatCompletionRequest chatCompletionRequest) {
            var chatCompletionResponseListener = new AmazonBedrockChatCompletionResponseListener(
                chatCompletionRequest,
                responseHandler,
                inferenceResultsListener
            );
            chatCompletionRequest.executeChatCompletionRequest(awsBedrockClient, chatCompletionResponseListener);
            return;
        }

        if (request instanceof AmazonBedrockEmbeddingsRequest embeddingsRequest) {
            var embeddingsResponseListener = new AmazonBedrockEmbeddingsResponseListener(
                embeddingsRequest,
                responseHandler,
                inferenceResultsListener
            );
            embeddingsRequest.executeEmbeddingsRequest(awsBedrockClient, embeddingsResponseListener);
            return;
        }

        throw new ElasticsearchException("Unsupported request type [" + request.getClass() + "]");
    }
}
