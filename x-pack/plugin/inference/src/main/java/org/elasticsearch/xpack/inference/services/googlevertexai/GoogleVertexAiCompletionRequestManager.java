/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.googlevertexai.response.GoogleVertexAiChatCompletionResponseEntity;

import java.util.Objects;
import java.util.function.Supplier;

public class GoogleVertexAiCompletionRequestManager extends GoogleVertexAiRequestManager {

    private static final Logger logger = LogManager.getLogger(GoogleVertexAiCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createGoogleVertexAiResponseHandler();

    private static ResponseHandler createGoogleVertexAiResponseHandler() {
        return new GoogleVertexAiUnifiedChatCompletionResponseHandler(
            "Google Vertex AI chat completion",
            GoogleVertexAiChatCompletionResponseEntity::fromResponse
        );
    }

    private final GoogleVertexAiChatCompletionModel model;

    public GoogleVertexAiCompletionRequestManager(GoogleVertexAiChatCompletionModel model, ThreadPool threadPool) {
        super(threadPool, model, RateLimitGrouping.of(model));
        this.model = model;
    }

    record RateLimitGrouping(int projectIdHash) {
        public static RateLimitGrouping of(GoogleVertexAiChatCompletionModel model) {
            Objects.requireNonNull(model);
            return new RateLimitGrouping(model.rateLimitServiceSettings().projectId().hashCode());
        }
    }

    public static GoogleVertexAiCompletionRequestManager of(GoogleVertexAiChatCompletionModel model, ThreadPool threadPool) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(threadPool);

        return new GoogleVertexAiCompletionRequestManager(model, threadPool);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {

        var chatInputs = inferenceInputs.castTo(UnifiedChatInput.class);
        var request = new GoogleVertexAiUnifiedChatCompletionRequest(chatInputs, model);
        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
