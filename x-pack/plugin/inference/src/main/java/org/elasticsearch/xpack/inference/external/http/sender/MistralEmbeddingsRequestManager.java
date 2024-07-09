/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.mistral.MistralEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.AzureMistralOpenAiExternalResponseHandler;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.external.response.mistral.MistralEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class MistralEmbeddingsRequestManager extends BaseRequestManager {
    private static final Logger logger = LogManager.getLogger(AzureOpenAiEmbeddingsRequestManager.class);
    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private final Truncator truncator;
    private final MistralEmbeddingsModel model;

    private static ResponseHandler createEmbeddingsHandler() {
        return new AzureMistralOpenAiExternalResponseHandler(
            "mistral text embedding",
            new MistralEmbeddingsResponseEntity(),
            ErrorMessageResponseEntity::fromResponse
        );
    }

    public MistralEmbeddingsRequestManager(MistralEmbeddingsModel model, Truncator truncator, ThreadPool threadPool) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitSettings());
        this.model = Objects.requireNonNull(model);
        this.truncator = Objects.requireNonNull(truncator);

    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        List<String> docsInput = DocumentsOnlyInput.of(inferenceInputs).getInputs();
        var truncatedInput = truncate(docsInput, model.getServiceSettings().maxInputTokens());
        MistralEmbeddingsRequest request = new MistralEmbeddingsRequest(truncator, truncatedInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }

    record RateLimitGrouping(int keyHashCode) {
        public static RateLimitGrouping of(MistralEmbeddingsModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.getSecretSettings().apiKey().hashCode());
        }
    }
}
