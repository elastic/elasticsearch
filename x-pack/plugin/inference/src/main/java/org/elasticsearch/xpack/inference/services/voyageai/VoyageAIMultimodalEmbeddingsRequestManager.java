/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIMultimodalEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.voyageai.response.VoyageAIEmbeddingsResponseEntity;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Request manager for VoyageAI multimodal embeddings.
 * Handles multimodal inputs including text, images (base64/url), and videos (base64/url).
 */
public class VoyageAIMultimodalEmbeddingsRequestManager extends BaseRequestManager {
    private static final Logger logger = LogManager.getLogger(VoyageAIMultimodalEmbeddingsRequestManager.class);
    private static final ResponseHandler HANDLER = createMultimodalEmbeddingsHandler();

    private static ResponseHandler createMultimodalEmbeddingsHandler() {
        return new VoyageAIResponseHandler("voyageai multimodal embedding", VoyageAIEmbeddingsResponseEntity::fromResponse);
    }

    public static VoyageAIMultimodalEmbeddingsRequestManager of(VoyageAIMultimodalEmbeddingsModel model, ThreadPool threadPool) {
        return new VoyageAIMultimodalEmbeddingsRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final VoyageAIMultimodalEmbeddingsModel model;

    private VoyageAIMultimodalEmbeddingsRequestManager(VoyageAIMultimodalEmbeddingsModel model, ThreadPool threadPool) {
        super(threadPool, model.getInferenceEntityId(), VoyageAIRequestManager.RateLimitGrouping.of(model), model.rateLimitSettings());
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        EmbeddingsInput embeddingsInput = inferenceInputs.castTo(EmbeddingsInput.class);

        // Get inputs as InferenceStringGroups - supports multimodal content (text + images + videos)
        List<InferenceStringGroup> inputs = embeddingsInput.getInputs();

        VoyageAIMultimodalEmbeddingsRequest request = new VoyageAIMultimodalEmbeddingsRequest(
            inputs,
            embeddingsInput.getInputType(),
            model
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
