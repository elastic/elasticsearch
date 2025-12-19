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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIContextualizedEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.voyageai.response.VoyageAIContextualizedEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class VoyageAIContextualEmbeddingsRequestManager extends BaseRequestManager {
    private static final Logger logger = LogManager.getLogger(VoyageAIContextualEmbeddingsRequestManager.class);
    private static final ResponseHandler HANDLER = createContextualEmbeddingsHandler();

    private static ResponseHandler createContextualEmbeddingsHandler() {
        return new VoyageAIResponseHandler("voyageai contextual embedding", VoyageAIContextualizedEmbeddingsResponseEntity::fromResponse);
    }

    public static VoyageAIContextualEmbeddingsRequestManager of(VoyageAIContextualEmbeddingsModel model, ThreadPool threadPool) {
        return new VoyageAIContextualEmbeddingsRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final VoyageAIContextualEmbeddingsModel model;

    private VoyageAIContextualEmbeddingsRequestManager(VoyageAIContextualEmbeddingsModel model, ThreadPool threadPool) {
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

        // Wrap all inputs as a single entry in the top-level list
        // Input: List<String> ["text1", "text2", "text3"]
        // Output: List<List<String>> [["text1", "text2", "text3"]]
        List<List<String>> nestedInputs = List.of(embeddingsInput.getTextInputs());

        VoyageAIContextualizedEmbeddingsRequest request = new VoyageAIContextualizedEmbeddingsRequest(
            nestedInputs,
            embeddingsInput.getInputType(),
            model
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
