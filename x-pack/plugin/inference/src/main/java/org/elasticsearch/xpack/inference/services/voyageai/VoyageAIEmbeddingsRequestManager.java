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
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.text.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.voyageai.response.VoyageAIEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class VoyageAIEmbeddingsRequestManager extends VoyageAIRequestManager {
    private static final Logger logger = LogManager.getLogger(VoyageAIEmbeddingsRequestManager.class);
    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new VoyageAIResponseHandler("voyageai text embedding", VoyageAIEmbeddingsResponseEntity::fromResponse);
    }

    public static VoyageAIEmbeddingsRequestManager of(VoyageAIEmbeddingsModel model, ThreadPool threadPool) {
        return new VoyageAIEmbeddingsRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final VoyageAIEmbeddingsModel model;

    private VoyageAIEmbeddingsRequestManager(VoyageAIEmbeddingsModel model, ThreadPool threadPool) {
        super(threadPool, model);
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
        List<String> docsInput = embeddingsInput.getTextInputs();
        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(docsInput, embeddingsInput.getInputType(), model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
