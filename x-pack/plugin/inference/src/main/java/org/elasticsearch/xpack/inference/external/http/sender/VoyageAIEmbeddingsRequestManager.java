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
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.voyageai.VoyageAIEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.voyageai.VoyageAIEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.external.voyageai.VoyageAIResponseHandler;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;

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
        List<String> docsInput = DocumentsOnlyInput.of(inferenceInputs).getInputs();
        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(docsInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
