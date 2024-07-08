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
import org.elasticsearch.xpack.inference.external.cohere.CohereResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.cohere.CohereEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.cohere.CohereEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class CohereEmbeddingsRequestManager extends CohereRequestManager {
    private static final Logger logger = LogManager.getLogger(CohereEmbeddingsRequestManager.class);
    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new CohereResponseHandler("cohere text embedding", CohereEmbeddingsResponseEntity::fromResponse);
    }

    public static CohereEmbeddingsRequestManager of(CohereEmbeddingsModel model, ThreadPool threadPool) {
        return new CohereEmbeddingsRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final CohereEmbeddingsModel model;

    private CohereEmbeddingsRequestManager(CohereEmbeddingsModel model, ThreadPool threadPool) {
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
        CohereEmbeddingsRequest request = new CohereEmbeddingsRequest(docsInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
