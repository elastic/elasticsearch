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
import org.elasticsearch.xpack.inference.external.googleaistudio.GoogleAiStudioResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.googleaistudio.GoogleAiStudioEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.googleaistudio.GoogleAiStudioEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class GoogleAiStudioEmbeddingsRequestManager extends GoogleAiStudioRequestManager {

    private static final Logger logger = LogManager.getLogger(GoogleAiStudioEmbeddingsRequestManager.class);

    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new GoogleAiStudioResponseHandler("google ai studio embeddings", GoogleAiStudioEmbeddingsResponseEntity::fromResponse);
    }

    private final GoogleAiStudioEmbeddingsModel model;

    private final Truncator truncator;

    public GoogleAiStudioEmbeddingsRequestManager(GoogleAiStudioEmbeddingsModel model, Truncator truncator, ThreadPool threadPool) {
        super(threadPool, model);
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
        GoogleAiStudioEmbeddingsRequest request = new GoogleAiStudioEmbeddingsRequest(truncator, truncatedInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
