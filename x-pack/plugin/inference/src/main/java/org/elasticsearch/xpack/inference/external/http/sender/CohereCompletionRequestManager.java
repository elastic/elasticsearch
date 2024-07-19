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
import org.elasticsearch.xpack.inference.external.request.cohere.completion.CohereCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.cohere.CohereCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class CohereCompletionRequestManager extends CohereRequestManager {

    private static final Logger logger = LogManager.getLogger(CohereCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private static ResponseHandler createCompletionHandler() {
        return new CohereResponseHandler("cohere completion", CohereCompletionResponseEntity::fromResponse);
    }

    public static CohereCompletionRequestManager of(CohereCompletionModel model, ThreadPool threadPool) {
        return new CohereCompletionRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final CohereCompletionModel model;

    private CohereCompletionRequestManager(CohereCompletionModel model, ThreadPool threadPool) {
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
        CohereCompletionRequest request = new CohereCompletionRequest(docsInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
