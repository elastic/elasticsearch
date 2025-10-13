/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.request.GoogleAiStudioCompletionRequest;
import org.elasticsearch.xpack.inference.services.googleaistudio.response.GoogleAiStudioCompletionResponseEntity;

import java.util.Objects;
import java.util.function.Supplier;

public class GoogleAiStudioCompletionRequestManager extends GoogleAiStudioRequestManager {

    private static final Logger logger = LogManager.getLogger(GoogleAiStudioCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private final GoogleAiStudioCompletionModel model;

    private static ResponseHandler createCompletionHandler() {
        return new GoogleAiStudioResponseHandler(
            "google ai studio completion",
            GoogleAiStudioCompletionResponseEntity::fromResponse,
            true,
            GoogleAiStudioCompletionResponseEntity::content
        );
    }

    public GoogleAiStudioCompletionRequestManager(GoogleAiStudioCompletionModel model, ThreadPool threadPool) {
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
        GoogleAiStudioCompletionRequest request = new GoogleAiStudioCompletionRequest(
            inferenceInputs.castTo(ChatCompletionInput.class),
            model
        );
        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
