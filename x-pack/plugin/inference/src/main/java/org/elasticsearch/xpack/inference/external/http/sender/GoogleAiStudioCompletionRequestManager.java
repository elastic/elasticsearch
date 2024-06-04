/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.googleaistudio.GoogleAiStudioResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.googleaistudio.GoogleAiStudioCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.googleaistudio.GoogleAiStudioCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class GoogleAiStudioCompletionRequestManager extends GoogleAiStudioRequestManager {

    private static final Logger logger = LogManager.getLogger(GoogleAiStudioCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private final GoogleAiStudioCompletionModel model;

    private static ResponseHandler createCompletionHandler() {
        return new GoogleAiStudioResponseHandler("google ai studio completion", GoogleAiStudioCompletionResponseEntity::fromResponse);
    }

    public GoogleAiStudioCompletionRequestManager(GoogleAiStudioCompletionModel model, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public Runnable create(
        String query,
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        GoogleAiStudioCompletionRequest request = new GoogleAiStudioCompletionRequest(input, model);
        return new ExecutableInferenceRequest(requestSender, logger, request, context, HANDLER, hasRequestCompletedFunction, listener);
    }
}
