/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.Objects;
import java.util.function.Supplier;

public class ElasticInferenceServiceUnifiedCompletionRequestManager extends ElasticInferenceServiceRequestManager {

    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceUnifiedCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    public static ElasticInferenceServiceUnifiedCompletionRequestManager of(
        ElasticInferenceServiceCompletionModel model,
        ThreadPool threadPool,
        TraceContext traceContext
    ) {
        return new ElasticInferenceServiceUnifiedCompletionRequestManager(
            Objects.requireNonNull(model),
            Objects.requireNonNull(threadPool),
            Objects.requireNonNull(traceContext)
        );
    }

    private final ElasticInferenceServiceCompletionModel model;
    private final TraceContext traceContext;

    private ElasticInferenceServiceUnifiedCompletionRequestManager(
        ElasticInferenceServiceCompletionModel model,
        ThreadPool threadPool,
        TraceContext traceContext
    ) {
        super(threadPool, model);
        this.model = model;
        this.traceContext = traceContext;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {

        ElasticInferenceServiceUnifiedChatCompletionRequest request = new ElasticInferenceServiceUnifiedChatCompletionRequest(
            inferenceInputs.castTo(UnifiedChatInput.class),
            model,
            traceContext,
            requestMetadata()
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }

    private static ResponseHandler createCompletionHandler() {
        return new ElasticInferenceServiceUnifiedChatCompletionResponseHandler(
            "elastic inference service completion",
            // We use OpenAiChatCompletionResponseEntity here as the ElasticInferenceServiceResponseEntity fields are a subset of the OpenAI
            // one.
            OpenAiChatCompletionResponseEntity::fromResponse
        );
    }
}
