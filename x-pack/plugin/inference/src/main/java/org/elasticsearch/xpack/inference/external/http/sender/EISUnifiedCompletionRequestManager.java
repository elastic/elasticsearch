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
import org.elasticsearch.xpack.inference.external.elastic.EISUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.elastic.EISUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.Objects;
import java.util.function.Supplier;

public class EISUnifiedCompletionRequestManager extends ElasticInferenceServiceRequestManager {

    private static final Logger logger = LogManager.getLogger(EISUnifiedCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    public static EISUnifiedCompletionRequestManager of(
        ElasticInferenceServiceCompletionModel model,
        ThreadPool threadPool,
        TraceContext traceContext
    ) {
        return new EISUnifiedCompletionRequestManager(
            Objects.requireNonNull(model),
            Objects.requireNonNull(threadPool),
            Objects.requireNonNull(traceContext)
        );
    }

    private final ElasticInferenceServiceCompletionModel model;
    private final TraceContext traceContext;

    private EISUnifiedCompletionRequestManager(
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

        EISUnifiedChatCompletionRequest request = new EISUnifiedChatCompletionRequest(
            inferenceInputs.castTo(UnifiedChatInput.class),
            model,
            traceContext
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }

    private static ResponseHandler createCompletionHandler() {
        return new EISUnifiedChatCompletionResponseHandler("eis completion", OpenAiChatCompletionResponseEntity::fromResponse);
    }
}
