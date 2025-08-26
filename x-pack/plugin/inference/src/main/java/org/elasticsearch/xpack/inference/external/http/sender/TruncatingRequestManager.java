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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class TruncatingRequestManager extends BaseRequestManager {
    private static final Logger logger = LogManager.getLogger(TruncatingRequestManager.class);

    private final ResponseHandler responseHandler;
    private final Function<Truncator.TruncationResult, Request> requestCreator;
    private final Integer maxInputTokens;

    public TruncatingRequestManager(
        ThreadPool threadPool,
        RateLimitGroupingModel rateLimitGroupingModel,
        ResponseHandler responseHandler,
        Function<Truncator.TruncationResult, Request> requestCreator,
        @Nullable Integer maxInputTokens
    ) {
        super(threadPool, rateLimitGroupingModel);
        this.responseHandler = Objects.requireNonNull(responseHandler);
        this.requestCreator = Objects.requireNonNull(requestCreator);
        this.maxInputTokens = maxInputTokens;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var docsInput = inferenceInputs.castTo(EmbeddingsInput.class).getStringInputs();
        var truncatedInput = truncate(docsInput, maxInputTokens);
        var request = requestCreator.apply(truncatedInput);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, responseHandler, hasRequestCompletedFunction, listener));
    }
}
