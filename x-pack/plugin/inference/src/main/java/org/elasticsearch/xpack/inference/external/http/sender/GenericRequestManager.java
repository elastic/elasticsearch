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
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This is a temporary class to use while we refactor all the request managers. After all the request managers extend
 * this class we'll move this functionality directly into the {@link BaseRequestManager}.
 */
public class GenericRequestManager<T extends InferenceInputs> extends BaseRequestManager {
    private static final Logger logger = LogManager.getLogger(GenericRequestManager.class);

    protected final ResponseHandler responseHandler;
    protected final Function<T, Request> requestCreator;
    protected final Class<T> inputType;

    public GenericRequestManager(
        ThreadPool threadPool,
        RateLimitGroupingModel rateLimitGroupingModel,
        ResponseHandler responseHandler,
        Function<T, Request> requestCreator,
        Class<T> inputType
    ) {
        super(threadPool, rateLimitGroupingModel);
        this.responseHandler = Objects.requireNonNull(responseHandler);
        this.requestCreator = Objects.requireNonNull(requestCreator);
        this.inputType = Objects.requireNonNull(inputType);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var request = requestCreator.apply(inferenceInputs.castTo(inputType));

        execute(new ExecutableInferenceRequest(requestSender, logger, request, responseHandler, hasRequestCompletedFunction, listener));
    }
}
