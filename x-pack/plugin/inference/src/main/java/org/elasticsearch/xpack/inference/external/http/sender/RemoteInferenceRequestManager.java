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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.function.Supplier;

public class RemoteInferenceRequestManager extends BaseRequestManager {

    private static final Logger logger = LogManager.getLogger(RemoteInferenceRequestManager.class);
    private final Request request;
    private final ResponseHandler responseHandler;

    public RemoteInferenceRequestManager(
        ThreadPool threadPool,
        String inferenceEntityId,
        Object rateLimitGroup,
        RateLimitSettings rateLimitSettings,
        Request request,
        ResponseHandler responseHandler
    ) {
        super(threadPool, inferenceEntityId, rateLimitGroup, rateLimitSettings);
        this.request = request;
        this.responseHandler = responseHandler;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        execute(new ExecutableInferenceRequest(requestSender, logger, request, responseHandler, hasRequestCompletedFunction, listener));
    }
}
