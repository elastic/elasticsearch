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
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceInferenceRequest;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class HuggingFaceExecutableRequestCreator implements ExecutableRequestCreator {
    private static final Logger logger = LogManager.getLogger(HuggingFaceExecutableRequestCreator.class);

    private final HuggingFaceModel model;
    private final HuggingFaceAccount account;
    private final ResponseHandler responseHandler;
    private final Truncator truncator;

    public HuggingFaceExecutableRequestCreator(HuggingFaceModel model, ResponseHandler responseHandler, Truncator truncator) {
        this.model = Objects.requireNonNull(model);
        account = new HuggingFaceAccount(model.getUri(), model.getApiKey());
        this.responseHandler = Objects.requireNonNull(responseHandler);
        this.truncator = Objects.requireNonNull(truncator);
    }

    @Override
    public Runnable create(
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        var truncatedInput = truncate(input, model.getTokenLimit());
        var request = new HuggingFaceInferenceRequest(truncator, account, truncatedInput, model);

        return new ExecutableInferenceRequest(
            requestSender,
            logger,
            request,
            context,
            responseHandler,
            hasRequestCompletedFunction,
            listener
        );
    }
}
