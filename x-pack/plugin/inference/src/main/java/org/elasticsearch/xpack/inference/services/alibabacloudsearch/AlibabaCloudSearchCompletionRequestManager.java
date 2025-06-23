/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

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
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.completion.AlibabaCloudSearchCompletionRequest;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.response.AlibabaCloudSearchCompletionResponseEntity;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class AlibabaCloudSearchCompletionRequestManager extends AlibabaCloudSearchRequestManager {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private static ResponseHandler createCompletionHandler() {
        return new AlibabaCloudSearchResponseHandler(
            "alibaba cloud search completion",
            AlibabaCloudSearchCompletionResponseEntity::fromResponse
        );
    }

    public static AlibabaCloudSearchCompletionRequestManager of(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchCompletionModel model,
        ThreadPool threadPool
    ) {
        return new AlibabaCloudSearchCompletionRequestManager(
            Objects.requireNonNull(account),
            Objects.requireNonNull(model),
            Objects.requireNonNull(threadPool)
        );
    }

    private final AlibabaCloudSearchCompletionModel model;

    private final AlibabaCloudSearchAccount account;

    private AlibabaCloudSearchCompletionRequestManager(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchCompletionModel model,
        ThreadPool threadPool
    ) {
        super(threadPool, model);
        this.account = Objects.requireNonNull(account);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        List<String> input = inferenceInputs.castTo(ChatCompletionInput.class).getInputs();
        AlibabaCloudSearchCompletionRequest request = new AlibabaCloudSearchCompletionRequest(account, input, model);
        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
