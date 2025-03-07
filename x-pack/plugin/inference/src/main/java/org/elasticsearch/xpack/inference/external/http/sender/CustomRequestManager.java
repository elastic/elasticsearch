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
import org.elasticsearch.xpack.inference.external.custom.CustomResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.custom.CustomRequest;
import org.elasticsearch.xpack.inference.external.response.custom.CustomResponseEntity;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class CustomRequestManager extends BaseRequestManager {
    private static final Logger logger = LogManager.getLogger(CustomRequestManager.class);

    private static final ResponseHandler HANDLER = createCustomHandler();

    record RateLimitGrouping(int apiKeyHash) {
        public static RateLimitGrouping of(CustomModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.rateLimitServiceSettings().hashCode());
        }
    }

    private static ResponseHandler createCustomHandler() {
        return new CustomResponseHandler("custom model", CustomResponseEntity::fromResponse);
    }

    public static CustomRequestManager of(CustomModel model, ThreadPool threadPool) {
        return new CustomRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final CustomModel model;

    private CustomRequestManager(CustomModel model, ThreadPool threadPool) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
        this.model = model;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        String query;
        List<String> input;
        if (inferenceInputs instanceof QueryAndDocsInputs) {
            QueryAndDocsInputs queryAndDocsInputs = QueryAndDocsInputs.of(inferenceInputs);
            query = queryAndDocsInputs.getQuery();
            input = queryAndDocsInputs.getChunks();
        } else if (inferenceInputs instanceof ChatCompletionInput chatInputs) {
            query = null;
            input = chatInputs.getInputs();
        } else if (inferenceInputs instanceof DocumentsOnlyInput) {
            DocumentsOnlyInput docsInputs = DocumentsOnlyInput.of(inferenceInputs);
            query = null;
            input = docsInputs.getInputs();
        } else {
            throw InferenceInputs.createUnsupportedTypeException(inferenceInputs, InferenceInputs.class);
        }
        CustomRequest request = new CustomRequest(query, input, model);
        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
