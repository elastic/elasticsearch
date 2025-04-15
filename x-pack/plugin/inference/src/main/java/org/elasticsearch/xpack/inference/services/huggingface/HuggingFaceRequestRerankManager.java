/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.services.huggingface.request.HuggingFaceInferenceRerankRequest;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class HuggingFaceRequestRerankManager extends BaseRequestManager {
    private static final Logger logger = LogManager.getLogger(HuggingFaceRequestRerankManager.class);

    public static HuggingFaceRequestRerankManager of(
        HuggingFaceModel model,
        ResponseHandler responseHandler,
        Truncator truncator,
        ThreadPool threadPool
    ) {
        return new HuggingFaceRequestRerankManager(
            Objects.requireNonNull(model),
            Objects.requireNonNull(responseHandler),
            Objects.requireNonNull(truncator),
            Objects.requireNonNull(threadPool)
        );
    }

    private final HuggingFaceModel model;
    private final ResponseHandler responseHandler;
    private final Truncator truncator;

    private HuggingFaceRequestRerankManager(HuggingFaceModel model, ResponseHandler responseHandler, Truncator truncator, ThreadPool threadPool) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
        this.model = model;
        this.responseHandler = responseHandler;
        this.truncator = truncator;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        List<String> docsInput = QueryAndDocsInputs.of(inferenceInputs).getChunks();
        var truncatedInput = truncate(docsInput, model.getTokenLimit());
        var request = new HuggingFaceInferenceRerankRequest(truncator, truncatedInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, responseHandler, hasRequestCompletedFunction, listener));
    }

    record RateLimitGrouping(int accountHash) {

        public static RateLimitGrouping of(HuggingFaceModel model) {
            return new RateLimitGrouping(new HuggingFaceAccount(model.rateLimitServiceSettings().uri(), model.apiKey()).hashCode());
        }
    }
}
