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
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.huggingface.request.embeddings.HuggingFaceEmbeddingsRequest;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

/**
 * This class is responsible for managing requests to the Hugging Face API for generating embeddings.
 * It handles the execution of requests, including truncation of input data and response handling.
 */
public class HuggingFaceEmbeddingsRequestManager extends HuggingFaceRequestManager {
    private static final Logger logger = LogManager.getLogger(HuggingFaceEmbeddingsRequestManager.class);

    /**
     * Creates a new instance of HuggingFaceEmbeddingsRequestManager.
     *
     * @param model           The Hugging Face model to be used for generating embeddings.
     * @param responseHandler  The response handler for processing the API responses.
     * @param truncator       The truncator for handling input data truncation.
     * @param threadPool      The thread pool for executing requests.
     * @return A new instance of HuggingFaceEmbeddingsRequestManager.
     */
    public static HuggingFaceEmbeddingsRequestManager of(
        HuggingFaceModel model,
        ResponseHandler responseHandler,
        Truncator truncator,
        ThreadPool threadPool
    ) {
        return new HuggingFaceEmbeddingsRequestManager(
            Objects.requireNonNull(model),
            Objects.requireNonNull(responseHandler),
            Objects.requireNonNull(truncator),
            Objects.requireNonNull(threadPool)
        );
    }

    private final HuggingFaceModel model;
    private final ResponseHandler responseHandler;
    private final Truncator truncator;

    private HuggingFaceEmbeddingsRequestManager(
        HuggingFaceModel model,
        ResponseHandler responseHandler,
        Truncator truncator,
        ThreadPool threadPool
    ) {
        super(model, threadPool);
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
        List<String> docsInput = EmbeddingsInput.of(inferenceInputs).getStringInputs();
        var truncatedInput = truncate(docsInput, model.getTokenLimit());
        var request = new HuggingFaceEmbeddingsRequest(truncator, truncatedInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, responseHandler, hasRequestCompletedFunction, listener));
    }
}
