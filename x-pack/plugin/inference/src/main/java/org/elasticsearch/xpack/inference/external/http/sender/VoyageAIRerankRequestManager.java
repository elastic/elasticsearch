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
import org.elasticsearch.xpack.inference.external.request.voyageai.VoyageAIRerankRequest;
import org.elasticsearch.xpack.inference.external.response.voyageai.VoyageAIRerankResponseEntity;
import org.elasticsearch.xpack.inference.external.voyageai.VoyageAIResponseHandler;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;

import java.util.Objects;
import java.util.function.Supplier;

public class VoyageAIRerankRequestManager extends VoyageAIRequestManager {
    private static final Logger logger = LogManager.getLogger(VoyageAIRerankRequestManager.class);
    private static final ResponseHandler HANDLER = createVoyageAIResponseHandler();

    private static ResponseHandler createVoyageAIResponseHandler() {
        return new VoyageAIResponseHandler("voyageai rerank", (request, response) -> VoyageAIRerankResponseEntity.fromResponse(response));
    }

    public static VoyageAIRerankRequestManager of(VoyageAIRerankModel model, ThreadPool threadPool) {
        return new VoyageAIRerankRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final VoyageAIRerankModel model;

    private VoyageAIRerankRequestManager(VoyageAIRerankModel model, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = model;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var rerankInput = QueryAndDocsInputs.of(inferenceInputs);
        VoyageAIRerankRequest request = new VoyageAIRerankRequest(rerankInput.getQuery(), rerankInput.getChunks(), model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
