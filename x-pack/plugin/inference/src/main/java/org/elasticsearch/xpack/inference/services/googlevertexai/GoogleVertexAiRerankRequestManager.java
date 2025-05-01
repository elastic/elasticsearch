/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiRerankRequest;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.response.GoogleVertexAiRerankResponseEntity;

import java.util.Objects;
import java.util.function.Supplier;

public class GoogleVertexAiRerankRequestManager extends GoogleVertexAiRequestManager {

    private static final Logger logger = LogManager.getLogger(GoogleVertexAiRerankRequestManager.class);

    private static final ResponseHandler HANDLER = createGoogleVertexAiResponseHandler();

    private static ResponseHandler createGoogleVertexAiResponseHandler() {
        return new GoogleVertexAiResponseHandler(
            "Google Vertex AI rerank",
            (request, response) -> GoogleVertexAiRerankResponseEntity.fromResponse(response)
        );
    }

    public static GoogleVertexAiRerankRequestManager of(GoogleVertexAiRerankModel model, ThreadPool threadPool) {
        return new GoogleVertexAiRerankRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final GoogleVertexAiRerankModel model;

    private GoogleVertexAiRerankRequestManager(GoogleVertexAiRerankModel model, ThreadPool threadPool) {
        super(threadPool, model, RateLimitGrouping.of(model));
        this.model = model;
    }

    record RateLimitGrouping(int projectIdHash) {
        public static RateLimitGrouping of(GoogleVertexAiRerankModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.rateLimitServiceSettings().projectId().hashCode());
        }
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var rerankInput = QueryAndDocsInputs.of(inferenceInputs);
        GoogleVertexAiRerankRequest request = new GoogleVertexAiRerankRequest(
            rerankInput.getQuery(),
            rerankInput.getChunks(),
            rerankInput.getReturnDocuments(),
            rerankInput.getTopN(),
            model
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
