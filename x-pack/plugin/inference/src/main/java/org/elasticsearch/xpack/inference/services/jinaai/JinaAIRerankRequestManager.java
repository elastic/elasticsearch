/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

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
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIRerankRequest;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;
import org.elasticsearch.xpack.inference.services.jinaai.response.JinaAIRerankResponseEntity;

import java.util.Objects;
import java.util.function.Supplier;

public class JinaAIRerankRequestManager extends JinaAIRequestManager {
    private static final Logger logger = LogManager.getLogger(JinaAIRerankRequestManager.class);
    private static final ResponseHandler HANDLER = createJinaAIResponseHandler();

    private static ResponseHandler createJinaAIResponseHandler() {
        return new JinaAIResponseHandler("jinaai rerank", (request, response) -> JinaAIRerankResponseEntity.fromResponse(response));
    }

    public static JinaAIRerankRequestManager of(JinaAIRerankModel model, ThreadPool threadPool) {
        return new JinaAIRerankRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final JinaAIRerankModel model;

    private JinaAIRerankRequestManager(JinaAIRerankModel model, ThreadPool threadPool) {
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
        JinaAIRerankRequest request = new JinaAIRerankRequest(
            rerankInput.getQuery(),
            rerankInput.getChunks(),
            rerankInput.getReturnDocuments(),
            rerankInput.getTopN(),
            model
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
