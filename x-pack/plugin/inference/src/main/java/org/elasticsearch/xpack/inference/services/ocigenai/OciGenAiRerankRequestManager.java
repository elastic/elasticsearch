/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiRerankRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModel;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiRerankResponseEntity;

import java.util.Objects;
import java.util.function.Supplier;

public class OciGenAiRerankRequestManager extends OciGenAiRequestManager {

    private static final Logger logger = LogManager.getLogger(OciGenAiRerankRequestManager.class);

    private static final ResponseHandler HANDLER = new OciGenAiResponseHandler(
        "OCI Generative AI rerank",
        (request, response) -> OciGenAiRerankResponseEntity.fromResponse(response)
    );

    private final OciGenAiRerankModel model;

    public OciGenAiRerankRequestManager(OciGenAiRerankModel model, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var rerankInput = inferenceInputs.castTo(QueryAndDocsInputs.class);
        var request = new OciGenAiRerankRequest(
            rerankInput.getQueryAsString(),
            rerankInput.getDocsAsStrings(),
            rerankInput.getTopN(),
            rerankInput.getReturnDocuments(),
            model
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
