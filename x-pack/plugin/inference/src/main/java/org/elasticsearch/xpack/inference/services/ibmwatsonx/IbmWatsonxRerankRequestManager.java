/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

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
import org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxRerankRequest;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxRankedResponseEntity;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class IbmWatsonxRerankRequestManager extends IbmWatsonxRequestManager {
    private static final Logger logger = LogManager.getLogger(IbmWatsonxRerankRequestManager.class);
    private static final ResponseHandler HANDLER = createIbmWatsonxResponseHandler();

    private static ResponseHandler createIbmWatsonxResponseHandler() {
        return new IbmWatsonxResponseHandler(
            "IBM Watsonx rerank",
            (request, response) -> IbmWatsonxRankedResponseEntity.fromResponse(response)
        );
    }

    public static IbmWatsonxRerankRequestManager of(IbmWatsonxRerankModel model, ThreadPool threadPool) {
        return new IbmWatsonxRerankRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final IbmWatsonxRerankModel model;

    public IbmWatsonxRerankRequestManager(IbmWatsonxRerankModel model, ThreadPool threadPool) {
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

        execute(
            new ExecutableInferenceRequest(
                requestSender,
                logger,
                getRerankRequest(rerankInput.getQuery(), rerankInput.getChunks(), model),
                HANDLER,
                hasRequestCompletedFunction,
                listener
            )
        );
    }

    protected IbmWatsonxRerankRequest getRerankRequest(String query, List<String> chunks, IbmWatsonxRerankModel model) {
        return new IbmWatsonxRerankRequest(query, chunks, model);
    }
}
