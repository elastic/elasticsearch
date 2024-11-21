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
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.ibmwatsonx.IbmWatsonxResponseHandler;
import org.elasticsearch.xpack.inference.external.request.ibmwatsonx.IbmWatsonxRerankRequest;
import org.elasticsearch.xpack.inference.external.response.ibmwatsonx.IbmWatsonxRankedResponseEntity;

import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class IbmWatsonxRerankRequestManager extends IbmWatsonxRequestManager {
    private static final Logger logger = LogManager.getLogger(IbmWatsonxRerankRequestManager.class);
    private static final ResponseHandler HANDLER = createIbmWatsonxResponseHandler();

    private static ResponseHandler createIbmWatsonxResponseHandler() {
        return new IbmWatsonxResponseHandler("ibm watsonx rerank", (request, response) -> IbmWatsonxRankedResponseEntity.fromResponse(response), false);
    }

    public static IbmWatsonxRerankRequestManager of(IbmWatsonxRerankModel model, Truncator truncator, ThreadPool threadPool) {
        return new IbmWatsonxRerankRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(truncator), Objects.requireNonNull(threadPool));
    }

    private final IbmWatsonxRerankModel model;
    private final Truncator truncator;

    private IbmWatsonxRerankRequestManager(IbmWatsonxRerankModel model, Truncator truncator, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = model;
        this.truncator = Objects.requireNonNull(truncator);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        List<String> docsInput = DocumentsOnlyInput.of(inferenceInputs).getInputs();
        var truncatedInput = truncate(docsInput, model.getServiceSettings().maxInputTokens());

        IbmWatsonxRerankRequest request = new IbmWatsonxRerankRequest(truncator, truncatedInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
