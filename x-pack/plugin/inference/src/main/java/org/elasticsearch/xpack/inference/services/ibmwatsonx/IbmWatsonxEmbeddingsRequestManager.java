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
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxEmbeddingsResponseEntity;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class IbmWatsonxEmbeddingsRequestManager extends IbmWatsonxRequestManager {

    private static final Logger logger = LogManager.getLogger(IbmWatsonxEmbeddingsRequestManager.class);

    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new IbmWatsonxResponseHandler("ibm watsonx embeddings", IbmWatsonxEmbeddingsResponseEntity::fromResponse);
    }

    private final IbmWatsonxEmbeddingsModel model;

    private final Truncator truncator;

    public IbmWatsonxEmbeddingsRequestManager(IbmWatsonxEmbeddingsModel model, Truncator truncator, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = Objects.requireNonNull(model);
        this.truncator = Objects.requireNonNull(truncator);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        List<String> docsInput = EmbeddingsInput.of(inferenceInputs).getStringInputs();
        var truncatedInput = truncate(docsInput, model.getServiceSettings().maxInputTokens());

        execute(
            new ExecutableInferenceRequest(
                requestSender,
                logger,
                getEmbeddingRequest(truncator, truncatedInput, model),
                HANDLER,
                hasRequestCompletedFunction,
                listener
            )
        );
    }

    protected IbmWatsonxEmbeddingsRequest getEmbeddingRequest(
        Truncator truncator,
        Truncator.TruncationResult truncatedInput,
        IbmWatsonxEmbeddingsModel model
    ) {
        return new IbmWatsonxEmbeddingsRequest(truncator, truncatedInput, model);
    }
}
