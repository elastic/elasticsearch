/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.external.cohere.CohereResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.cohere.CohereEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.cohere.CohereEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class CohereEmbeddingsExecutableRequestCreator implements ExecutableRequestCreator {
    private static final Logger logger = LogManager.getLogger(CohereEmbeddingsExecutableRequestCreator.class);
    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new CohereResponseHandler("cohere text embedding", CohereEmbeddingsResponseEntity::fromResponse);
    }

    private final CohereAccount account;
    private final CohereEmbeddingsModel model;

    public CohereEmbeddingsExecutableRequestCreator(CohereEmbeddingsModel model) {
        this.model = Objects.requireNonNull(model);
        account = new CohereAccount(this.model.getServiceSettings().getCommonSettings().getUri(), this.model.getSecretSettings().apiKey());
    }

    @Override
    public Runnable create(
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        CohereEmbeddingsRequest request = new CohereEmbeddingsRequest(account, input, model);

        return new ExecutableInferenceRequest(requestSender, logger, request, context, HANDLER, hasRequestCompletedFunction, listener);
    }
}
