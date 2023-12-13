/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.util.List;
import java.util.Objects;

public class OpenAiEmbeddingsRequestCreator implements RequestCreator<OpenAiAccount> {

    private static final Logger logger = LogManager.getLogger(OpenAiEmbeddingsRequestCreator.class);

    private final OpenAiEmbeddingsModel model;
    private final OpenAiAccount account;
    private final ResponseHandler responseHandler;

    public OpenAiEmbeddingsRequestCreator(OpenAiEmbeddingsModel model, OpenAiAccount account, ResponseHandler responseHandler) {
        this.model = Objects.requireNonNull(model);
        this.account = Objects.requireNonNull(account);
        this.responseHandler = Objects.requireNonNull(responseHandler);
    }

    @Override
    public Runnable createRequest(
        List<String> input,
        BatchingComponents components,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        logger.warn(Strings.format("OpenAI request input array size: %s", input.size()));
        var request = new OpenAiEmbeddingsRequest(
            account,
            new OpenAiEmbeddingsRequestEntity(input, model.getTaskSettings().model(), model.getTaskSettings().user())
        );

        return () -> components.retrier().send(logger, request.createRequest(), context, responseHandler, listener);
    }

    @Override
    public OpenAiAccount key() {
        return account;
    }
}
