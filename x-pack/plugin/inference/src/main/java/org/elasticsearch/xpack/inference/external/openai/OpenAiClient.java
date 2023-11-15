/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.retry.AlwaysRetryingResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.io.IOException;
import java.util.List;

public class OpenAiClient {
    private static final Logger logger = LogManager.getLogger(OpenAiClient.class);
    private static final ResponseHandler EMBEDDINGS_HANDLER = createEmbeddingsHandler();

    private final RetryingHttpSender sender;

    public OpenAiClient(Sender sender, ServiceComponents serviceComponents) {
        this.sender = new RetryingHttpSender(
            sender,
            serviceComponents.throttlerManager(),
            logger,
            new RetrySettings(serviceComponents.settings()),
            serviceComponents.threadPool()
        );
    }

    public void send(OpenAiEmbeddingsRequest request, ActionListener<List<? extends InferenceResults>> listener) throws IOException {
        sender.send(request.createRequest(), EMBEDDINGS_HANDLER, listener);
    }

    private static ResponseHandler createEmbeddingsHandler() {
        return new AlwaysRetryingResponseHandler(
            "openai text embedding",
            // TODO this is a hack to get the response to fit within List<InferenceResults> and will be addressed in a follow up PR
            result -> List.of(OpenAiEmbeddingsResponseEntity.fromResponse(result))
        );
    }
}
