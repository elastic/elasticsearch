/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.huggingface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.retry.AlwaysRetryingResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceElserResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.io.IOException;
import java.util.List;

public class HuggingFaceClient {
    private static final Logger logger = LogManager.getLogger(HuggingFaceClient.class);
    private static final ResponseHandler ELSER_RESPONSE_HANDLER = createElserHandler();

    private final RetryingHttpSender sender;

    public HuggingFaceClient(Sender sender, ServiceComponents serviceComponents) {
        this.sender = new RetryingHttpSender(
            sender,
            serviceComponents.throttlerManager(),
            logger,
            new RetrySettings(serviceComponents.settings()),
            serviceComponents.threadPool()
        );
    }

    public void send(HuggingFaceElserRequest request, ActionListener<List<? extends InferenceResults>> listener) throws IOException {
        this.sender.send(request.createRequest(), ELSER_RESPONSE_HANDLER, listener);
    }

    private static ResponseHandler createElserHandler() {
        return new AlwaysRetryingResponseHandler("elser hugging face", HuggingFaceElserResponseEntity::fromResponse);
    }
}
