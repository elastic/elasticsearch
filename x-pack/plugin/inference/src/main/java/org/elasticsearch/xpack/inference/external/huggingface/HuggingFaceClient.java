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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.DefaultResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceElserResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;

public class HuggingFaceClient {
    private static final Logger logger = LogManager.getLogger(HuggingFaceClient.class);
    private static final ResponseHandler ELSER_RESPONSE_HANDLER = createElserHandler();

    private final RetryingHttpSender sender;

    // TODO remove after RetrySettings is plumbed all the way through the InterfacePlugin
    public HuggingFaceClient(Sender sender, ThrottlerManager throttlerManager) {
        this(sender, throttlerManager, new RetrySettings(5, TimeValue.timeValueSeconds(1)));
    }

    public HuggingFaceClient(Sender sender, ThrottlerManager throttlerManager, RetrySettings retrySettings) {
        this.sender = new RetryingHttpSender(sender, throttlerManager, logger, retrySettings);
    }

    public void send(HuggingFaceElserRequest request, ActionListener<InferenceResults> listener) throws IOException {
        this.sender.send(request.createRequest(), ELSER_RESPONSE_HANDLER, listener);
    }

    private static ResponseHandler createElserHandler() {
        return new DefaultResponseHandler("elser hugging face") {
            @Override
            public InferenceResults parseResult(HttpResult result) throws IOException {
                return HuggingFaceElserResponseEntity.fromResponse(result);
            }
        };
    }
}
