/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequestEntity;

import java.util.List;
import java.util.Objects;

/**
 * This must be constructed in the HuggingFaceAction
 */
public class HuggingFaceElserRequestCreator implements RequestCreator<HuggingFaceAccount> {

    private static final Logger logger = LogManager.getLogger(HuggingFaceElserRequestCreator.class);

    private final HuggingFaceAccount account;
    private final ResponseHandler2 responseHandler;

    public HuggingFaceElserRequestCreator(HuggingFaceAccount account, ResponseHandler2 responseHandler) {
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
        var elserRequest = new HuggingFaceElserRequest(account, new HuggingFaceElserRequestEntity(input));

        return components.threadPool()
            .getThreadContext()
            .preserveContext(new Command(components, context, elserRequest.createRequest(), responseHandler, listener));
    }

    @Override
    public HuggingFaceAccount key() {
        return account;
    }

    private record Command(
        BatchingComponents components,
        HttpClientContext context,
        HttpRequestBase request,
        ResponseHandler2 responseHandler,
        ActionListener<InferenceServiceResults> listener
    ) implements Runnable {
        @Override
        public void run() {
            components.retryingHttpSender().send(logger, request, context, responseHandler, listener);
        }
    }
}
