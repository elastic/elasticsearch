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
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceInferenceRequest;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceInferenceRequestEntity;

import java.util.List;
import java.util.Objects;

public class HuggingFaceInferenceRequestCreator implements RequestCreator<HuggingFaceAccount> {

    private static final Logger logger = LogManager.getLogger(HuggingFaceInferenceRequestCreator.class);

    private final HuggingFaceAccount account;
    private final ResponseHandler responseHandler;

    public HuggingFaceInferenceRequestCreator(HuggingFaceAccount account, ResponseHandler responseHandler) {
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
        var request = new HuggingFaceInferenceRequest(account, new HuggingFaceInferenceRequestEntity(input));

        return () -> components.retrier().send(logger, request.createRequest(), context, responseHandler, listener);
    }

    @Override
    public HuggingFaceAccount key() {
        return account;
    }
}
