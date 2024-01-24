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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceInferenceRequest;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class HuggingFaceInferenceRequestCreator implements RequestCreator<HuggingFaceAccount> {

    private static final Logger logger = LogManager.getLogger(HuggingFaceInferenceRequestCreator.class);

    private final HuggingFaceAccount account;
    private final ResponseHandler responseHandler;
    private final Truncator truncator;
    private final Integer tokenLimit;

    public HuggingFaceInferenceRequestCreator(
        HuggingFaceAccount account,
        ResponseHandler responseHandler,
        Truncator truncator,
        @Nullable Integer tokenLimit
    ) {
        this.account = Objects.requireNonNull(account);
        this.responseHandler = Objects.requireNonNull(responseHandler);
        this.truncator = Objects.requireNonNull(truncator);
        this.tokenLimit = tokenLimit;
    }

    @Override
    public Runnable createRequest(
        List<String> input,
        BatchingComponents components,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        var truncatedInput = truncate(input, tokenLimit);
        var request = new HuggingFaceInferenceRequest(truncator, account, truncatedInput);

        return new RunnableRequest(components, logger, request, context, responseHandler, listener);
    }

    @Override
    public HuggingFaceAccount key() {
        return account;
    }
}
