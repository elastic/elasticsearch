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
import org.elasticsearch.xpack.inference.external.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.external.alibabacloudsearch.AlibabaCloudSearchResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.alibabacloudsearch.AlibabaCloudSearchEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class AlibabaCloudSearchEmbeddingsRequestManager extends AlibabaCloudSearchRequestManager {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchEmbeddingsRequestManager.class);

    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new AlibabaCloudSearchResponseHandler(
            "alibaba cloud search text embedding",
            AlibabaCloudSearchEmbeddingsResponseEntity::fromResponse
        );
    }

    public static AlibabaCloudSearchEmbeddingsRequestManager of(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchEmbeddingsModel model,
        ThreadPool threadPool
    ) {
        return new AlibabaCloudSearchEmbeddingsRequestManager(
            Objects.requireNonNull(account),
            Objects.requireNonNull(model),
            Objects.requireNonNull(threadPool)
        );
    }

    private final AlibabaCloudSearchEmbeddingsModel model;

    private final AlibabaCloudSearchAccount account;

    private AlibabaCloudSearchEmbeddingsRequestManager(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchEmbeddingsModel model,
        ThreadPool threadPool
    ) {
        super(threadPool, model);
        this.account = Objects.requireNonNull(account);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        List<String> input = DocumentsOnlyInput.of(inferenceInputs).getInputs();
        AlibabaCloudSearchEmbeddingsRequest request = new AlibabaCloudSearchEmbeddingsRequest(account, input, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
