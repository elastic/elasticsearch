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
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchSparseRequest;
import org.elasticsearch.xpack.inference.external.response.alibabacloudsearch.AlibabaCloudSearchSparseResponseEntity;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class AlibabaCloudSearchSparseRequestManager extends AlibabaCloudSearchRequestManager {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchSparseRequestManager.class);

    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new AlibabaCloudSearchResponseHandler(
            "alibaba cloud search sparse embedding",
            AlibabaCloudSearchSparseResponseEntity::fromResponse
        );
    }

    public static AlibabaCloudSearchSparseRequestManager of(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchSparseModel model,
        ThreadPool threadPool
    ) {
        return new AlibabaCloudSearchSparseRequestManager(
            Objects.requireNonNull(account),
            Objects.requireNonNull(model),
            Objects.requireNonNull(threadPool)
        );
    }

    private final AlibabaCloudSearchSparseModel model;

    private final AlibabaCloudSearchAccount account;

    private AlibabaCloudSearchSparseRequestManager(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchSparseModel model,
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
        AlibabaCloudSearchSparseRequest request = new AlibabaCloudSearchSparseRequest(account, input, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
