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
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchRerankRequest;
import org.elasticsearch.xpack.inference.external.response.alibabacloudsearch.AlibabaCloudSearchRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankModel;

import java.util.Objects;
import java.util.function.Supplier;

public class AlibabaCloudSearchRerankRequestManager extends AlibabaCloudSearchRequestManager {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchRerankRequestManager.class);
    private static final ResponseHandler HANDLER = createRerankHandler();

    private static ResponseHandler createRerankHandler() {
        return new AlibabaCloudSearchResponseHandler("alibaba cloud search rerank", AlibabaCloudSearchRerankResponseEntity::fromResponse);
    }

    public static AlibabaCloudSearchRerankRequestManager of(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchRerankModel model,
        ThreadPool threadPool
    ) {
        return new AlibabaCloudSearchRerankRequestManager(
            Objects.requireNonNull(account),
            Objects.requireNonNull(model),
            Objects.requireNonNull(threadPool)
        );
    }

    private final AlibabaCloudSearchRerankModel model;

    private final AlibabaCloudSearchAccount account;

    private AlibabaCloudSearchRerankRequestManager(
        AlibabaCloudSearchAccount account,
        AlibabaCloudSearchRerankModel model,
        ThreadPool threadPool
    ) {
        super(threadPool, model);
        this.account = account;
        this.model = model;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var rerankInput = QueryAndDocsInputs.of(inferenceInputs);
        AlibabaCloudSearchRerankRequest request = new AlibabaCloudSearchRerankRequest(
            account,
            rerankInput.getQuery(),
            rerankInput.getChunks(),
            model
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
