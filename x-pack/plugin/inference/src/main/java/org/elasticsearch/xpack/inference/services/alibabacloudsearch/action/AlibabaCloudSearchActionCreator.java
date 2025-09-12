/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchRerankRequestManager;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchSparseRequestManager;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the alibaba cloud search model type.
 */
public class AlibabaCloudSearchActionCreator implements AlibabaCloudSearchActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AlibabaCloudSearchActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AlibabaCloudSearchEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AlibabaCloudSearchEmbeddingsModel.of(model, taskSettings);
        var account = new AlibabaCloudSearchAccount(overriddenModel.getSecretSettings().apiKey());
        var requestManager = AlibabaCloudSearchEmbeddingsRequestManager.of(account, overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("AlibabaCloud Search text embeddings");

        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(AlibabaCloudSearchSparseModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AlibabaCloudSearchSparseModel.of(model, taskSettings);
        var account = new AlibabaCloudSearchAccount(overriddenModel.getSecretSettings().apiKey());
        var requestManager = AlibabaCloudSearchSparseRequestManager.of(account, overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("AlibabaCloud Search sparse embeddings");

        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(AlibabaCloudSearchRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AlibabaCloudSearchRerankModel.of(model, taskSettings);
        var account = new AlibabaCloudSearchAccount(overriddenModel.getSecretSettings().apiKey());
        var requestManager = AlibabaCloudSearchRerankRequestManager.of(account, overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("AlibabaCloud Search rerank");

        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(AlibabaCloudSearchCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AlibabaCloudSearchCompletionModel.of(model, taskSettings);
        var account = new AlibabaCloudSearchAccount(overriddenModel.getSecretSettings().apiKey());
        var requestManager = AlibabaCloudSearchCompletionRequestManager.of(account, overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("AlibabaCloud Search completion");

        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }
}
