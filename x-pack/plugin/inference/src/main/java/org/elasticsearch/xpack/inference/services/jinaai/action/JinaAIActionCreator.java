/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIRerankRequestManager;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the jinaai model type.
 */
public class JinaAIActionCreator implements JinaAIActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public JinaAIActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(JinaAIEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = JinaAIEmbeddingsModel.of(model, taskSettings);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("JinaAI embeddings");
        var requestCreator = JinaAIEmbeddingsRequestManager.of(overriddenModel, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(JinaAIRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = JinaAIRerankModel.of(model, taskSettings);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("JinaAI rerank");
        var requestCreator = JinaAIRerankRequestManager.of(overriddenModel, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }
}
