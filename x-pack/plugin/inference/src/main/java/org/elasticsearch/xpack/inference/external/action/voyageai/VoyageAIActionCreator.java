/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.voyageai;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.VoyageAIEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.VoyageAIRerankRequestManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the voyageai model type.
 */
public class VoyageAIActionCreator implements VoyageAIActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public VoyageAIActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(VoyageAIEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        var overriddenModel = VoyageAIEmbeddingsModel.of(model, taskSettings, inputType);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(overriddenModel.uri(), "VoyageAI embeddings");
        var requestCreator = VoyageAIEmbeddingsRequestManager.of(overriddenModel, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(VoyageAIRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = VoyageAIRerankModel.of(model, taskSettings);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(overriddenModel.uri(), "VoyageAI rerank");
        var requestCreator = VoyageAIRerankRequestManager.of(overriddenModel, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }
}
