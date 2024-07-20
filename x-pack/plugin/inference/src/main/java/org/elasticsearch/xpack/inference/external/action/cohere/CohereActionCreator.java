/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.cohere;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.CohereCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.CohereEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.CohereRerankRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the cohere model type.
 */
public class CohereActionCreator implements CohereActionVisitor {
    private static final String COMPLETION_ERROR_PREFIX = "Cohere completion";
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public CohereActionCreator(Sender sender, ServiceComponents serviceComponents) {
        // TODO Batching - accept a class that can handle batching
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(CohereEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        var overriddenModel = CohereEmbeddingsModel.of(model, taskSettings, inputType);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(
            overriddenModel.getServiceSettings().getCommonSettings().uri(),
            "Cohere embeddings"
        );
        // TODO - Batching pass the batching class on to the CohereEmbeddingsRequestManager
        var requestCreator = CohereEmbeddingsRequestManager.of(overriddenModel, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(CohereRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = CohereRerankModel.of(model, taskSettings);
        var requestCreator = CohereRerankRequestManager.of(overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(
            overriddenModel.getServiceSettings().uri(),
            "Cohere rerank"
        );
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(CohereCompletionModel model, Map<String, Object> taskSettings) {
        // no overridden model as task settings are always empty for cohere completion model
        var requestManager = CohereCompletionRequestManager.of(model, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(
            model.getServiceSettings().uri(),
            COMPLETION_ERROR_PREFIX
        );
        return new SingleInputSenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage, COMPLETION_ERROR_PREFIX);
    }
}
