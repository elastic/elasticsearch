/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiResponseHandler;
import org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequest;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;
import org.elasticsearch.xpack.inference.services.contextualai.response.ContextualAiRerankResponseEntity;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the ContextualAI model type.
 */
public class ContextualAiActionCreator implements ContextualAiActionVisitor {

    private static final ResponseHandler RERANK_HANDLER = new ContextualAiResponseHandler(
        "contextualai rerank",
        (request, response) -> ContextualAiRerankResponseEntity.fromResponse((ContextualAiRerankRequest) request, response),
        false
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public ContextualAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(ContextualAiRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = ContextualAiRerankModel.of(model, taskSettings);

        Function<QueryAndDocsInputs, Request> requestCreator = rerankInput -> new ContextualAiRerankRequest(
            rerankInput.getQuery(),
            rerankInput.getChunks(),
            rerankInput.getTopN(),
            overriddenModel.getTaskSettings().getInstruction(),
            overriddenModel
        );

        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            requestCreator,
            QueryAndDocsInputs.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("ContextualAI rerank");
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }
}
