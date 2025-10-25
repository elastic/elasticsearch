/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.TruncatingRequestManager;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiResponseHandler;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.fireworksai.request.FireworksAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.fireworksai.request.FireworksAiRerankRequest;
import org.elasticsearch.xpack.inference.services.fireworksai.rerank.FireworksAiRerankModel;
import org.elasticsearch.xpack.inference.services.fireworksai.response.FireworksAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.fireworksai.response.FireworksAiRerankResponseEntity;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the FireworksAI model type.
 * Supports both embeddings and rerank models.
 */
public class FireworksAiActionCreator implements FireworksAiActionVisitor {

    private static final ResponseHandler EMBEDDINGS_HANDLER = new FireworksAiResponseHandler(
        "fireworksai embeddings",
        FireworksAiEmbeddingsResponseEntity::fromResponse,
        false
    );

    private static final ResponseHandler RERANK_HANDLER = new FireworksAiResponseHandler(
        "fireworksai rerank",
        (request, response) -> FireworksAiRerankResponseEntity.fromResponse((FireworksAiRerankRequest) request, response),
        false
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public FireworksAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(FireworksAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = FireworksAiEmbeddingsModel.of(model, taskSettings);
        var manager = new TruncatingRequestManager(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            (truncationResult) -> new FireworksAiEmbeddingsRequest(serviceComponents.truncator(), truncationResult, overriddenModel),
            overriddenModel.getServiceSettings().maxInputTokens()
        );

        var errorMessage = constructFailedToSendRequestMessage("FireworksAI embeddings");
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(FireworksAiRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = FireworksAiRerankModel.of(model, taskSettings);

        Function<QueryAndDocsInputs, Request> requestCreator = rerankInput -> new FireworksAiRerankRequest(
            rerankInput.getQuery(),
            rerankInput.getChunks(),
            rerankInput.getTopN(),
            overriddenModel.getTaskSettings().getReturnDocuments(),
            overriddenModel
        );

        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            requestCreator,
            QueryAndDocsInputs.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("FireworksAI rerank");
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }
}
