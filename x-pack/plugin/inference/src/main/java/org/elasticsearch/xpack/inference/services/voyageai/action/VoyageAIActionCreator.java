/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIResponseHandler;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIRerankRequest;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;
import org.elasticsearch.xpack.inference.services.voyageai.response.VoyageAIEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.voyageai.response.VoyageAIRerankResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the voyageai model type.
 */
public class VoyageAIActionCreator implements VoyageAIActionVisitor {
    public static final ResponseHandler EMBEDDINGS_HANDLER = new VoyageAIResponseHandler(
        "voyageai text embedding",
        VoyageAIEmbeddingsResponseEntity::fromResponse
    );
    static final ResponseHandler RERANK_HANDLER = new VoyageAIResponseHandler(
        "voyageai rerank",
        (request, response) -> VoyageAIRerankResponseEntity.fromResponse(response)
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public VoyageAIActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(VoyageAIEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = VoyageAIEmbeddingsModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            (embeddingsInput) -> new VoyageAIEmbeddingsRequest(
                embeddingsInput.getStringInputs(),
                embeddingsInput.getInputType(),
                overriddenModel
            ),
            EmbeddingsInput.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("VoyageAI embeddings");
        return new SenderExecutableAction(sender, manager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(VoyageAIRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = VoyageAIRerankModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            (rerankInput) -> new VoyageAIRerankRequest(
                rerankInput.getQuery(),
                rerankInput.getChunks(),
                rerankInput.getReturnDocuments(),
                rerankInput.getTopN(),
                model
            ),
            QueryAndDocsInputs.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("VoyageAI rerank");
        return new SenderExecutableAction(sender, manager, failedToSendRequestErrorMessage);
    }
}
