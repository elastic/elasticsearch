/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIResponseHandler;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIRerankRequest;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;
import org.elasticsearch.xpack.inference.services.jinaai.response.JinaAIEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.jinaai.response.JinaAIRerankResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the jinaai model type.
 */
public class JinaAIActionCreator implements JinaAIActionVisitor {

    private static final ResponseHandler TEXT_EMBEDDINGS_HANDLER = new JinaAIResponseHandler(
        "jinaai text embedding",
        JinaAIEmbeddingsResponseEntity::fromResponse
    );

    private static final ResponseHandler EMBEDDINGS_HANDLER = new JinaAIResponseHandler(
        "jinaai embedding",
        JinaAIEmbeddingsResponseEntity::fromResponse
    );

    private static final ResponseHandler RERANK_HANDLER = new JinaAIResponseHandler(
        "jinaai rerank",
        (request, response) -> JinaAIRerankResponseEntity.fromResponse(response)
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public JinaAIActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(JinaAIEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = JinaAIEmbeddingsModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            getEmbeddingsResponseHandler(model),
            (embeddingsInput) -> new JinaAIEmbeddingsRequest(embeddingsInput.getInputs(), embeddingsInput.getInputType(), overriddenModel),
            EmbeddingsInput.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage("JinaAI embeddings"));
    }

    private static ResponseHandler getEmbeddingsResponseHandler(JinaAIEmbeddingsModel model) {
        return switch (model.getTaskType()) {
            case TEXT_EMBEDDING -> TEXT_EMBEDDINGS_HANDLER;
            case EMBEDDING -> EMBEDDINGS_HANDLER;
            // Should not be possible
            default -> throw new IllegalArgumentException(
                Strings.format("Received unexpected task type [%s] for JinaAI embeddings model", model.getTaskType())
            );
        };
    }

    @Override
    public ExecutableAction create(JinaAIRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = JinaAIRerankModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            (rerankInput) -> new JinaAIRerankRequest(
                rerankInput.getQuery(),
                rerankInput.getChunks(),
                rerankInput.getReturnDocuments(),
                rerankInput.getTopN(),
                overriddenModel
            ),
            QueryAndDocsInputs.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage("JinaAI rerank"));
    }
}
