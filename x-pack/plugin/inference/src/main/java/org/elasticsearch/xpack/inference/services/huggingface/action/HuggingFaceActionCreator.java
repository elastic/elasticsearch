/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceRequestManager;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceResponseHandler;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.huggingface.request.rerank.HuggingFaceRerankRequest;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankModel;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceElserResponseEntity;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceRerankResponseEntity;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the hugging face model type.
 */
public class HuggingFaceActionCreator implements HuggingFaceActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    private static final String FAILED_TO_SEND_REQUEST_ERROR_MESSAGE =
        "Failed to send Hugging Face %s request from inference entity id [%s]";
    private static final String INVALID_REQUEST_TYPE_MESSAGE = "Invalid request type: expected HuggingFace %s request but got %s";
    static final ResponseHandler RERANK_HANDLER = new HuggingFaceResponseHandler("hugging face rerank", (request, response) -> {
        var errorMessage = format(INVALID_REQUEST_TYPE_MESSAGE, "RERANK", request != null ? request.getClass().getName() : "null");

        if ((request instanceof HuggingFaceRerankRequest) == false) {
            throw new IllegalArgumentException(errorMessage);
        }
        return HuggingFaceRerankResponseEntity.fromResponse((HuggingFaceRerankRequest) request, response);
    });

    public HuggingFaceActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(HuggingFaceRerankModel model) {
        var overriddenModel = HuggingFaceRerankModel.of(model, model.getTaskSettings());
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            inputs -> new HuggingFaceRerankRequest(
                inputs.getQuery(),
                inputs.getChunks(),
                inputs.getReturnDocuments(),
                inputs.getTopN(),
                model
            ),
            QueryAndDocsInputs.class
        );
        var errorMessage = format(FAILED_TO_SEND_REQUEST_ERROR_MESSAGE, "RERANK", model.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(HuggingFaceEmbeddingsModel model) {
        var responseHandler = new HuggingFaceResponseHandler(
            "hugging face text embeddings",
            HuggingFaceEmbeddingsResponseEntity::fromResponse
        );
        var requestCreator = HuggingFaceRequestManager.of(
            model,
            responseHandler,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = format(
            "Failed to send Hugging Face %s request from inference entity id [%s]",
            "text embeddings",
            model.getInferenceEntityId()
        );
        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }

    @Override
    public ExecutableAction create(HuggingFaceElserModel model) {
        var responseHandler = new HuggingFaceResponseHandler("hugging face elser", HuggingFaceElserResponseEntity::fromResponse);
        var requestCreator = HuggingFaceRequestManager.of(
            model,
            responseHandler,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = format(FAILED_TO_SEND_REQUEST_ERROR_MESSAGE, "ELSER", model.getInferenceEntityId());
        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }
}
