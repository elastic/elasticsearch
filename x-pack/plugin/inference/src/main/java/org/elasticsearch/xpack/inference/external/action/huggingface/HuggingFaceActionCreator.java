/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.HuggingFaceRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceResponseHandler;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceElserResponseEntity;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the hugging face model type.
 */
public class HuggingFaceActionCreator implements HuggingFaceActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public HuggingFaceActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
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
        var errorMessage = format(
            "Failed to send Hugging Face %s request from inference entity id [%s]",
            "ELSER",
            model.getInferenceEntityId()
        );
        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }
}
