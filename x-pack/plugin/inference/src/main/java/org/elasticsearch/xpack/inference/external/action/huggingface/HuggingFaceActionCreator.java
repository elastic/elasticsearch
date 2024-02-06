/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceResponseHandler;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceElserResponseEntity;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;

import java.util.Objects;

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

        return new HuggingFaceAction(sender, model, serviceComponents, responseHandler, "text embeddings");
    }

    @Override
    public ExecutableAction create(HuggingFaceElserModel model) {
        var responseHandler = new HuggingFaceResponseHandler("hugging face elser", HuggingFaceElserResponseEntity::fromResponse);

        return new HuggingFaceAction(sender, model, serviceComponents, responseHandler, "ELSER");
    }
}
