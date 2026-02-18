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
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiResponseHandler;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.fireworksai.request.FireworksAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the FireworksAI model type.
 * Supports embeddings models.
 */
public class FireworksAiActionCreator implements FireworksAiActionVisitor {

    // FireworksAI uses OpenAI-compatible embeddings format, so we reuse the OpenAI response parser
    private static final ResponseHandler EMBEDDINGS_HANDLER = new FireworksAiResponseHandler(
        "fireworksai embeddings",
        OpenAiEmbeddingsResponseEntity::fromResponse,
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
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            (embeddingInput) -> new FireworksAiEmbeddingsRequest(embeddingInput.getTextInputs(), overriddenModel),
            EmbeddingsInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage("FireworksAI embeddings");
        return new SenderExecutableAction(sender, manager, errorMessage);
    }
}
