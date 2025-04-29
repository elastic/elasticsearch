/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiRerankRequestManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class GoogleVertexAiActionCreator implements GoogleVertexAiActionVisitor {

    public static final String COMPLETION_ERROR_PREFIX = "Google VertexAI chat completion";
    private final Sender sender;

    private final ServiceComponents serviceComponents;

    public GoogleVertexAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(GoogleVertexAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, taskSettings);
        var requestManager = new GoogleVertexAiEmbeddingsRequestManager(
            overriddenModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Google Vertex AI embeddings");
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(GoogleVertexAiRerankModel model, Map<String, Object> taskSettings) {
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Google Vertex AI rerank");
        var requestManager = GoogleVertexAiRerankRequestManager.of(model, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(GoogleVertexAiChatCompletionModel model, Map<String, Object> taskSettings) {
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Google Vertex AI chat completion");
        var requestManager = GoogleVertexAiCompletionRequestManager.of(model, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }
}
