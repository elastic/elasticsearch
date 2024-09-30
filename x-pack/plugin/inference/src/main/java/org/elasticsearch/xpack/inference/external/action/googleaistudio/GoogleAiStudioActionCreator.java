/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.googleaistudio;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.GoogleAiStudioCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.GoogleAiStudioEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class GoogleAiStudioActionCreator implements GoogleAiStudioActionVisitor {

    private static final String COMPLETION_ERROR_MESSAGE = "Google AI Studio completion";
    private final Sender sender;

    private final ServiceComponents serviceComponents;

    public GoogleAiStudioActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(GoogleAiStudioCompletionModel model, Map<String, Object> taskSettings) {
        // no overridden model as task settings are always empty for Google AI Studio completion model
        var requestManager = new GoogleAiStudioCompletionRequestManager(model, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(model.uri(), COMPLETION_ERROR_MESSAGE);
        return new SingleInputSenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage, COMPLETION_ERROR_MESSAGE);
    }

    @Override
    public ExecutableAction create(GoogleAiStudioEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestManager = new GoogleAiStudioEmbeddingsRequestManager(
            model,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(model.uri(), "Google AI Studio embeddings");
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }
}
