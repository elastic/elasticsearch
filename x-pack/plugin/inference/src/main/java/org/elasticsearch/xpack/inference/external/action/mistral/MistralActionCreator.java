/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.mistral;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.MistralEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class MistralActionCreator implements MistralActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public MistralActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(MistralEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        var requestManager = new MistralEmbeddingsRequestManager(
            embeddingsModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = constructFailedToSendRequestMessage(embeddingsModel.uri(), "Mistral embeddings");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }
}
