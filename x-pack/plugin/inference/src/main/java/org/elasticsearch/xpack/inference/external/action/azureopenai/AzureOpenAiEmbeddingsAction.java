/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.azureopenai;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.AzureOpenAiEmbeddingsExecutableRequestCreator;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;

import java.net.URI;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class AzureOpenAiEmbeddingsAction implements ExecutableAction {

    private String errorMessage;
    private final AzureOpenAiEmbeddingsExecutableRequestCreator requestCreator;
    private final Sender sender;

    public AzureOpenAiEmbeddingsAction(Sender sender, AzureOpenAiEmbeddingsModel model, ServiceComponents serviceComponents) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        requestCreator = new AzureOpenAiEmbeddingsExecutableRequestCreator(model, serviceComponents.truncator());
        errorMessage = constructFailedToSendRequestMessage(requestCreator.getAzureOpenAiRequestUri(), "Azure OpenAI embeddings");
    }

    public void setRequestUri(URI uri) {
        this.requestCreator.setAzureOpenAiRequestUri(uri);
        errorMessage = constructFailedToSendRequestMessage(uri, "Azure OpenAI embeddings");
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(requestCreator, inferenceInputs, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
