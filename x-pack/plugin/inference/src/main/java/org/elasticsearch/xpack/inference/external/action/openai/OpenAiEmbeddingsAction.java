/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.OpenAiEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class OpenAiEmbeddingsAction implements ExecutableAction {

    private final String errorMessage;
    private final OpenAiEmbeddingsRequestManager requestCreator;
    private final Sender sender;

    public OpenAiEmbeddingsAction(Sender sender, OpenAiEmbeddingsModel model, ServiceComponents serviceComponents) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        requestCreator = OpenAiEmbeddingsRequestManager.of(model, serviceComponents.truncator(), serviceComponents.threadPool());
        errorMessage = constructFailedToSendRequestMessage(model.getServiceSettings().uri(), "OpenAI embeddings");
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(requestCreator, inferenceInputs, timeout, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
