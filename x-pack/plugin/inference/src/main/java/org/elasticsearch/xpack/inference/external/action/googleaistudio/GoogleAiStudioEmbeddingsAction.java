/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.googleaistudio;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.GoogleAiStudioEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class GoogleAiStudioEmbeddingsAction implements ExecutableAction {

    private final String failedToSendRequestErrorMessage;

    private final GoogleAiStudioEmbeddingsRequestManager requestManager;

    private final Sender sender;

    public GoogleAiStudioEmbeddingsAction(Sender sender, GoogleAiStudioEmbeddingsModel model, ServiceComponents serviceComponents) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.requestManager = new GoogleAiStudioEmbeddingsRequestManager(
            model,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        this.failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(model.uri(), "Google AI Studio embeddings");
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(
                failedToSendRequestErrorMessage,
                listener
            );

            sender.send(requestManager, inferenceInputs, timeout, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, failedToSendRequestErrorMessage));
        }
    }
}
