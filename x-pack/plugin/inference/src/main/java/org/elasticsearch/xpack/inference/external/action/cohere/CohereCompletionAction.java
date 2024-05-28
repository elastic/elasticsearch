/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.cohere;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.CohereCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class CohereCompletionAction implements ExecutableAction {

    private final String failedToSendRequestErrorMessage;

    private final Sender sender;

    private final CohereCompletionRequestManager requestManager;

    public CohereCompletionAction(Sender sender, CohereCompletionModel model, ThreadPool threadPool) {
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(model.getServiceSettings().uri(), "Cohere completion");
        this.requestManager = CohereCompletionRequestManager.of(model, threadPool);
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (inferenceInputs instanceof DocumentsOnlyInput == false) {
            listener.onFailure(new ElasticsearchStatusException("Invalid inference input type", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }

        var docsOnlyInput = (DocumentsOnlyInput) inferenceInputs;
        if (docsOnlyInput.getInputs().size() > 1) {
            listener.onFailure(new ElasticsearchStatusException("Cohere completion only accepts 1 input", RestStatus.BAD_REQUEST));
            return;
        }

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
