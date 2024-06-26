/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.anthropic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.AnthropicCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class AnthropicChatCompletionAction implements ExecutableAction {

    private final String errorMessage;
    private final AnthropicCompletionRequestManager requestCreator;

    private final Sender sender;

    public AnthropicChatCompletionAction(Sender sender, AnthropicChatCompletionModel model, ServiceComponents serviceComponents) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.requestCreator = AnthropicCompletionRequestManager.of(model, serviceComponents.threadPool());
        this.errorMessage = constructFailedToSendRequestMessage(model.getUri(), "Anthropic chat completions");
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (inferenceInputs instanceof DocumentsOnlyInput == false) {
            listener.onFailure(new ElasticsearchStatusException("Invalid inference input type", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }

        var docsOnlyInput = (DocumentsOnlyInput) inferenceInputs;
        if (docsOnlyInput.getInputs().size() > 1) {
            listener.onFailure(new ElasticsearchStatusException("Anthropic completions only accepts 1 input", RestStatus.BAD_REQUEST));
            return;
        }

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
