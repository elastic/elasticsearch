/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.OpenAiCompletionExecutableRequestCreator;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class OpenAiChatCompletionAction implements ExecutableAction {

    private final String errorMessage;
    private final OpenAiCompletionExecutableRequestCreator requestCreator;

    private final Sender sender;

    public OpenAiChatCompletionAction(Sender sender, OpenAiChatCompletionModel model, ServiceComponents serviceComponents) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.requestCreator = new OpenAiCompletionExecutableRequestCreator(model, serviceComponents.truncator());
        this.errorMessage = constructFailedToSendRequestMessage(model.getServiceSettings().uri(), "OpenAI chat completions");
    }

    @Override
    public void execute(List<String> input, ActionListener<InferenceServiceResults> listener) {
        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(requestCreator, input, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
