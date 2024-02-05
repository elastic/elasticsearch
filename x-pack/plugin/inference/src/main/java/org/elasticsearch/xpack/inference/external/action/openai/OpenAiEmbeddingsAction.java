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
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.openai.OpenAiClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

public class OpenAiEmbeddingsAction implements ExecutableAction {

    private final OpenAiAccount account;
    private final OpenAiClient client;
    private final OpenAiEmbeddingsModel model;
    private final String errorMessage;
    private final Truncator truncator;

    public OpenAiEmbeddingsAction(Sender sender, OpenAiEmbeddingsModel model, ServiceComponents serviceComponents) {
        this.model = Objects.requireNonNull(model);
        this.account = new OpenAiAccount(
            this.model.getServiceSettings().uri(),
            this.model.getServiceSettings().organizationId(),
            this.model.getSecretSettings().apiKey()
        );
        this.client = new OpenAiClient(Objects.requireNonNull(sender), Objects.requireNonNull(serviceComponents));
        this.errorMessage = constructFailedToSendRequestMessage(this.model.getServiceSettings().uri(), "OpenAI embeddings");
        this.truncator = Objects.requireNonNull(serviceComponents.truncator());
    }

    @Override
    public void execute(List<String> input, ActionListener<InferenceServiceResults> listener) {
        try {
            var truncatedInput = truncate(input, model.getServiceSettings().maxInputTokens());

            OpenAiEmbeddingsRequest request = new OpenAiEmbeddingsRequest(truncator, account, truncatedInput, model);
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            client.send(request, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
