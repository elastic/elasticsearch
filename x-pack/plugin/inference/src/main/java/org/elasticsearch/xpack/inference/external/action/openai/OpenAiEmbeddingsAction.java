/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.openai.OpenAiClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

public class OpenAiEmbeddingsAction implements ExecutableAction {

    private final OpenAiAccount account;
    private final OpenAiClient client;
    private final OpenAiEmbeddingsModel model;

    public OpenAiEmbeddingsAction(Sender sender, OpenAiEmbeddingsModel model, ThrottlerManager throttlerManager) {
        this.account = new OpenAiAccount(model.getServiceSettings().uri(), model.getSecretSettings().apiKey());
        this.client = new OpenAiClient(sender, throttlerManager);
        this.model = model;
    }

    @Override
    public void execute(String input, ActionListener<InferenceResults> listener) {
        try {
            OpenAiEmbeddingsRequest request = new OpenAiEmbeddingsRequest(
                account,
                new OpenAiEmbeddingsRequestEntity(input, model.getTaskSettings().model(), model.getTaskSettings().user())
            );

            client.send(request, listener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchStatusException("Failed to send OpenAI embeddings request", RestStatus.INTERNAL_SERVER_ERROR, e)
            );
        }
    }
}
