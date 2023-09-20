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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.openai.OpenAiClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;

public class OpenAiEmbeddingsAction implements ExecutableAction {
    private final String input;
    private final HttpClient httpClient;
    private final OpenAiEmbeddingsServiceSettings serviceSettings;
    private final OpenAiEmbeddingsTaskSettings taskSettings;

    public OpenAiEmbeddingsAction(
        String input,
        HttpClient httpClient,
        OpenAiEmbeddingsServiceSettings serviceSettings,
        OpenAiEmbeddingsTaskSettings taskSettings
    ) {
        this.input = input;
        this.httpClient = httpClient;
        this.serviceSettings = serviceSettings;
        this.taskSettings = taskSettings;
    }

    public void execute(ActionListener<InferenceResult> listener) {
        try {
            OpenAiAccount account = new OpenAiAccount(serviceSettings.apiKey());
            OpenAiEmbeddingsRequestEntity entity = new OpenAiEmbeddingsRequestEntity(input, taskSettings.model(), taskSettings.user());
            OpenAiEmbeddingsRequest request = new OpenAiEmbeddingsRequest(account, entity);
            OpenAiClient client = new OpenAiClient(httpClient);

            client.send(request, listener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchStatusException("Failed to send open ai request", RestStatus.INTERNAL_SERVER_ERROR, e));
        }
    }
}
