/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.openai.OpenAiClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsEntity;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;

import java.io.IOException;

public class OpenAiEmbeddingsAction implements ExecutableAction {
    private final ThreadPool threadPool;
    private final String input;
    private final HttpClient httpClient;
    private final OpenAiEmbeddingsServiceSettings serviceSettings;
    private final OpenAiEmbeddingsTaskSettings taskSettings;
    private final ActionListener<InferenceResult> listener;

    public OpenAiEmbeddingsAction(
        ThreadPool threadPool,
        String input,
        HttpClient httpClient,
        OpenAiEmbeddingsServiceSettings serviceSettings,
        OpenAiEmbeddingsTaskSettings taskSettings,
        ActionListener<InferenceResult> listener
    ) {
        this.threadPool = threadPool;
        this.input = input;
        this.httpClient = httpClient;
        this.serviceSettings = serviceSettings;
        this.taskSettings = taskSettings;
        this.listener = listener;
    }

    public void execute() {
        try {
            // TODO when should we execute on another thread?
            OpenAiAccount account = new OpenAiAccount(serviceSettings.getApiToken());
            OpenAiEmbeddingsEntity entity = new OpenAiEmbeddingsEntity(taskSettings.getModel(), taskSettings.getUser());
            OpenAiEmbeddingsRequest request = new OpenAiEmbeddingsRequest(account, entity);
            OpenAiClient client = new OpenAiClient(httpClient);

            client.send(request);

        } catch (IOException e) {
            throw new ElasticsearchStatusException("Failed to send open ai request", RestStatus.INTERNAL_SERVER_ERROR, e);
        }

    }

}
