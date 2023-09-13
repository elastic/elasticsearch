/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;

public class OpenAiEmbeddingsAction implements ExecutableAction {
    private final ThreadPool threadPool;
    private final String input;
    private final OpenAiEmbeddingsServiceSettings serviceSettings;
    private final OpenAiEmbeddingsTaskSettings taskSettings;
    private final ActionListener<InferenceResult> listener;

    public OpenAiEmbeddingsAction(
        ThreadPool threadPool,
        String input,
        OpenAiEmbeddingsServiceSettings serviceSettings,
        OpenAiEmbeddingsTaskSettings taskSettings,
        ActionListener<InferenceResult> listener
    ) {
        this.threadPool = threadPool;
        this.input = input;
        this.serviceSettings = serviceSettings;
        this.taskSettings = taskSettings;
        this.listener = listener;
    }

    public void execute() {

    }

}
