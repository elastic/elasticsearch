/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.ModelCreator;

import java.util.Objects;

/**
 * Abstract base for Azure OpenAI model creators.
 *
 * @param <M> the type of {@link AzureOpenAiModel} created by the concrete creator
 */
public abstract class AzureOpenAiModelCreator<M extends AzureOpenAiModel> implements ModelCreator<M> {

    protected final ThreadPool threadPool;

    protected AzureOpenAiModelCreator(ThreadPool threadPool) {
        this.threadPool = Objects.requireNonNull(threadPool);
    }
}
