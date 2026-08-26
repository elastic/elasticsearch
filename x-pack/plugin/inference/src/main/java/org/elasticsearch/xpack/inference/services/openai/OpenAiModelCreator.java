/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2ClusterSettings;
import org.elasticsearch.xpack.inference.common.oauth2.TokenCache;
import org.elasticsearch.xpack.inference.services.ModelCreator;

import java.util.Objects;

/**
 * Abstract base for OpenAI model creators, providing shared {@link ThreadPool}, {@link TokenCache},
 * and {@link OAuth2ClusterSettings} dependencies.
 */
public abstract class OpenAiModelCreator<M extends OpenAiModel> implements ModelCreator<M> {

    protected final ThreadPool threadPool;
    protected final TokenCache tokenCache;
    protected final OAuth2ClusterSettings oauth2ClusterSettings;

    protected OpenAiModelCreator(ThreadPool threadPool, TokenCache tokenCache, OAuth2ClusterSettings oauth2ClusterSettings) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.tokenCache = Objects.requireNonNull(tokenCache);
        this.oauth2ClusterSettings = Objects.requireNonNull(oauth2ClusterSettings);
    }
}
