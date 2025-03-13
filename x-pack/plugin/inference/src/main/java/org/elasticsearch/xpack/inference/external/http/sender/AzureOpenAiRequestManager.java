/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;

import java.util.Objects;

public abstract class AzureOpenAiRequestManager extends BaseRequestManager {
    protected AzureOpenAiRequestManager(ThreadPool threadPool, AzureOpenAiModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
    }

    record RateLimitGrouping(int resourceNameHash, int deploymentIdHash) {
        public static RateLimitGrouping of(AzureOpenAiModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(
                model.rateLimitServiceSettings().resourceName().hashCode(),
                model.rateLimitServiceSettings().deploymentId().hashCode()
            );
        }
    }
}
