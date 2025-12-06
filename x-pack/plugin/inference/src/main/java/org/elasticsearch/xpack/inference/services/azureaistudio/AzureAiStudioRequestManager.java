/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;

import java.util.Objects;

public abstract class AzureAiStudioRequestManager extends BaseRequestManager {

    protected AzureAiStudioRequestManager(ThreadPool threadPool, AzureAiStudioModel model) {
        super(threadPool, model.getInferenceEntityId(), AzureAiStudioRequestManager.RateLimitGrouping.of(model), model.rateLimitSettings());
    }

    record RateLimitGrouping(int targetHashcode) {
        public static AzureAiStudioRequestManager.RateLimitGrouping of(AzureAiStudioModel model) {
            Objects.requireNonNull(model);

            return new AzureAiStudioRequestManager.RateLimitGrouping(model.target().hashCode());
        }
    }
}
