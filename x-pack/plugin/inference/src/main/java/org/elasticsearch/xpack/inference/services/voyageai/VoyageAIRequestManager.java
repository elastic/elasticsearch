/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;

import java.util.Map;
import java.util.Objects;

abstract class VoyageAIRequestManager extends BaseRequestManager {
    private static final String DEFAULT_MODEL_FAMILY = "default_model_family";
    private static final Map<String, String> MODEL_TO_MODEL_FAMILY = Map.ofEntries(
        Map.entry("voyage-multimodal-3", "embed_multimodal"),
        Map.entry("voyage-multimodal-3.5", "embed_multimodal"),
        Map.entry("voyage-3-large", "embed_large"),
        Map.entry("voyage-code-3", "embed_large"),
        Map.entry("voyage-3", "embed_medium"),
        Map.entry("voyage-3-lite", "embed_small"),
        Map.entry("voyage-finance-2", "embed_large"),
        Map.entry("voyage-law-2", "embed_large"),
        Map.entry("voyage-code-2", "embed_large"),
        Map.entry("rerank-2", "rerank_large"),
        Map.entry("rerank-2-lite", "rerank_small")
    );

    protected VoyageAIRequestManager(ThreadPool threadPool, VoyageAIModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitSettings());
    }

    record RateLimitGrouping(int apiKeyHash) {
        public static RateLimitGrouping of(VoyageAIModel model) {
            Objects.requireNonNull(model);
            String modelId = model.getServiceSettings().modelId();
            String modelFamily = MODEL_TO_MODEL_FAMILY.getOrDefault(modelId, DEFAULT_MODEL_FAMILY);

            return new RateLimitGrouping(modelFamily.hashCode());
        }
    }
}
