/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.fireworksai.action.FireworksAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;
import java.util.Objects;

/**
 * Base model class for FireworksAI inference services.
 * This class extends RateLimitGroupingModel and provides common functionality
 * for embeddings models, including API key management and rate limiting.
 */
public abstract class FireworksAiModel extends RateLimitGroupingModel {

    private final SecureString apiKey;
    private final FireworksAiRateLimitServiceSettings rateLimitServiceSettings;

    public FireworksAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        @Nullable ApiKeySecrets apiKeySecrets,
        FireworksAiRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        apiKey = ServiceUtils.apiKey(apiKeySecrets);
    }

    protected FireworksAiModel(FireworksAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
    }

    protected FireworksAiModel(FireworksAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public FireworksAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public abstract ExecutableAction accept(FireworksAiActionVisitor creator, Map<String, Object> taskSettings);

    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings.rateLimitSettings();
    }

    // FireworksAI rate limits are per API key only, not per model or URL
    // See: https://docs.fireworks.ai/guides/quotas_usage/rate-limits
    public int rateLimitGroupingHash() {
        return apiKey().hashCode();
    }
}
