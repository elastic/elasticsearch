/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * Model implementation for DeepSeek's chat completion service.
 * This class is responsible for holding the configuration and secrets necessary to make requests to DeepSeek's chat completion API,
 * as well as defining the rate limiting settings for those requests.
 */
public class DeepSeekChatCompletionModel extends Model {
    // Per-node rate limit group and settings, limiting the outbound requests this node can make to INTEGER.MAX_VALUE per minute.
    private static final Object RATE_LIMIT_GROUP = new Object();
    private static final RateLimitSettings RATE_LIMIT_SETTINGS = new RateLimitSettings(Integer.MAX_VALUE);

    private static final URI DEFAULT_URI = URI.create("https://api.deepseek.com/chat/completions");

    public DeepSeekChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            DeepSeekServiceSettings.fromMap(serviceSettings),
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    // Should only be used directly for testing
    DeepSeekChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        DeepSeekServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secretSettings)
        );
    }

    public DeepSeekChatCompletionModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    public Optional<SecureString> apiKey() {
        return Optional.ofNullable(((DefaultSecretSettings) getSecretSettings())).map(DefaultSecretSettings::apiKey);
    }

    public String model() {
        return getServiceSettings().modelId();
    }

    public URI uri() {
        var serviceSettings = (DeepSeekServiceSettings) getServiceSettings();
        return serviceSettings.uri() != null ? serviceSettings.uri() : DEFAULT_URI;
    }

    public Object rateLimitGroup() {
        return RATE_LIMIT_GROUP;
    }

    public RateLimitSettings rateLimitSettings() {
        return RATE_LIMIT_SETTINGS;
    }
}
