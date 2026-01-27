/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class OpenAiModel extends RateLimitGroupingModel {

    private final OpenAiRateLimitServiceSettings rateLimitServiceSettings;
    private final SecureString apiKey;
    private final URI uri;

    public OpenAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        OpenAiRateLimitServiceSettings rateLimitServiceSettings,
        @Nullable ApiKeySecrets apiKeySecrets,
        URI uri
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        apiKey = ServiceUtils.apiKey(apiKeySecrets);
        this.uri = Objects.requireNonNull(uri);
    }

    protected OpenAiModel(OpenAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
        uri = model.uri;
    }

    protected OpenAiModel(OpenAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
        uri = model.uri;
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public OpenAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public abstract ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings);

    public int rateLimitGroupingHash() {
        return Objects.hash(rateLimitServiceSettings.modelId(), apiKey, uri);
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings.rateLimitSettings();
    }

    public URI uri() {
        return uri;
    }
}
