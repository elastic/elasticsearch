/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2ClusterSettings;
import org.elasticsearch.xpack.inference.common.oauth2.TokenCache;
import org.elasticsearch.xpack.inference.common.secrets.SecretsApplier;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.openai.secrets.OpenAiSecretsFactory;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class OpenAiModel extends RateLimitGroupingModel {

    private final OpenAiRateLimitServiceSettings rateLimitServiceSettings;
    private final SecretsApplier secretsApplier;
    private final URI uri;

    public OpenAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        OpenAiRateLimitServiceSettings rateLimitServiceSettings,
        @Nullable SecretSettings secretSettings,
        OpenAiServiceSettings serviceSettings,
        ThreadPool threadPool,
        TokenCache tokenCache,
        OAuth2ClusterSettings oauth2ClusterSettings,
        URI uri
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.secretsApplier = OpenAiSecretsFactory.createSecretsApplier(
            configurations.getInferenceEntityId(),
            threadPool,
            tokenCache,
            secretSettings,
            serviceSettings,
            oauth2ClusterSettings
        );
        this.uri = Objects.requireNonNull(uri);
    }

    public OpenAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        OpenAiRateLimitServiceSettings rateLimitServiceSettings,
        @Nullable ApiKeySecrets apiKeySecrets,
        URI uri
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.secretsApplier = OpenAiSecretsFactory.createSecretsApplier(apiKeySecrets);
        this.uri = Objects.requireNonNull(uri);
    }

    protected OpenAiModel(OpenAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        secretsApplier = model.secretsApplier;
        uri = model.uri;
    }

    protected OpenAiModel(OpenAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        secretsApplier = model.secretsApplier;
        uri = model.uri;
    }

    public SecretsApplier secretsApplier() {
        return secretsApplier;
    }

    public OpenAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public abstract ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings);

    public int rateLimitGroupingHash() {
        return Objects.hash(rateLimitServiceSettings.modelId(), getSecrets().getSecretSettings(), uri);
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings.rateLimitSettings();
    }

    public URI uri() {
        return uri;
    }
}
