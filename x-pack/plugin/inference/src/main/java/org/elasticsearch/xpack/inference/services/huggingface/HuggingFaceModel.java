/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.huggingface.action.HuggingFaceActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

public abstract class HuggingFaceModel extends RateLimitGroupingModel {
    private final HuggingFaceRateLimitServiceSettings rateLimitServiceSettings;
    private final SecureString apiKey;

    public HuggingFaceModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        HuggingFaceRateLimitServiceSettings rateLimitServiceSettings,
        @Nullable ApiKeySecrets apiKeySecrets
    ) {
        super(configurations, secrets);
        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        apiKey = ServiceUtils.apiKey(apiKeySecrets);
    }

    public HuggingFaceRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(rateLimitServiceSettings.uri(), apiKey);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings.rateLimitSettings();
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public abstract Integer getTokenLimit();

    public abstract ExecutableAction accept(HuggingFaceActionVisitor creator);

}
