/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionVisitor;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for all TencentCloud models. Holds the API key, the effective request URI, and the rate limit settings.
 */
public abstract class TencentCloudModel extends RateLimitGroupingModel {

    private final SecureString apiKey;
    private final TencentCloudRateLimitServiceSettings rateLimitServiceSettings;
    private final URI uri;

    public TencentCloudModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        @Nullable ApiKeySecrets apiKeySecrets,
        TencentCloudRateLimitServiceSettings rateLimitServiceSettings,
        URI uri
    ) {
        super(configurations, secrets);
        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.apiKey = ServiceUtils.apiKey(apiKeySecrets);
        this.uri = uri;
    }

    protected TencentCloudModel(TencentCloudModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        this.rateLimitServiceSettings = model.rateLimitServiceSettings();
        this.apiKey = model.apiKey();
        this.uri = model.uri();
    }

    protected TencentCloudModel(TencentCloudModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        this.rateLimitServiceSettings = model.rateLimitServiceSettings();
        this.apiKey = model.apiKey();
        this.uri = model.uri();
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public TencentCloudRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public URI uri() {
        return uri;
    }

    @Override
    public int rateLimitGroupingHash() {
        return apiKey().hashCode();
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings.rateLimitSettings();
    }

    public abstract ExecutableAction accept(TencentCloudActionVisitor creator, Map<String, Object> taskSettings);
}
