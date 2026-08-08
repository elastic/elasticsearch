/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionVisitor;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for all TencentCloud models. Holds the effective request URI.
 * The API key is obtained from the model secrets on demand (see {@link #getSecretSettings()}) rather than stored as a
 * field, keeping the model consistent with how the rest of the inference codebase handles secrets.
 */
public abstract class TencentCloudModel extends RateLimitGroupingModel {

    private final URI uri;

    public TencentCloudModel(ModelConfigurations configurations, ModelSecrets secrets, URI uri) {
        super(configurations, secrets);
        this.uri = Objects.requireNonNull(uri);
    }

    protected TencentCloudModel(TencentCloudModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        this.uri = model.uri();
    }

    protected TencentCloudModel(TencentCloudModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        this.uri = model.uri();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        return uri;
    }

    @Override
    public int rateLimitGroupingHash() {
        // Group by the upstream model and host (uri) so requests that share the same endpoint and model share a
        // rate-limit bucket. This avoids grouping by the secret api key.
        return Objects.hash(getServiceSettings().modelId(), uri());
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return ((TencentCloudCommonServiceSettings) getServiceSettings()).rateLimitSettings();
    }

    public abstract ExecutableAction accept(TencentCloudActionVisitor creator, Map<String, Object> taskSettings);
}
