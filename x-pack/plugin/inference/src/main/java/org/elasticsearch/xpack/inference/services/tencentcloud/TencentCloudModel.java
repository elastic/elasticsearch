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

    /**
     * Constructor for creating a TencentCloudModel with resolved configurations, secrets, and request URI.
     *
     * @param configurations the model configurations
     * @param secrets the secret settings for the model
     * @param uri the effective request URI for the TencentCloud AI Gateway endpoint
     */
    public TencentCloudModel(ModelConfigurations configurations, ModelSecrets secrets, URI uri) {
        super(configurations, secrets);
        this.uri = Objects.requireNonNull(uri);
    }

    /**
     * Constructor for creating a TencentCloudModel by copying an existing model with new task settings.
     *
     * @param model the base TencentCloudModel to copy properties from
     * @param taskSettings the new task settings to apply
     */
    protected TencentCloudModel(TencentCloudModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        this.uri = model.uri();
    }

    /**
     * Constructor for creating a TencentCloudModel by copying an existing model with new service settings.
     *
     * @param model the base TencentCloudModel to copy properties from
     * @param serviceSettings the new service settings to apply
     */
    protected TencentCloudModel(TencentCloudModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        this.uri = model.uri();
    }

    /**
     * Returns the secret settings for this model, cast to {@link DefaultSecretSettings}.
     *
     * @return the DefaultSecretSettings associated with this model
     */
    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Returns the effective request URI for the TencentCloud AI Gateway endpoint.
     *
     * @return the request URI
     */
    public URI uri() {
        return uri;
    }

    /**
     * Computes a hash used to group requests for rate limiting.
     * Groups by the upstream model id and request URI, so requests sharing the same endpoint and model share a bucket.
     *
     * @return a hash grouping this model with other models sharing the same model id and URI
     */
    @Override
    public int rateLimitGroupingHash() {
        // Group by the upstream model and host (uri) so requests that share the same endpoint and model share a
        // rate-limit bucket. This avoids grouping by the secret api key.
        return Objects.hash(getServiceSettings().modelId(), uri());
    }

    /**
     * Returns the rate limit settings for this model from its service settings.
     *
     * @return the RateLimitSettings for this model
     */
    @Override
    public RateLimitSettings rateLimitSettings() {
        return ((TencentCloudCommonServiceSettings) getServiceSettings()).rateLimitSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this TencentCloud model.
     *
     * @param creator the visitor that creates the executable action
     * @param taskSettings the task-level settings for this request
     * @return an ExecutableAction representing this model
     */
    public abstract ExecutableAction accept(TencentCloudActionVisitor creator, Map<String, Object> taskSettings);
}
