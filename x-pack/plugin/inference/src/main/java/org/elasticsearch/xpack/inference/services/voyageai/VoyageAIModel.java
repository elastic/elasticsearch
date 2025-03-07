/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.voyageai.VoyageAIActionVisitor;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

public abstract class VoyageAIModel extends Model {
    private final SecureString apiKey;
    private final VoyageAIRateLimitServiceSettings rateLimitServiceSettings;
    protected final URI uri;

    public VoyageAIModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        @Nullable ApiKeySecrets apiKeySecrets,
        VoyageAIRateLimitServiceSettings rateLimitServiceSettings
    ) {
        this(configurations, secrets, apiKeySecrets, rateLimitServiceSettings, null);
    }

    public VoyageAIModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        @Nullable ApiKeySecrets apiKeySecrets,
        VoyageAIRateLimitServiceSettings rateLimitServiceSettings,
        String url
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.apiKey = ServiceUtils.apiKey(apiKeySecrets);
        this.uri = url == null ? null : URI.create(url);
    }

    protected VoyageAIModel(VoyageAIModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        this.rateLimitServiceSettings = model.rateLimitServiceSettings();
        this.apiKey = model.apiKey();
        this.uri = model.uri;
    }

    protected VoyageAIModel(VoyageAIModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        this.rateLimitServiceSettings = model.rateLimitServiceSettings();
        this.apiKey = model.apiKey();
        this.uri = model.uri;
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public VoyageAIRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public abstract ExecutableAction accept(VoyageAIActionVisitor creator, Map<String, Object> taskSettings, InputType inputType);

    public URI uri() {
        return uri;
    }

    public URI buildUri() throws URISyntaxException {
        if (uri == null) {
            return buildRequestUri();
        }
        return uri;
    }

    protected abstract URI buildRequestUri() throws URISyntaxException;
}
