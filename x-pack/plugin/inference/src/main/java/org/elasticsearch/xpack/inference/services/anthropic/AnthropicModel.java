/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.anthropic.action.AnthropicActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

public abstract class AnthropicModel extends Model {

    private final AnthropicRateLimitServiceSettings rateLimitServiceSettings;
    private final SecureString apiKey;
    private final URI uri;

    public AnthropicModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        AnthropicRateLimitServiceSettings rateLimitServiceSettings,
        CheckedSupplier<URI, URISyntaxException> uriSupplier,
        @Nullable ApiKeySecrets apiKeySecrets
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        apiKey = ServiceUtils.apiKey(apiKeySecrets);

        try {
            uri = uriSupplier.get();
        } catch (URISyntaxException e) {
            throw new ElasticsearchStatusException(
                Strings.format("Failed to construct %s URL", configurations.getService()),
                RestStatus.BAD_REQUEST,
                e
            );
        }
    }

    protected AnthropicModel(AnthropicModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
        uri = model.getUri();
    }

    protected AnthropicModel(AnthropicModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
        uri = model.getUri();
    }

    public URI getUri() {
        return uri;
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public AnthropicRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public abstract ExecutableAction accept(AnthropicActionVisitor creator, Map<String, Object> taskSettings);
}
