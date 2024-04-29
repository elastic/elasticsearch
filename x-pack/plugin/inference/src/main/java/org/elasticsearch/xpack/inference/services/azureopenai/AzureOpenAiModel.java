/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class AzureOpenAiModel extends Model {

    protected URI uri;
    private final AzureOpenAiRateLimitServiceSettings rateLimitServiceSettings;

    public AzureOpenAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        AzureOpenAiRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        this.uri = model.getUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        this.uri = model.getUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public abstract ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings);

    public URI getUri() {
        return uri;
    }

    // Needed for testing
    public void setUri(URI newUri) {
        this.uri = newUri;
    }

    public AzureOpenAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }
}
