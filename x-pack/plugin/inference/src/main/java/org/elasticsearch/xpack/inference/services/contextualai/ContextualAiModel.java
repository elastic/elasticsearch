/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.contextualai.action.ContextualAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class ContextualAiModel extends RateLimitGroupingModel {
    private final URI uri;

    protected ContextualAiModel(ModelConfigurations configurations, ModelSecrets secrets, URI uri) {
        super(configurations, secrets);
        this.uri = Objects.requireNonNull(uri);
    }

    protected ContextualAiModel(ContextualAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        this.uri = model.uri;
    }

    public URI uri() {
        return uri;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    @Override
    public ContextualAiServiceSettings getServiceSettings() {
        return (ContextualAiServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public abstract ExecutableAction accept(ContextualAiActionVisitor creator, Map<String, Object> taskSettings);

    public int rateLimitGroupingHash() {
        return Objects.hashCode(getSecretSettings().apiKey());
    }
}
