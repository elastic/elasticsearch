/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadActionVisitor;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class representing a Mixedbread model for inference.
 * This class extends RateLimitGroupingModel and provides common functionality for Mixedbread models.
 */
public abstract class MixedbreadModel extends RateLimitGroupingModel {
    private final URI uri;

    public MixedbreadModel(ModelConfigurations configurations, ModelSecrets secrets, @Nullable ApiKeySecrets apiKeySecrets, URI uri) {
        super(configurations, secrets);
        this.uri = uri;
    }

    protected MixedbreadModel(MixedbreadModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        uri = model.uri();
    }

    public abstract ExecutableAction accept(MixedbreadActionVisitor creator, Map<String, Object> taskSettings);

    public URI uri() {
        return uri;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    @Override
    public MixedbreadRerankServiceSettings getServiceSettings() {
        return (MixedbreadRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public int rateLimitGroupingHash() {
        return Objects.hash(getServiceSettings().modelId(), uri, getSecretSettings());
    }
}
