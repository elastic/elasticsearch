/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadActionVisitor;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;

/**
 * Abstract class representing a Mixedbread model for inference.
 * This class extends RateLimitGroupingModel and provides common functionality for Mixedbread models.
 */
public abstract class MixedbreadModel extends RateLimitGroupingModel {
    private final SecureString apiKey;
    private final URI uri;

    public MixedbreadModel(ModelConfigurations configurations, ModelSecrets secrets, @Nullable ApiKeySecrets apiKeySecrets, URI uri) {
        super(configurations, secrets);

        apiKey = ServiceUtils.apiKey(apiKeySecrets);
        this.uri = uri;
    }

    protected MixedbreadModel(MixedbreadModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        apiKey = model.apiKey();
        uri = model.uri();
    }

    public SecureString apiKey() {
        return apiKey;
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

    public int rateLimitGroupingHash() {
        return apiKey().hashCode();
    }
}
