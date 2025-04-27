/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.cohere.action.CohereActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class CohereModel extends Model {
    private final SecureString apiKey;
    private final CohereRateLimitServiceSettings rateLimitServiceSettings;

    public CohereModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        @Nullable ApiKeySecrets apiKeySecrets,
        CohereRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        apiKey = ServiceUtils.apiKey(apiKeySecrets);
    }

    protected CohereModel(CohereModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
    }

    protected CohereModel(CohereModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        apiKey = model.apiKey();
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public CohereRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public abstract ExecutableAction accept(CohereActionVisitor creator, Map<String, Object> taskSettings);

    public abstract URI uri();
}
