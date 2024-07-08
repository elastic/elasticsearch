/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.googlevertexai.GoogleVertexAiActionVisitor;

import java.util.Map;
import java.util.Objects;

public abstract class GoogleVertexAiModel extends Model {

    private final GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings;

    public GoogleVertexAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
    }

    public GoogleVertexAiModel(GoogleVertexAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public abstract ExecutableAction accept(GoogleVertexAiActionVisitor creator, Map<String, Object> taskSettings);

    public GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

}
