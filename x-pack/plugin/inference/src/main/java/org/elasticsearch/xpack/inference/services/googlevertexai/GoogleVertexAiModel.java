/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class GoogleVertexAiModel extends RateLimitGroupingModel {

    private final GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings;

    protected URI nonStreamingUri;

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

        nonStreamingUri = model.nonStreamingUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public GoogleVertexAiModel(GoogleVertexAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        nonStreamingUri = model.nonStreamingUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public abstract ExecutableAction accept(GoogleVertexAiActionVisitor creator, Map<String, Object> taskSettings);

    public GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public URI nonStreamingUri() {
        return nonStreamingUri;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings().rateLimitSettings();
    }
}
