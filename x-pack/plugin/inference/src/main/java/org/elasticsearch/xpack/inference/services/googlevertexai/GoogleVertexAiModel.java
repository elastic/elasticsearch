/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionVisitor;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiRequestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public abstract class GoogleVertexAiModel extends RateLimitGroupingModel {

    private final GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings;

    protected URI nonStreamingUri;

    /**
     * This field defines the behaviour used to apply authorization headers to a {@link HttpPost}. By default, this is
     * {@link GoogleVertexAiRequestUtils#decorateWithBearerToken(HttpPost, GoogleVertexAiSecretSettings)}. Unit tests may provide different
     * behaviour to allow requests to be created without needing to retrieve credentials.
     */
    private final BiConsumer<HttpPost, GoogleVertexAiModel> authHeaderDecorator;

    public GoogleVertexAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.authHeaderDecorator = (httpPost, model) -> GoogleVertexAiRequestUtils.decorateWithBearerToken(
            httpPost,
            (GoogleVertexAiSecretSettings) model.getSecretSettings()
        );
    }

    // Should only be used for testing. Allows bypassing retrieving the OAuth2 credentials when creating a request
    public GoogleVertexAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings,
        BiConsumer<HttpPost, GoogleVertexAiModel> authHeaderDecorator
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.authHeaderDecorator = authHeaderDecorator;
    }

    public GoogleVertexAiModel(GoogleVertexAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        nonStreamingUri = model.nonStreamingUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
        authHeaderDecorator = model.authHeaderDecorator();
    }

    public GoogleVertexAiModel(GoogleVertexAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        nonStreamingUri = model.nonStreamingUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
        authHeaderDecorator = model.authHeaderDecorator();
    }

    public abstract ExecutableAction accept(GoogleVertexAiActionVisitor creator, Map<String, Object> taskSettings);

    public GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public URI nonStreamingUri() {
        return nonStreamingUri;
    }

    public BiConsumer<HttpPost, GoogleVertexAiModel> authHeaderDecorator() {
        return authHeaderDecorator;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings().rateLimitSettings();
    }
}
