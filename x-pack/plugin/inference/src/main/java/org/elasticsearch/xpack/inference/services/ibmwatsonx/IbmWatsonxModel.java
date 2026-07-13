/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionVisitor;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxRequestUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public abstract class IbmWatsonxModel extends RateLimitGroupingModel {

    private final IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings;
    /**
     * This field defines the behaviour used to apply authorization headers to a {@link HttpPost}. By default, this is
     * {@link IbmWatsonxRequestUtils#decorateWithBearerToken(HttpPost, DefaultSecretSettings, String)}. Unit tests may provide different
     * behaviour to allow requests to be created without needing to retrieve credentials.
     */
    private final BiConsumer<HttpPost, IbmWatsonxModel> authHeaderDecorator;

    public IbmWatsonxModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings
    ) {
        this(
            configurations,
            secrets,
            rateLimitServiceSettings,
            (httpPost, model) -> IbmWatsonxRequestUtils.decorateWithBearerToken(
                httpPost,
                (DefaultSecretSettings) model.getSecretSettings(),
                model.getInferenceEntityId()
            )
        );
    }

    public IbmWatsonxModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings,
        BiConsumer<HttpPost, IbmWatsonxModel> authHeaderDecorator
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.authHeaderDecorator = authHeaderDecorator;
    }

    public IbmWatsonxModel(IbmWatsonxModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        authHeaderDecorator = model.authHeaderDecorator();
    }

    public IbmWatsonxModel(IbmWatsonxModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
        authHeaderDecorator = model.authHeaderDecorator();
    }

    public abstract ExecutableAction accept(IbmWatsonxActionVisitor creator, Map<String, Object> taskSettings);

    public IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public BiConsumer<HttpPost, IbmWatsonxModel> authHeaderDecorator() {
        return authHeaderDecorator;
    }

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(this.rateLimitServiceSettings);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitServiceSettings().rateLimitSettings();
    }
}
