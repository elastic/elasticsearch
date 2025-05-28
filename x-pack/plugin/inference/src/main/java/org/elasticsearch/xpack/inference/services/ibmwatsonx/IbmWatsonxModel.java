/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class IbmWatsonxModel extends RateLimitGroupingModel {

    private final IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings;

    protected URI uri;

    public IbmWatsonxModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
    }

    public IbmWatsonxModel(IbmWatsonxModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public IbmWatsonxModel(IbmWatsonxModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public abstract ExecutableAction accept(IbmWatsonxActionVisitor creator, Map<String, Object> taskSettings);

    public IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(uri);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitServiceSettings().rateLimitSettings();
    }
}
