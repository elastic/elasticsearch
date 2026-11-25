/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

public abstract class GoogleAiStudioModel extends RateLimitGroupingModel {

    private final GoogleAiStudioServiceSettings serviceSettings;

    public GoogleAiStudioModel(ModelConfigurations configurations, ModelSecrets secrets, GoogleAiStudioServiceSettings serviceSettings) {
        super(configurations, secrets);

        this.serviceSettings = Objects.requireNonNull(serviceSettings);
    }

    public GoogleAiStudioModel(GoogleAiStudioModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        this.serviceSettings = model.serviceSettings;
    }

    @Override
    public int rateLimitGroupingHash() {
        return serviceSettings.modelId().hashCode();
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return serviceSettings.rateLimitSettings();
    }
}
