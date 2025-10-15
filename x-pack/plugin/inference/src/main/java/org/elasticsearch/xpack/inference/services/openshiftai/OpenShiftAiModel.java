/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

/**
 * Represents an OpenShift AI modelId that can be used for inference tasks.
 * This class extends RateLimitGroupingModel to handle rate limiting based on modelId and API key.
 */
public abstract class OpenShiftAiModel extends RateLimitGroupingModel {
    protected RateLimitSettings rateLimitSettings;

    protected OpenShiftAiModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    protected OpenShiftAiModel(RateLimitGroupingModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    protected OpenShiftAiModel(RateLimitGroupingModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(getServiceSettings().uri, getServiceSettings().modelId(), getSecretSettings().apiKey());
    }

    @Override
    public OpenShiftAiServiceSettings getServiceSettings() {
        return (OpenShiftAiServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

}
