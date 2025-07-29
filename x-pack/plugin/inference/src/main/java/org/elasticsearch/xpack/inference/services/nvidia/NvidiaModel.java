/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.nvidia.action.NvidiaActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

/**
 * Abstract class representing an Nvidia model for inference.
 * This class extends RateLimitGroupingModel and provides common functionality for Nvidia models.
 */
public abstract class NvidiaModel extends RateLimitGroupingModel {

    /**
     * Constructor for creating a NvidiaModel with specified configurations and secrets.
     *
     * @param configurations the model configurations
     * @param secrets the secret settings for the model
     */
    protected NvidiaModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    /**
     * Constructor for creating a NvidiaModel with specified model, service settings, and secret settings.
     * @param model the model configurations
     * @param serviceSettings the settings for the inference service
     */
    protected NvidiaModel(RateLimitGroupingModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action for the Nvidia model.
     * @param creator the visitor that creates the executable action
     * @return an executable action for the Nvidia model
     */
    protected abstract ExecutableAction accept(NvidiaActionVisitor creator);
}
