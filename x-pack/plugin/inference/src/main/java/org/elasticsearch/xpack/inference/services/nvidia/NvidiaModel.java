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
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.nvidia.action.NvidiaActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;
import java.util.Objects;

/**
 * Abstract base class representing an Nvidia model for inference.
 * This class extends {@link RateLimitGroupingModel} and provides common functionality for Nvidia models.
 * Subclasses must implement the {@link #accept(NvidiaActionVisitor, Map)}
 * method to handle creation of {@link ExecutableAction} for inference tasks.
 */
public abstract class NvidiaModel extends RateLimitGroupingModel {
    /**
     * Constructs an {@link NvidiaModel} with specified model configurations and secrets.
     *
     * @param configurations the model configurations
     * @param secrets the secret settings for the model
     */
    protected NvidiaModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    /**
     * Constructs an {@link NvidiaModel} by copying an existing model and applying new service settings.
     *
     * @param model the existing model to copy
     * @param serviceSettings the settings for the inference service
     */
    protected NvidiaModel(RateLimitGroupingModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    /**
     * Constructs an {@link NvidiaModel} by copying an existing model and applying new task settings.
     *
     * @param model the existing model to copy
     * @param taskSettings the task settings for the inference task
     */
    protected NvidiaModel(RateLimitGroupingModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    @Override
    public int rateLimitGroupingHash() {
        var serviceSettings = getServiceSettings();
        return Objects.hash(serviceSettings.uri(), serviceSettings.modelId());
    }

    @Override
    public NvidiaServiceSettings getServiceSettings() {
        return (NvidiaServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Nvidia model.
     *
     * @param creator the visitor that creates the executable action
     * @param taskSettings the task settings for the inference task
     * @return an {@link ExecutableAction} for this Nvidia model
     */
    public abstract ExecutableAction accept(NvidiaActionVisitor creator, Map<String, Object> taskSettings);

}
