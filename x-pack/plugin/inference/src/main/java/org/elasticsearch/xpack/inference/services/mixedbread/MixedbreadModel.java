/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class representing a Mixedbread model for inference.
 * This class extends {@link RateLimitGroupingModel} and provides common functionality for Mixedbread models.
 */
public abstract class MixedbreadModel extends RateLimitGroupingModel {
    private final URI uri;

    /**
     * Constructs a {@link MixedbreadModel} with specified model configurations and secrets.
     *
     * @param configurations the model configurations
     * @param secrets the secret settings for the model
     */
    public MixedbreadModel(ModelConfigurations configurations, ModelSecrets secrets, URI uri) {
        super(configurations, secrets);
        this.uri = uri;
    }

    /**
     * Constructs a {@link MixedbreadModel} by copying an existing model and applying new task settings.
     *
     * @param model the existing model to copy
     * @param taskSettings the task-specific settings to be applied
     */
    protected MixedbreadModel(MixedbreadModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        uri = model.uri();
    }

    /**
     * Constructs a {@link MixedbreadModel} by copying an existing model and applying new service settings.
     *
     * @param model the existing model to copy
     * @param serviceSettings the settings for the inference service
     */
    protected MixedbreadModel(MixedbreadModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        uri = model.uri();
    }

    public URI uri() {
        return uri;
    }

    /**
     * Accepts a visitor to create an executable action for this Mixedbread model.
     *
     * @param creator the visitor that creates the executable action
     * @param taskSettings the task-specific settings to be applied
     * @return an {@link ExecutableAction} for this Mixedbread model
     */
    public abstract ExecutableAction accept(MixedbreadActionVisitor creator, Map<String, Object> taskSettings);

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public int rateLimitGroupingHash() {
        return Objects.hash(getServiceSettings().modelId(), getSecretSettings());
    }
}
