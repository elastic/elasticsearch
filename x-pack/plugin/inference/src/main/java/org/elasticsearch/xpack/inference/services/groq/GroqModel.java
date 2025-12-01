/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.groq.action.GroqActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

/**
 * Abstract base class representing a Groq model for inference.
 * This class extends {@link RateLimitGroupingModel} and provides common functionality for Groq models.
 * Subclasses must implement the {@link #accept(GroqActionVisitor)}
 * method to handle creation of {@link ExecutableAction} for inference tasks.
 */
public abstract class GroqModel extends RateLimitGroupingModel {
    /**
     * Constructs a {@link GroqModel} with specified model configurations and secrets.
     *
     * @param configurations the model configurations
     * @param secrets the secret settings for the model
     */
    protected GroqModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    @Override
    public int rateLimitGroupingHash() {
        var serviceSettings = getServiceSettings();
        return Objects.hash(serviceSettings.modelId());
    }

    @Override
    public GroqServiceSettings getServiceSettings() {
        return (GroqServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Groq model.
     *
     * @param creator the visitor that creates the executable action
     * @return an {@link ExecutableAction} for this Groq model
     */
    public abstract ExecutableAction accept(GroqActionVisitor creator);

}
