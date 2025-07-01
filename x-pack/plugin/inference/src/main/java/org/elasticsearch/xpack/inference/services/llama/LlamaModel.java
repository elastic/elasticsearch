/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class representing a Llama model for inference.
 * This class extends RateLimitGroupingModel and provides common functionality for Llama models.
 */
public abstract class LlamaModel extends RateLimitGroupingModel {
    protected String modelId;
    protected URI uri;
    protected RateLimitSettings rateLimitSettings;

    /**
     * Constructor for creating a LlamaModel with specified configurations and secrets.
     *
     * @param configurations the model configurations
     * @param secrets the secret settings for the model
     */
    protected LlamaModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    /**
     * Constructor for creating a LlamaModel with specified model, service settings, and secret settings.
     * @param model the model configurations
     * @param serviceSettings the settings for the inference service
     */
    protected LlamaModel(RateLimitGroupingModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public String model() {
        return this.modelId;
    }

    public URI uri() {
        return this.uri;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(modelId, uri, getSecretSettings());
    }

    // Needed for testing only
    public void setURI(String newUri) {
        try {
            this.uri = new URI(newUri);
        } catch (URISyntaxException e) {
            // swallow any error
        }
    }

    /**
     * Retrieves the secret settings from the provided map of secrets.
     * If the map is null or empty, it returns an instance of EmptySecretSettings.
     * Caused by the fact that Llama model doesn't have out of the box security settings and can be used witout authentication.
     *
     * @param secrets the map containing secret settings
     * @return an instance of SecretSettings
     */
    protected static SecretSettings retrieveSecretSettings(Map<String, Object> secrets) {
        return (secrets != null && secrets.isEmpty()) ? EmptySecretSettings.INSTANCE : DefaultSecretSettings.fromMap(secrets);
    }
}
