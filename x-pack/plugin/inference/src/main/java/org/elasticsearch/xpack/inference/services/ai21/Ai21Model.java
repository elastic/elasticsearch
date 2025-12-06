/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Represents a AI21 model that can be used for inference tasks.
 * This class extends RateLimitGroupingModel to handle rate limiting based on model and API key.
 */
public abstract class Ai21Model extends RateLimitGroupingModel {
    protected URI uri;
    protected RateLimitSettings rateLimitSettings;

    protected Ai21Model(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    protected Ai21Model(RateLimitGroupingModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
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
        return Objects.hash(getServiceSettings().modelId(), getSecretSettings().apiKey());
    }

    // Needed for testing only
    public void setURI(String newUri) {
        try {
            this.uri = new URI(newUri);
        } catch (URISyntaxException e) {
            // swallow any error
        }
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }
}
