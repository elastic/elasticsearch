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

public abstract class LlamaModel extends RateLimitGroupingModel {
    protected String modelId;
    protected URI uri;
    protected RateLimitSettings rateLimitSettings;

    protected LlamaModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

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

    protected static SecretSettings retrieveSecretSettings(Map<String, Object> secrets) {
        return (secrets != null && secrets.isEmpty()) ? EmptySecretSettings.INSTANCE : DefaultSecretSettings.fromMap(secrets);
    }
}
