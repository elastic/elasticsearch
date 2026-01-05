/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.azureaistudio.action.AzureAiStudioActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for Azure AI Studio models. There are some common properties across the task types
 * including:
 * - target:
 * - uri:
 * - provider:
 * - endpointType:
 */
public abstract class AzureAiStudioModel extends Model {
    protected String target;
    protected URI uri;
    protected AzureAiStudioProvider provider;
    protected AzureAiStudioEndpointType endpointType;
    protected RateLimitSettings rateLimitSettings;

    public AzureAiStudioModel(AzureAiStudioModel model, TaskSettings taskSettings, RateLimitSettings rateLimitSettings) {
        super(model, taskSettings);
        this.rateLimitSettings = Objects.requireNonNull(rateLimitSettings);
        setPropertiesFromServiceSettings((AzureAiStudioServiceSettings) model.getServiceSettings());
    }

    public AzureAiStudioModel(AzureAiStudioModel model, AzureAiStudioServiceSettings serviceSettings) {
        super(model, serviceSettings);
        setPropertiesFromServiceSettings(serviceSettings);
    }

    protected AzureAiStudioModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets);
        setPropertiesFromServiceSettings((AzureAiStudioServiceSettings) modelConfigurations.getServiceSettings());
    }

    private void setPropertiesFromServiceSettings(AzureAiStudioServiceSettings serviceSettings) {
        this.target = serviceSettings.target;
        this.provider = serviceSettings.provider();
        this.endpointType = serviceSettings.endpointType();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
        try {
            this.uri = getEndpointUri();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract URI getEndpointUri() throws URISyntaxException;

    public String target() {
        return this.target;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    public AzureAiStudioProvider provider() {
        return this.provider;
    }

    public AzureAiStudioEndpointType endpointType() {
        return this.endpointType;
    }

    public URI uri() {
        return this.uri;
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

    public abstract ExecutableAction accept(AzureAiStudioActionVisitor creator, Map<String, Object> taskSettings);
}
