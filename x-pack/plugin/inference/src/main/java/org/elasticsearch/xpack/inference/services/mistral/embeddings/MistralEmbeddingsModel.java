/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.mistral.MistralActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.API_EMBEDDINGS_PATH;

public class MistralEmbeddingsModel extends Model {
    protected String model;
    protected URI uri;
    protected RateLimitSettings rateLimitSettings;

    public MistralEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            MistralEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,    // no task settings for Mistral embeddings
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public MistralEmbeddingsModel(MistralEmbeddingsModel model, MistralEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
        setPropertiesFromServiceSettings(serviceSettings);
    }

    public MistralEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        MistralEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, new EmptyTaskSettings(), chunkingSettings),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    private void setPropertiesFromServiceSettings(MistralEmbeddingsServiceSettings serviceSettings) {
        this.model = serviceSettings.modelId();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
        setEndpointUrl();
    }

    @Override
    public MistralEmbeddingsServiceSettings getServiceSettings() {
        return (MistralEmbeddingsServiceSettings) super.getServiceSettings();
    }

    public String model() {
        return this.model;
    }

    public URI uri() {
        return this.uri;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    private void setEndpointUrl() {
        try {
            this.uri = new URI(API_EMBEDDINGS_PATH);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
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

    public ExecutableAction accept(MistralActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
