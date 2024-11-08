/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.googlevertexai.GoogleVertexAiActionVisitor;
import org.elasticsearch.xpack.inference.external.request.googlevertexai.GoogleVertexAiUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankTaskSettings.TOP_N;

public class GoogleVertexAiRerankModel extends GoogleVertexAiModel {

    private URI uri;

    public GoogleVertexAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            GoogleVertexAiRerankServiceSettings.fromMap(serviceSettings, context),
            GoogleVertexAiRerankTaskSettings.fromMap(taskSettings),
            GoogleVertexAiSecretSettings.fromMap(secrets)
        );
    }

    public GoogleVertexAiRerankModel(GoogleVertexAiRerankModel model, GoogleVertexAiRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    // Should only be used directly for testing
    GoogleVertexAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleVertexAiRerankServiceSettings serviceSettings,
        GoogleVertexAiRerankTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = buildUri(serviceSettings.projectId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    protected GoogleVertexAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String uri,
        GoogleVertexAiRerankServiceSettings serviceSettings,
        GoogleVertexAiRerankTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GoogleVertexAiRerankServiceSettings getServiceSettings() {
        return (GoogleVertexAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public GoogleVertexAiRerankTaskSettings getTaskSettings() {
        return (GoogleVertexAiRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public GoogleVertexAiSecretSettings getSecretSettings() {
        return (GoogleVertexAiSecretSettings) super.getSecretSettings();
    }

    @Override
    public GoogleDiscoveryEngineRateLimitServiceSettings rateLimitServiceSettings() {
        return (GoogleDiscoveryEngineRateLimitServiceSettings) super.rateLimitServiceSettings();
    }

    public URI uri() {
        return uri;
    }

    @Override
    public ExecutableAction accept(GoogleVertexAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    public static URI buildUri(String projectId) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GoogleVertexAiUtils.GOOGLE_DISCOVERY_ENGINE_HOST)
            .setPathSegments(
                GoogleVertexAiUtils.V1,
                GoogleVertexAiUtils.PROJECTS,
                projectId,
                GoogleVertexAiUtils.LOCATIONS,
                GoogleVertexAiUtils.GLOBAL,
                GoogleVertexAiUtils.RANKING_CONFIGS,
                format("%s:%s", GoogleVertexAiUtils.DEFAULT_RANKING_CONFIG, GoogleVertexAiUtils.RANK)
            )
            .build();
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    TOP_N,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TOGGLE)
                        .setLabel("Top N")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies the number of the top n documents, which should be returned.")
                        .setType(SettingsConfigurationFieldType.BOOLEAN)
                        .setValue(false)
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
