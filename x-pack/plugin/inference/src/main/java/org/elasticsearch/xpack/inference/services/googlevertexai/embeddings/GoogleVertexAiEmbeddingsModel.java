/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.inference.configuration.SettingsConfigurationSelectOption;
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
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE;
import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE;

public class GoogleVertexAiEmbeddingsModel extends GoogleVertexAiModel {

    public static GoogleVertexAiEmbeddingsModel of(
        GoogleVertexAiEmbeddingsModel model,
        Map<String, Object> taskSettings,
        InputType inputType
    ) {
        var requestTaskSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        return new GoogleVertexAiEmbeddingsModel(
            model,
            GoogleVertexAiEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings, inputType)
        );
    }

    public GoogleVertexAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            GoogleVertexAiEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            GoogleVertexAiSecretSettings.fromMap(secrets)
        );
    }

    public GoogleVertexAiEmbeddingsModel(GoogleVertexAiEmbeddingsModel model, GoogleVertexAiEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public GoogleVertexAiEmbeddingsModel(GoogleVertexAiEmbeddingsModel model, GoogleVertexAiEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    // Should only be used directly for testing
    GoogleVertexAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleVertexAiEmbeddingsServiceSettings serviceSettings,
        GoogleVertexAiEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = buildUri(serviceSettings.location(), serviceSettings.projectId(), serviceSettings.modelId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    protected GoogleVertexAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String uri,
        GoogleVertexAiEmbeddingsServiceSettings serviceSettings,
        GoogleVertexAiEmbeddingsTaskSettings taskSettings,
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
    public GoogleVertexAiEmbeddingsServiceSettings getServiceSettings() {
        return (GoogleVertexAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public GoogleVertexAiEmbeddingsTaskSettings getTaskSettings() {
        return (GoogleVertexAiEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public GoogleVertexAiSecretSettings getSecretSettings() {
        return (GoogleVertexAiSecretSettings) super.getSecretSettings();
    }

    @Override
    public GoogleVertexAiEmbeddingsRateLimitServiceSettings rateLimitServiceSettings() {
        return (GoogleVertexAiEmbeddingsRateLimitServiceSettings) super.rateLimitServiceSettings();
    }

    @Override
    public ExecutableAction accept(GoogleVertexAiActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
        return visitor.create(this, taskSettings, inputType);
    }

    public static URI buildUri(String location, String projectId, String modelId) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(format("%s%s", location, GoogleVertexAiUtils.GOOGLE_VERTEX_AI_HOST_SUFFIX))
            .setPathSegments(
                GoogleVertexAiUtils.V1,
                GoogleVertexAiUtils.PROJECTS,
                projectId,
                GoogleVertexAiUtils.LOCATIONS,
                location,
                GoogleVertexAiUtils.PUBLISHERS,
                GoogleVertexAiUtils.PUBLISHER_GOOGLE,
                GoogleVertexAiUtils.MODELS,
                format("%s:%s", modelId, GoogleVertexAiUtils.PREDICT)
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
                    INPUT_TYPE,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.DROPDOWN)
                        .setLabel("Input Type")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies the type of input passed to the model.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .setOptions(
                            Stream.of(
                                InputType.CLASSIFICATION.toString(),
                                InputType.CLUSTERING.toString(),
                                InputType.INGEST.toString(),
                                InputType.SEARCH.toString()
                            ).map(v -> new SettingsConfigurationSelectOption.Builder().setLabelAndValue(v).build()).toList()
                        )
                        .setValue("")
                        .build()
                );

                configurationMap.put(
                    AUTO_TRUNCATE,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TOGGLE)
                        .setLabel("Auto Truncate")
                        .setOrder(2)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies if the API truncates inputs longer than the maximum token length automatically.")
                        .setType(SettingsConfigurationFieldType.BOOLEAN)
                        .setValue(false)
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
