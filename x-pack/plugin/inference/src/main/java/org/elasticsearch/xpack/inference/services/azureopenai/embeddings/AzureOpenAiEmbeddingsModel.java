/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public class AzureOpenAiEmbeddingsModel extends AzureOpenAiModel {

    public static AzureOpenAiEmbeddingsModel of(AzureOpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        return new AzureOpenAiEmbeddingsModel(model, AzureOpenAiEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public AzureOpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AzureOpenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AzureOpenAiEmbeddingsTaskSettings.fromMap(taskSettings),
            AzureOpenAiSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    AzureOpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureOpenAiEmbeddingsServiceSettings serviceSettings,
        AzureOpenAiEmbeddingsTaskSettings taskSettings,
        @Nullable AzureOpenAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = getEmbeddingsUri(serviceSettings.resourceName(), serviceSettings.deploymentId(), serviceSettings.apiVersion());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private AzureOpenAiEmbeddingsModel(AzureOpenAiEmbeddingsModel originalModel, AzureOpenAiEmbeddingsTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    public AzureOpenAiEmbeddingsModel(AzureOpenAiEmbeddingsModel originalModel, AzureOpenAiEmbeddingsServiceSettings serviceSettings) {
        super(originalModel, serviceSettings);
    }

    @Override
    public AzureOpenAiEmbeddingsServiceSettings getServiceSettings() {
        return (AzureOpenAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureOpenAiEmbeddingsTaskSettings getTaskSettings() {
        return (AzureOpenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public AzureOpenAiSecretSettings getSecretSettings() {
        return (AzureOpenAiSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    public static URI getEmbeddingsUri(String resourceName, String deploymentId, String apiVersion) throws URISyntaxException {
        String hostname = format("%s.%s", resourceName, AzureOpenAiUtils.HOST_SUFFIX);
        return new URIBuilder().setScheme("https")
            .setHost(hostname)
            .setPathSegments(
                AzureOpenAiUtils.OPENAI_PATH,
                AzureOpenAiUtils.DEPLOYMENTS_PATH,
                deploymentId,
                AzureOpenAiUtils.EMBEDDINGS_PATH
            )
            .addParameter(AzureOpenAiUtils.API_VERSION_PARAMETER, apiVersion)
            .build();
    }
}
