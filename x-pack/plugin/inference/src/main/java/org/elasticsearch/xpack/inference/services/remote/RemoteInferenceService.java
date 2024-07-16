/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.remote;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultTaskSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;

public class RemoteInferenceService extends SenderService {
    private final Map<TaskType, RemoteInferenceIntegration> integrations;
    private final String name;

    public RemoteInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        Map<TaskType, RemoteInferenceIntegration> integrations,
        String name
    ) {
        super(factory, serviceComponents);
        this.integrations = integrations;
        this.name = name;
    }

    @Override
    protected void doInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {

    }

    @Override
    protected void doInfer(
        Model model,
        String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {

    }

    @Override
    protected void doChunkedInfer(
        Model model,
        String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {

    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        ActionListener<Model> parsedModelListener
    ) {
        ActionListener.completeWith(parsedModelListener, () -> {
            var integration = integrations.get(taskType);
            var secretSettings = integration.parseSecretSettings(config);
            var serviceSettings = integration.parseServiceSettings(config);
            var taskSettings = integration.parseTaskSettings(config);
            return new RemoteInferenceModel(modelId, taskType, name, serviceSettings, secretSettings, taskSettings);
        });
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        var serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        var taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        var serviceSettings = DefaultServiceSettings.fromStorage(serviceSettingsMap);
        var taskSettings = DefaultTaskSettings.fromStorage(taskSettingsMap);
        var secretSettings = secrets != null ? DefaultSecretSettings.fromMap(secrets) : null;
        return new RemoteInferenceModel(modelId, taskType, name, serviceSettings, secretSettings, taskSettings);
    }

    @Override
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        return parsePersistedConfigWithSecrets(modelId, taskType, config, null);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null;
    }
}
