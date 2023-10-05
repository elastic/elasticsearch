/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.InferenceServicePlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.STARTED;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.throwIfNotEmptyMap;

public class ElserMlNodeService implements InferenceService {

    public static final String NAME = "elser_mlnode";

    static final String ELSER_V1_MODEL = ".elser_model_1";
    // Default non platform specific v2 model
    static final String ELSER_V2_MODEL = ".elser_model_2";
    static final String ELSER_V2_MODEL_LINUX_X86 = ".elser_model_2_linux-x86_64";

    public static ElserMlNodeModel parseConfig(
        boolean throwOnUnknownFields,
        String modelId,
        TaskType taskType,
        Map<String, Object> settings,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(settings, ModelConfigurations.SERVICE_SETTINGS);
        var serviceSettings = serviceSettingsFromMap(serviceSettingsMap);

        Map<String, Object> taskSettingsMap;
        // task settings are optional
        if (settings.containsKey(ModelConfigurations.TASK_SETTINGS)) {
            taskSettingsMap = removeFromMapOrThrowIfNull(settings, ModelConfigurations.TASK_SETTINGS);
        } else {
            taskSettingsMap = Map.of();
        }

        var taskSettings = taskSettingsFromMap(taskType, taskSettingsMap);

        if (throwOnUnknownFields) {
            throwIfNotEmptyMap(settings, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);
        }

        return new ElserMlNodeModel(modelId, taskType, NAME, serviceSettings, taskSettings);
    }

    private final OriginSettingClient client;

    public ElserMlNodeService(InferenceServicePlugin.InferenceServiceFactoryContext context) {
        this.client = new OriginSettingClient(context.client(), ClientHelper.INFERENCE_ORIGIN);
    }

    @Override
    public ElserMlNodeModel parseRequestConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfig(true, modelId, taskType, config, config);
    }

    @Override
    public ElserMlNodeModel parsePersistedConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parseConfig(false, modelId, taskType, config, secrets);
    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        if (model instanceof ElserMlNodeModel == false) {
            listener.onFailure(
                new IllegalStateException("Error starting model, [" + model.getConfigurations().getModelId() + "] is not an elser model")
            );
            return;
        }

        if (model.getConfigurations().getTaskType() != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(
                new IllegalStateException(TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME))
            );
            return;
        }

        var elserModel = (ElserMlNodeModel) model;
        var serviceSettings = elserModel.getServiceSettings();

        var startRequest = new StartTrainedModelDeploymentAction.Request(
            serviceSettings.getModelVariant(),
            model.getConfigurations().getModelId()
        );
        startRequest.setNumberOfAllocations(serviceSettings.getNumAllocations());
        startRequest.setThreadsPerAllocation(serviceSettings.getNumThreads());
        startRequest.setWaitForState(STARTED);

        client.execute(
            StartTrainedModelDeploymentAction.INSTANCE,
            startRequest,
            ActionListener.wrap(r -> listener.onResponse(Boolean.TRUE), listener::onFailure)
        );
    }

    @Override
    public void infer(Model model, String input, Map<String, Object> taskSettings, ActionListener<InferenceResults> listener) {
        // No task settings to override with requestTaskSettings

        if (model.getConfigurations().getTaskType() != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

        var request = InferTrainedModelDeploymentAction.Request.forTextInput(
            model.getConfigurations().getModelId(),
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            List.of(input),
            TimeValue.timeValueSeconds(10)  // TODO get timeout from request
        );
        client.execute(InferTrainedModelDeploymentAction.INSTANCE, request, ActionListener.wrap(inferenceResult -> {
            var textExpansionResult = (TextExpansionResults) inferenceResult.getResults().get(0);
            listener.onResponse(textExpansionResult);
        }, listener::onFailure));
    }

    private static ElserMlNodeServiceSettings serviceSettingsFromMap(Map<String, Object> config) {
        return ElserMlNodeServiceSettings.fromMap(config);
    }

    private static ElserMlNodeTaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.SPARSE_EMBEDDING) {
            throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        }

        // no config options yet
        return ElserMlNodeTaskSettings.DEFAULT;
    }

    @Override
    public String name() {
        return NAME;
    }
}
