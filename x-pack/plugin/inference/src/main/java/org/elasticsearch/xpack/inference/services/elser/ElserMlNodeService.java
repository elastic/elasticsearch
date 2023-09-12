/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResult;
import org.elasticsearch.xpack.inference.services.InferenceService;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.STARTED;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;

public class ElserMlNodeService implements InferenceService {

    public static final String NAME = "elser_mlnode";

    private static final String ELSER_V1_MODEL = ".elser_model_1";

    public static ElserMlNodeModel parseConfig(
        boolean throwOnUnknownFields,
        String modelId,
        TaskType taskType,
        Map<String, Object> settings
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(settings, Model.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(settings, Model.TASK_SETTINGS);

        var serviceSettings = serviceSettingsFromMap(serviceSettingsMap);
        var taskSettings = taskSettingsFromMap(taskType, taskSettingsMap);

        if (throwOnUnknownFields == false) {
            throwIfNotEmptyMap(settings);
            throwIfNotEmptyMap(serviceSettingsMap);
            throwIfNotEmptyMap(taskSettingsMap);
        }

        return new ElserMlNodeModel(modelId, taskType, NAME, serviceSettings, taskSettings);
    }

    private final OriginSettingClient client;

    public ElserMlNodeService(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    @Override
    public ElserMlNodeModel parseConfigStrict(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfig(true, modelId, taskType, config);
    }

    @Override
    public ElserMlNodeModel parseConfigLenient(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfig(false, modelId, taskType, config);
    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        if (model instanceof ElserMlNodeModel == false) {
            listener.onFailure(new IllegalStateException("Error starting model, [" + model.getModelId() + "] is not an elser model"));
            return;
        }

        if (model.getTaskType() != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(new IllegalStateException(unsupportedTaskTypeErrorMsg(model.getTaskType())));
            return;
        }

        var elserModel = (ElserMlNodeModel) model;
        var serviceSettings = elserModel.getServiceSettings();

        var startRequest = new StartTrainedModelDeploymentAction.Request(ELSER_V1_MODEL, model.getModelId());
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
    public void infer(Model model, String input, Map<String, Object> requestTaskSettings, ActionListener<InferenceResult> listener) {
        // No task settings to override with requestTaskSettings

        if (model.getTaskType() != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(new ElasticsearchStatusException(unsupportedTaskTypeErrorMsg(model.getTaskType()), RestStatus.BAD_REQUEST));
            return;
        }

        var request = InferTrainedModelDeploymentAction.Request.forTextInput(
            model.getModelId(),
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            List.of(input),
            TimeValue.timeValueSeconds(10)  // TODO get timeout from request
        );
        client.execute(InferTrainedModelDeploymentAction.INSTANCE, request, ActionListener.wrap(inferenceResult -> {
            var textExpansionResult = (TextExpansionResults) inferenceResult.getResults().get(0);
            var sparseEmbeddingResult = new SparseEmbeddingResult(textExpansionResult.getWeightedTokens());
            listener.onResponse(sparseEmbeddingResult);
        }, listener::onFailure));
    }

    private static ElserMlNodeServiceSettings serviceSettingsFromMap(Map<String, Object> config) {
        return ElserMlNodeServiceSettings.fromMap(config);
    }

    private static ElserMlNodeTaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.SPARSE_EMBEDDING) {
            throw new ElasticsearchStatusException(unsupportedTaskTypeErrorMsg(taskType), RestStatus.BAD_REQUEST);
        }

        // no config options yet
        throwIfNotEmptyMap(config);

        return ElserMlNodeTaskSettings.DEFAULT;
    }

    @Override
    public String name() {
        return NAME;
    }

    private static void throwIfNotEmptyMap(Map<String, Object> settingsMap) {
        if (settingsMap.isEmpty() == false) {
            throw MapParsingUtils.unknownSettingsError(settingsMap, NAME);
        }
    }

    private static String unsupportedTaskTypeErrorMsg(TaskType taskType) {
        return "The [" + NAME + "] service does not support task type [" + taskType + "]";
    }
}
