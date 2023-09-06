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
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.TaskSettings;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResult;
import org.elasticsearch.xpack.inference.services.InferenceService;

import java.util.List;
import java.util.Map;

public class ElserMlNodeService implements InferenceService {

    public static final String NAME = "elser";

    public static Model parseConfigLenient(String modelId, TaskType taskType, Map<String, Object> settings) {
        Map<String, Object> serviceSettings = removeMApOrThrowIfNull(settings, Model.SERVICE_SETTINGS);
        Map<String, Object> taskSettings = removeMApOrThrowIfNull(settings, Model.TASK_SETTINGS);

        return new Model(modelId, taskType, NAME, serviceSettingsFromMap(serviceSettings), taskSettingsFromMap(taskType, taskSettings));
    }

    private final OriginSettingClient client;

    public ElserMlNodeService(Client client) {
        this.client = new OriginSettingClient(client, InferencePlugin.INFERENCE_ORIGIN);
    }

    @Override
    public Model parseConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfigLenient(modelId, taskType, config);
    }

    @Override
    public void infer(
        String modelId,
        TaskType taskType,
        String input,
        Map<String, Object> config,
        ActionListener<InferenceResult> listener
    ) {

        if (taskType != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "The Elser ML Node service does not support task type [{}]",
                    RestStatus.BAD_REQUEST,
                    taskType
                )
            );
            return;
        }

        var request = InferTrainedModelDeploymentAction.Request.forTextInput(
            modelId,
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

    private static ServiceSettings serviceSettingsFromMap(Map<String, Object> config) {
        // no config yet
        if (config.isEmpty() == false) {
            throw unknownSettingsError(config);
        }
        return new ElserServiceSettings();
    }

    private static TaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.SPARSE_EMBEDDING) {
            throw new ElasticsearchStatusException(
                "The [{}] service does not support task type [{}]",
                RestStatus.BAD_REQUEST,
                NAME,
                taskType
            );
        }

        // no config yet
        if (config.isEmpty() == false) {
            throw unknownSettingsError(config);
        }

        return new ElserSparseEmbeddingTaskSettings();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> removeMApOrThrowIfNull(Map<String, Object> sourceMap, String fieldName) {
        Map<String, Object> value = (Map<String, Object>) sourceMap.remove(fieldName);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }
        return value;
    }

    private static ElasticsearchStatusException unknownSettingsError(Map<String, Object> config) {
        // TOOD map as JSON
        return new ElasticsearchStatusException(
            "Model configuration contains settings [{}] unknown to the [{}] service",
            RestStatus.BAD_REQUEST,
            config,
            NAME
        );
    }

    @Override
    public String name() {
        return NAME;
    }
}
