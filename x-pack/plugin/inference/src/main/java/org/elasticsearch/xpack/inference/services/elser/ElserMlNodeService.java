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
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.TaskSettings;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResult;
import org.elasticsearch.xpack.inference.services.InferenceService;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.unknownSettingsError;

public class ElserMlNodeService implements InferenceService {

    public static final String NAME = "elser";

    public static Model parseConfigLenient(String modelId, TaskType taskType, Map<String, Object> settings) {
        Map<String, Object> serviceSettings = removeFromMapOrThrowIfNull(settings, Model.SERVICE_SETTINGS);
        Map<String, Object> taskSettings = removeFromMapOrThrowIfNull(settings, Model.TASK_SETTINGS);

        return new Model(modelId, taskType, NAME, serviceSettingsFromMap(serviceSettings), taskSettingsFromMap(taskType, taskSettings));
    }

    private final OriginSettingClient client;

    public ElserMlNodeService(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    @Override
    public Model parseConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfigLenient(modelId, taskType, config);
    }

    public void init(Model model, ActionListener<Boolean> listener) {

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
        // no config options yet
        if (config.isEmpty() == false) {
            throw unknownSettingsError(config, NAME);
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

//        Integer numAllocations = config.get(ElserTaskSettings.NUM_ALLOCATIONS);
//        Integer numThreads = config.get(ElserTaskSettings.NUM_THREADS);
        // no config options yet
        if (config.isEmpty() == false) {
            throw unknownSettingsError(config, NAME);
        }

        return ElserTaskSettings.DEFAULT;
    }

    @Override
    public String name() {
        return NAME;
    }
}
