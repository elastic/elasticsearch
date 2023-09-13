/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.services.InferenceService;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;

public class OpenAiService implements InferenceService {

    public static final String NAME = "openai_embeddings";

    public static OpenAiEmbeddingsModel parseConfig(
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

        return new OpenAiEmbeddingsModel(modelId, taskType, NAME, serviceSettings, taskSettings);
    }

    // TODO add http client and CryptoService here
    // private final OriginSettingClient client;

    public OpenAiService() {}

    @Override
    public OpenAiEmbeddingsModel parseConfigStrict(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfig(true, modelId, taskType, config);
    }

    @Override
    public OpenAiEmbeddingsModel parseConfigLenient(String modelId, TaskType taskType, Map<String, Object> config) {
        return parseConfig(false, modelId, taskType, config);
    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        listener.onResponse(Boolean.TRUE);
    }

    @Override
    public void infer(
        String modelId,
        TaskType taskType,
        String input,
        Map<String, Object> config,
        ActionListener<InferenceResult> listener
    ) {

        if (taskType != TaskType.TEXT_EMBEDDING) {
            listener.onFailure(
                new ElasticsearchStatusException("The [{}] service does not support task type [{}]", RestStatus.BAD_REQUEST, NAME, taskType)
            );
            return;
        }

        OpenAiEmbeddingsTaskSettings taskSettings = taskSettingsFromMap(taskType, config);

        // TODO make http request
        // var request = InferTrainedModelDeploymentAction.Request.forTextInput(
        // modelId,
        // TextExpansionConfigUpdate.EMPTY_UPDATE,
        // List.of(input),
        // TimeValue.timeValueSeconds(10) // TODO get timeout from request
        // );
        // client.execute(InferTrainedModelDeploymentAction.INSTANCE, request, ActionListener.wrap(inferenceResult -> {
        // var textExpansionResult = (TextExpansionResults) inferenceResult.getResults().get(0);
        // var sparseEmbeddingResult = new SparseEmbeddingResult(textExpansionResult.getWeightedTokens());
        // listener.onResponse(sparseEmbeddingResult);
        // }, listener::onFailure));
    }

    private static OpenAiEmbeddingsServiceSettings serviceSettingsFromMap(Map<String, Object> config) {
        return OpenAiEmbeddingsServiceSettings.fromMap(config);
    }

    private static OpenAiEmbeddingsTaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.TEXT_EMBEDDING) {
            throw new ElasticsearchStatusException(unsupportedTaskTypeErrorMsg(taskType), RestStatus.BAD_REQUEST);
        }

        return OpenAiEmbeddingsTaskSettings.fromMap(config);
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
