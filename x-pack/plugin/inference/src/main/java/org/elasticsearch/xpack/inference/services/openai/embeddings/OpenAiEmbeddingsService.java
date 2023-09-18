/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiEmbeddingsAction;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.services.InferenceService;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;

public class OpenAiEmbeddingsService implements InferenceService {

    public static final String NAME = "openai";

    private final ThreadPool threadPool;
    private final HttpClient httpClient;

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

    public OpenAiEmbeddingsService(ThreadPool threadPool, HttpClient httpClient) {
        this.threadPool = threadPool;
        this.httpClient = httpClient;
    }

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
    public void infer(Model model, String input, Map<String, Object> requestTaskSettings, ActionListener<InferenceResult> listener) {
        if (model.getTaskType() != TaskType.TEXT_EMBEDDING) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "The [{}] service does not support task type [{}]",
                    RestStatus.BAD_REQUEST,
                    NAME,
                    model.getTaskType()
                )
            );
            return;
        }

        if (model instanceof OpenAiEmbeddingsModel == false) {
            listener.onFailure(new ElasticsearchStatusException("The internal model was invalid", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }

        OpenAiEmbeddingsAction action = getOpenAiEmbeddingsAction((OpenAiEmbeddingsModel) model, input, listener);
        action.execute();
    }

    private OpenAiEmbeddingsAction getOpenAiEmbeddingsAction(
        OpenAiEmbeddingsModel model,
        String input,
        ActionListener<InferenceResult> listener
    ) {
        // TODO figure out how to merge these settings together
        // OpenAiEmbeddingsTaskSettings parsedRequestTaskSettings = taskSettingsFromMap(model.getTaskType(), requestTaskSettings);

        return new OpenAiEmbeddingsAction(threadPool, input, httpClient, model.getServiceSettings(), model.getTaskSettings(), listener);
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
