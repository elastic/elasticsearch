/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.common.SimilarityMeasure;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class OpenAiService extends SenderService {
    public static final String NAME = "openai";

    public OpenAiService(SetOnce<HttpRequestSenderFactory> factory, SetOnce<ServiceComponents> serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public OpenAiModel parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);

        OpenAiModel model = createModel(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            serviceSettingsMap,
            TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME)
        );

        throwIfNotEmptyMap(config, NAME);
        throwIfNotEmptyMap(serviceSettingsMap, NAME);
        throwIfNotEmptyMap(taskSettingsMap, NAME);

        return model;
    }

    private static OpenAiModel createModel(
        String modelId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new OpenAiEmbeddingsModel(modelId, taskType, NAME, serviceSettings, taskSettings, secretSettings);
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public OpenAiModel parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        return createModel(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(modelId, NAME)
        );
    }

    @Override
    public OpenAiModel parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);

        return createModel(modelId, taskType, serviceSettingsMap, taskSettingsMap, null, parsePersistedConfigErrorMsg(modelId, NAME));
    }

    @Override
    public void doInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof OpenAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        OpenAiModel openAiModel = (OpenAiModel) model;
        var actionCreator = new OpenAiActionCreator(getSender(), getServiceComponents());

        var action = openAiModel.accept(actionCreator, taskSettings);
        action.execute(input, listener);
    }

    /**
     * For text embedding models get the embedding size and
     * update the service settings.
     *
     * @param model The new model
     * @param listener The listener
     */
    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof OpenAiEmbeddingsModel embeddingsModel) {
            ServiceUtils.getEmbeddingSize(
                model,
                this,
                listener.delegateFailureAndWrap((l, size) -> l.onResponse(updateModelWithEmbeddingDetails(embeddingsModel, size)))
            );
        } else {
            listener.onResponse(model);
        }
    }

    private OpenAiEmbeddingsModel updateModelWithEmbeddingDetails(OpenAiEmbeddingsModel model, int embeddingSize) {
        OpenAiServiceSettings serviceSettings = new OpenAiServiceSettings(
            model.getServiceSettings().uri(),
            model.getServiceSettings().organizationId(),
            SimilarityMeasure.DOT_PRODUCT,
            embeddingSize,
            model.getServiceSettings().maxInputTokens()
        );

        return new OpenAiEmbeddingsModel(model, serviceSettings);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_OPENAI_ADDED;
    }
}
