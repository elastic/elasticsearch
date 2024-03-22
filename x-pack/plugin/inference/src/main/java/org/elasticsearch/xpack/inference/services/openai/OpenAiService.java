/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class OpenAiService extends SenderService {
    public static final String NAME = "openai";

    public OpenAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            moveModelFromTaskToServiceSettings(taskSettingsMap, serviceSettingsMap);

            OpenAiModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                serviceSettingsMap,
                TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME),
                ConfigurationParseContext.REQUEST
            );

            throwIfNotEmptyMap(config, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    private static OpenAiModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            taskSettings,
            secretSettings,
            failureMessage,
            ConfigurationParseContext.PERSISTENT
        );
    }

    private static OpenAiModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new OpenAiEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public OpenAiModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        moveModelFromTaskToServiceSettings(taskSettingsMap, serviceSettingsMap);

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
    }

    @Override
    public OpenAiModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        moveModelFromTaskToServiceSettings(taskSettingsMap, serviceSettingsMap);

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            null,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
    }

    @Override
    public void doInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
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

    @Override
    protected void doChunkedInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        listener.onFailure(new ElasticsearchStatusException("Chunking not supported by the {} service", RestStatus.BAD_REQUEST, NAME));
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
        if (model.getServiceSettings().dimensionsSetByUser()
            && model.getServiceSettings().dimensions() != null
            && model.getServiceSettings().dimensions() != embeddingSize) {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "The retrieved embeddings size [%s] does not match the size specified in the settings [%s]. "
                        + "Please recreate the [%s] configuration with the correct dimensions",
                    embeddingSize,
                    model.getServiceSettings().dimensions(),
                    model.getConfigurations().getInferenceEntityId()
                ),
                RestStatus.BAD_REQUEST
            );
        }

        OpenAiEmbeddingsServiceSettings serviceSettings = new OpenAiEmbeddingsServiceSettings(
            model.getServiceSettings().modelId(),
            model.getServiceSettings().uri(),
            model.getServiceSettings().organizationId(),
            SimilarityMeasure.DOT_PRODUCT,
            embeddingSize,
            model.getServiceSettings().maxInputTokens(),
            model.getServiceSettings().dimensionsSetByUser()
        );

        return new OpenAiEmbeddingsModel(model, serviceSettings);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    /**
     * Model was originally defined in task settings, but it should
     * have been part of the service settings.
     *
     * If model or model_id are in the task settings map move
     * them to service settings ready for parsing
     *
     * @param taskSettings Task settings map
     * @param serviceSettings Service settings map
     */
    static void moveModelFromTaskToServiceSettings(Map<String, Object> taskSettings, Map<String, Object> serviceSettings) {
        if (serviceSettings.containsKey(MODEL_ID)) {
            return;
        }

        final String OLD_MODEL_ID_FIELD = "model";
        var oldModelId = taskSettings.remove(OLD_MODEL_ID_FIELD);
        if (oldModelId != null) {
            serviceSettings.put(MODEL_ID, oldModelId);
        } else {
            var modelId = taskSettings.remove(MODEL_ID);
            serviceSettings.put(MODEL_ID, modelId);
        }
    }
}
