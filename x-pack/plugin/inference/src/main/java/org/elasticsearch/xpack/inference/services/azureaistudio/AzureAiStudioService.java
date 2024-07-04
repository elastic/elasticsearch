/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.azureaistudio.AzureAiStudioActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsServiceSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProviderCapabilities.providerAllowsEndpointTypeForTask;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProviderCapabilities.providerAllowsTaskType;
import static org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettings.DEFAULT_MAX_NEW_TOKENS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;

public class AzureAiStudioService extends SenderService {

    static final String NAME = "azureaistudio";

    public AzureAiStudioService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
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
        var actionCreator = new AzureAiStudioActionCreator(getSender(), getServiceComponents());

        if (model instanceof AzureAiStudioModel baseAzureAiStudioModel) {
            var action = baseAzureAiStudioModel.accept(actionCreator, taskSettings);
            action.execute(new DocumentsOnlyInput(input), timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
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
        throw new UnsupportedOperationException("Azure AI Studio service does not support inference with query input");
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
        if (model instanceof AzureAiStudioModel baseAzureAiStudioModel) {
            var actionCreator = new AzureAiStudioActionCreator(getSender(), getServiceComponents());
            var batchedRequests = new EmbeddingRequestChunker(input, EMBEDDING_MAX_BATCH_SIZE, EmbeddingRequestChunker.EmbeddingType.FLOAT)
                .batchRequestsWithListeners(listener);
            for (var request : batchedRequests) {
                var action = baseAzureAiStudioModel.accept(actionCreator, taskSettings);
                action.execute(new DocumentsOnlyInput(request.batch().inputs()), timeout, request.listener());
            }
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
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

            AzureAiStudioModel model = createModel(
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

    @Override
    public AzureAiStudioModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

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
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

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
    public String name() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_AZURE_AI_STUDIO;
    }

    private static AzureAiStudioModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {

        if (taskType == TaskType.TEXT_EMBEDDING) {
            var embeddingsModel = new AzureAiStudioEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            checkProviderAndEndpointTypeForTask(
                TaskType.TEXT_EMBEDDING,
                embeddingsModel.getServiceSettings().provider(),
                embeddingsModel.getServiceSettings().endpointType()
            );
            return embeddingsModel;
        }

        if (taskType == TaskType.COMPLETION) {
            var completionModel = new AzureAiStudioChatCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            checkProviderAndEndpointTypeForTask(
                TaskType.COMPLETION,
                completionModel.getServiceSettings().provider(),
                completionModel.getServiceSettings().endpointType()
            );
            return completionModel;
        }

        throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
    }

    private AzureAiStudioModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings,
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

    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof AzureAiStudioEmbeddingsModel embeddingsModel) {
            ServiceUtils.getEmbeddingSize(
                model,
                this,
                listener.delegateFailureAndWrap((l, size) -> l.onResponse(updateEmbeddingModelConfig(embeddingsModel, size)))
            );
        } else if (model instanceof AzureAiStudioChatCompletionModel chatCompletionModel) {
            listener.onResponse(updateChatCompletionModelConfig(chatCompletionModel));
        } else {
            listener.onResponse(model);
        }
    }

    private AzureAiStudioEmbeddingsModel updateEmbeddingModelConfig(AzureAiStudioEmbeddingsModel embeddingsModel, int embeddingsSize) {
        if (embeddingsModel.getServiceSettings().dimensionsSetByUser()
            && embeddingsModel.getServiceSettings().dimensions() != null
            && embeddingsModel.getServiceSettings().dimensions() != embeddingsSize) {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "The retrieved embeddings size [%s] does not match the size specified in the settings [%s]. "
                        + "Please recreate the [%s] configuration with the correct dimensions",
                    embeddingsSize,
                    embeddingsModel.getServiceSettings().dimensions(),
                    embeddingsModel.getConfigurations().getInferenceEntityId()
                ),
                RestStatus.BAD_REQUEST
            );
        }

        var similarityFromModel = embeddingsModel.getServiceSettings().similarity();
        var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

        AzureAiStudioEmbeddingsServiceSettings serviceSettings = new AzureAiStudioEmbeddingsServiceSettings(
            embeddingsModel.getServiceSettings().target(),
            embeddingsModel.getServiceSettings().provider(),
            embeddingsModel.getServiceSettings().endpointType(),
            embeddingsSize,
            embeddingsModel.getServiceSettings().dimensionsSetByUser(),
            embeddingsModel.getServiceSettings().maxInputTokens(),
            similarityToUse,
            embeddingsModel.getServiceSettings().rateLimitSettings()
        );

        return new AzureAiStudioEmbeddingsModel(embeddingsModel, serviceSettings);
    }

    private AzureAiStudioChatCompletionModel updateChatCompletionModelConfig(AzureAiStudioChatCompletionModel chatCompletionModel) {
        var modelMaxNewTokens = chatCompletionModel.getTaskSettings().maxNewTokens();
        var maxNewTokensToUse = modelMaxNewTokens == null ? DEFAULT_MAX_NEW_TOKENS : modelMaxNewTokens;
        var updatedTaskSettings = new AzureAiStudioChatCompletionTaskSettings(
            chatCompletionModel.getTaskSettings().temperature(),
            chatCompletionModel.getTaskSettings().topP(),
            chatCompletionModel.getTaskSettings().doSample(),
            maxNewTokensToUse
        );
        return new AzureAiStudioChatCompletionModel(chatCompletionModel, updatedTaskSettings);
    }

    private static void checkProviderAndEndpointTypeForTask(
        TaskType taskType,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType
    ) {
        if (providerAllowsTaskType(provider, taskType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format("The [%s] task type for provider [%s] is not available", taskType, provider),
                RestStatus.BAD_REQUEST
            );
        }

        if (providerAllowsEndpointTypeForTask(provider, taskType, endpointType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "The [%s] endpoint type with [%s] task type for provider [%s] is not available",
                    endpointType,
                    taskType,
                    provider
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }
}
