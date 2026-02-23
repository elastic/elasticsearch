/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionCreator;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModelCreator;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.EMBEDDING_MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.PROJECT_ID;

public class IbmWatsonxService extends SenderService implements RerankingInferenceService {

    private static final String SERVICE_NAME = "IBM watsonx";
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK
    );
    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new IbmWatsonUnifiedChatCompletionResponseHandler(
        "IBM watsonx chat completions",
        OpenAiChatCompletionResponseEntity::fromResponse
    );
    private static final IbmWatsonxChatCompletionModelCreator CHAT_COMPLETION_MODEL_CREATOR = new IbmWatsonxChatCompletionModelCreator();
    private static final Map<TaskType, ModelCreator<? extends IbmWatsonxModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        new IbmWatsonxEmbeddingsModelCreator(),
        TaskType.COMPLETION,
        CHAT_COMPLETION_MODEL_CREATOR,
        TaskType.CHAT_COMPLETION,
        CHAT_COMPLETION_MODEL_CREATOR,
        TaskType.RERANK,
        new IbmWatsonxRerankModelCreator()
    );

    public static final String NAME = "watsonxai";

    // IBM watsonx has a single rerank model with a token limit of 512
    // (see https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/fm-models-embed.html?context=wx#reranker-overview)
    // Using 1 token = 0.75 words as a rough estimate, we get 384 words
    // allowing for some headroom, we set the window size below 384 words
    public static final int RERANK_WINDOW_SIZE = 350;

    public IbmWatsonxService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public IbmWatsonxService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService);
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
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            ChunkingSettings chunkingSettings = null;
            if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
                chunkingSettings = ChunkingSettingsBuilder.fromMap(
                    removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
                );
            }

            IbmWatsonxModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                chunkingSettings,
                serviceSettingsMap,
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

    private static IbmWatsonxModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return retrieveModelCreatorFromMapOrThrow(MODEL_CREATORS, inferenceEntityId, taskType, NAME, context).createFromMaps(
            inferenceEntityId,
            taskType,
            NAME,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            context
        );
    }

    @Override
    public IbmWatsonxModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap
        );
    }

    @Override
    public Model buildModelFromConfigAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return retrieveModelCreatorFromMapOrThrow(
            MODEL_CREATORS,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.PERSISTENT
        ).createFromModelConfigurationsAndSecrets(config, secrets);
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
    }

    private static IbmWatsonxModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(inferenceEntityId, taskType, serviceSettingsMap, taskSettingsMap, chunkingSettings, null);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof IbmWatsonxEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            var updatedServiceSettings = new IbmWatsonxEmbeddingsServiceSettings(
                serviceSettings.modelId(),
                serviceSettings.projectId(),
                serviceSettings.url(),
                serviceSettings.apiVersion(),
                serviceSettings.maxInputTokens(),
                embeddingSize,
                similarityToUse,
                serviceSettings.rateLimitSettings()
            );

            return new IbmWatsonxEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs input,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof IbmWatsonxModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        IbmWatsonxModel ibmWatsonxModel = (IbmWatsonxModel) model;

        var action = ibmWatsonxModel.accept(getActionCreator(getSender(), getServiceComponents()), taskSettings);
        action.execute(input, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof IbmWatsonxChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        IbmWatsonxChatCompletionModel ibmWatsonxChatCompletionModel = (IbmWatsonxChatCompletionModel) model;
        var overriddenModel = IbmWatsonxChatCompletionModel.of(ibmWatsonxChatCompletionModel, inputs.getRequest());
        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new IbmWatsonxChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );
        var errorMessage = IbmWatsonxActionCreator.buildErrorMessage(TaskType.CHAT_COMPLETION, model.getInferenceEntityId());
        var action = new SenderExecutableAction(getSender(), manager, errorMessage);

        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        List<ChunkInferenceInput> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        IbmWatsonxModel ibmWatsonxModel = (IbmWatsonxModel) model;

        var batchedRequests = new EmbeddingRequestChunker<>(
            input,
            EMBEDDING_MAX_BATCH_SIZE,
            ibmWatsonxModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        var actionCreator = getActionCreator(getSender(), getServiceComponents());

        for (var request : batchedRequests) {
            var action = ibmWatsonxModel.accept(actionCreator, taskSettings);
            action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
        }
    }

    protected IbmWatsonxActionCreator getActionCreator(Sender sender, ServiceComponents serviceComponents) {
        return new IbmWatsonxActionCreator(getSender(), getServiceComponents());
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        return RERANK_WINDOW_SIZE;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    API_VERSION,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("The IBM watsonx API version ID to use.")
                        .setLabel("API Version")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    PROJECT_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("")
                        .setLabel("Project ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The name of the model to use for the inference task."
                    )
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("")
                        .setLabel("URL")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    MAX_INPUT_TOKENS,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING)).setDescription(
                        "Allows you to specify the maximum number of tokens per input."
                    )
                        .setLabel("Maximum Input Tokens")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
