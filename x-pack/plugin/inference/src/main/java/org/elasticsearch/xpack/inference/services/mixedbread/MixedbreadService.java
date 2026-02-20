/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadActionCreator;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModelCreator;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedUnifiedCompletionOperation;

/**
 * Mixedbread inference service for reranking tasks.
 * This service uses the Mixedbread REST API to perform document reranking.
 */
public class MixedbreadService extends SenderService implements RerankingInferenceService {
    public static final String NAME = "mixedbread";
    public static final String SERVICE_NAME = "Mixedbread";

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.RERANK);

    /**
     * {@link #rerankerWindowSize(String modelId)} method returns the size in words, not in tokens, so we'll need to translate
     * tokens to words by multiplying by 0.75 and rounding down

     * The context window size for v1 models is 512 tokens / 300 words
     * For v2 models it is from 8k / 5500 words to 32k / 22000 words
     * <a href="https://github.com/elastic/elasticsearch/pull/132169">tokens to words conversion reference</a>
     */
    private static final int DEFAULT_RERANKER_INPUT_SIZE_WORDS = 22000;

    private static final Map<String, Integer> RERANKERS_INPUT_SIZE = Map.of(
        "mixedbread-ai/mxbai-rerank-xsmall-v1",
        300,
        "mixedbread-ai/mxbai-rerank-base-v1",
        300,
        "mixedbread-ai/mxbai-rerank-large-v1",
        300
    );

    private static final Map<TaskType, ModelCreator<? extends MixedbreadModel>> MODEL_CREATORS = Map.of(
        TaskType.RERANK,
        new MixedbreadRerankModelCreator()
    );

    /**
     * Constructor for creating an {@link MixedbreadService} with specified HTTP request sender factory and service components.
     *
     * @param factory the factory to create HTTP request senders
     * @param serviceComponents the components required for the inference service
     * @param context the context for the inference service factory
     */
    public MixedbreadService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public MixedbreadService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
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
            Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            MixedbreadModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                null,
                serviceSettingsMap,
                ConfigurationParseContext.REQUEST
            );

            ServiceUtils.throwIfNotEmptyMap(config, NAME);
            ServiceUtils.throwIfNotEmptyMap(serviceSettingsMap, NAME);
            ServiceUtils.throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    private MixedbreadModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings
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

    /**
     * Creates an {@link MixedbreadModel} based on the provided parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param serviceSettings the settings for the inference service
     * @param taskSettings the task-specific settings, if applicable
     * @param chunkingSettings the settings for chunking, if applicable
     * @param secretSettings the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link MixedbreadModel} based on the provided parameters
     */
    protected MixedbreadModel createModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return retrieveModelCreatorFromMapOrThrow(MODEL_CREATORS, inferenceId, taskType, NAME, context).createFromMaps(
            inferenceId,
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
    public MixedbreadModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        return parsePersistedConfigWithSecrets(inferenceEntityId, taskType, serviceSettingsMap, taskSettingsMap, null, secretSettingsMap);
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
    public MixedbreadModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        return parsePersistedConfigWithSecrets(inferenceEntityId, taskType, serviceSettingsMap, taskSettingsMap, null, null);
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throwUnsupportedUnifiedCompletionOperation(NAME);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        List<ChunkInferenceInput> inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        throw new UnsupportedOperationException(Strings.format("%s service does not support chunked inference", NAME));
    }

    @Override
    protected boolean supportsChunkedInfer() {
        return false;
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof MixedbreadModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        MixedbreadModel mixedbreadModel = (MixedbreadModel) model;
        var actionCreator = new MixedbreadActionCreator(getSender(), getServiceComponents());

        var action = mixedbreadModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {}

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return MixedbreadUtils.INFERENCE_MIXEDBREAD_ADDED;
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        Integer inputSize = RERANKERS_INPUT_SIZE.get(modelId);
        return inputSize != null ? inputSize : DEFAULT_RERANKER_INPUT_SIZE_WORDS;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("The model ID to use for Mixedbread requests.")
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(DefaultSecretSettings.toSettingsConfiguration(SUPPORTED_TASK_TYPES));
                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration(SUPPORTED_TASK_TYPES));

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
