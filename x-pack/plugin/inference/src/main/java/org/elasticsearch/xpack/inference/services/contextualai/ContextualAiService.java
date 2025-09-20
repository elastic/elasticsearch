/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.contextualai.action.ContextualAiActionCreator;
import org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequest;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;
import org.elasticsearch.xpack.inference.services.contextualai.response.ContextualAiRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;


/**
 * Contextual AI inference service for reranking tasks.
 * This service uses the Contextual AI REST API to perform document reranking.
 */
public class ContextualAiService extends SenderService implements RerankingInferenceService {
    public static final String NAME = "contextualai";
    private static final String SERVICE_NAME = "Contextual AI";
    private static final Logger logger = LogManager.getLogger(ContextualAiService.class);

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.RERANK);

    public ContextualAiService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public ContextualAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
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

            ContextualAiRerankModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                serviceSettingsMap,
                TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME),
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

    private static ContextualAiRerankModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        if (taskType != TaskType.RERANK) {
            throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        }
        
        return new ContextualAiRerankModel(inferenceEntityId, serviceSettings, taskSettings, secretSettings, context);
    }

    @Override
    public ContextualAiRerankModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            ServiceUtils.parsePersistedConfigErrorMsg(inferenceEntityId, NAME),
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public ContextualAiRerankModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            null,
            ServiceUtils.parsePersistedConfigErrorMsg(inferenceEntityId, NAME),
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof ContextualAiRerankModel == false) {
            listener.onFailure(ServiceUtils.createInvalidModelException(model));
            return;
        }

        ContextualAiRerankModel contextualAiModel = (ContextualAiRerankModel) model;
        var actionCreator = new ContextualAiActionCreator(getSender(), getServiceComponents());

        var action = contextualAiModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        EmbeddingsInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        listener.onFailure(new ElasticsearchStatusException("Chunked inference is not supported for rerank task", RestStatus.BAD_REQUEST));
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        listener.onFailure(new ElasticsearchStatusException("Unified completion is not supported for rerank task", RestStatus.BAD_REQUEST));
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        // Rerank accepts any input type
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        // Using conservative default as the actual window size is not known
        return RerankingInferenceService.CONSERVATIVE_DEFAULT_WINDOW_SIZE;
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    "model_id",
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The model ID to use for Contextual AI requests."
                    )
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
