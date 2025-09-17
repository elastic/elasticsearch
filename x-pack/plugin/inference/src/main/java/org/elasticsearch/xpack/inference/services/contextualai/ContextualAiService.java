/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
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

    private static final ResponseHandler RERANK_HANDLER = createRerankHandler();

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
            if (taskType != TaskType.RERANK) {
                throw new ElasticsearchStatusException(
                    format("The [%s] service does not support task type [%s]", NAME, taskType),
                    RestStatus.BAD_REQUEST
                );
            }
            System.out.println("\n*************************************WHERE AM I?\n");
            System.out.printf("ContextualAiService - parseRequestConfig - inferenceEntityId: %s\n", inferenceEntityId);
            logger.info("ContextualAiService loaded from: {}",ContextualAiService.class.getProtectionDomain().getCodeSource().getLocation());

            var serviceSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.SERVICE_SETTINGS);
            var taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
            var secretsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelSecrets.SECRET_SETTINGS);

            var model = new ContextualAiRerankModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettingsMap,
                taskSettingsMap,
                secretsMap,
                ConfigurationParseContext.REQUEST
            );

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return new ContextualAiRerankModel(
            inferenceEntityId,
            taskType,
            NAME,
            config,
            Map.of(),
            secrets,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        return parsePersistedConfigWithSecrets(inferenceEntityId, taskType, config, Map.of());
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof ContextualAiRerankModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var contextualAiModel = (ContextualAiRerankModel) model;
        
        // Debug logging to see what task settings are being passed
        logger.debug("ContextualAI doInfer - taskSettings: {}", taskSettings);
        logger.debug("ContextualAI doInfer - model task settings: {}", 
            contextualAiModel.getTaskSettings() != null ? 
                "topN=" + contextualAiModel.getTaskSettings().getTopN() + 
                ", returnDocs=" + contextualAiModel.getTaskSettings().getReturnDocuments() : 
                "null");
        
        // Safely convert inputs with proper error handling
        QueryAndDocsInputs rerankInput;
        try {
            if (inputs == null) {
                throw new IllegalArgumentException("Inference inputs cannot be null");
            }
            rerankInput = QueryAndDocsInputs.of(inputs);
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchStatusException(
                format("Invalid input type for ContextualAI rerank service: %s", e.getMessage()),
                RestStatus.BAD_REQUEST,
                e
            ));
            return;
        }

        var actionCreator = new ContextualAiActionCreator(getSender(), getServiceComponents());
        var action = actionCreator.create(contextualAiModel, taskSettings, RERANK_HANDLER);
        action.execute(rerankInput, timeout, listener);
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
        // TODO: Make this configurable or retrieve from model metadata
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

    private static ResponseHandler createRerankHandler() {
        return new BaseResponseHandler("contextual_ai_rerank", (request, result) -> {
            if (request instanceof ContextualAiRerankRequest == false) {
                throw new IllegalArgumentException(
                    format(
                        "Invalid request type: expected ContextualAi rerank request but got %s",
                        request != null ? request.getClass().getSimpleName() : "null"
                    )
                );
            }
            try {
                return ContextualAiRerankResponseEntity.fromResponse((ContextualAiRerankRequest) request, result);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse response: " + e.getMessage(), e);
            }
        },
            result -> null // No specific error parsing for now
        ) {
            @Override
            protected void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
                if (result.isSuccessfulResponse()) {
                    return;
                }

                int statusCode = result.response().getStatusLine().getStatusCode();
                if (statusCode == 503 || statusCode == 502 || statusCode == 429) {
                    throw new RetryException(
                        true,
                        new ElasticsearchStatusException(
                            format("Contextual AI rate limit reached. Status code %d", statusCode),
                            RestStatus.fromCode(statusCode)
                        )
                    );
                } else if (statusCode >= 500) {
                    throw new RetryException(
                        false,
                        new ElasticsearchStatusException(
                            format("Contextual AI server error. Status code %d", statusCode),
                            RestStatus.fromCode(statusCode)
                        )
                    );
                } else if (statusCode == 401) {
                    throw new RetryException(
                        false,
                        new ElasticsearchStatusException("Contextual AI authentication failed. Check your API key", RestStatus.UNAUTHORIZED)
                    );
                } else {
                    throw new RetryException(
                        false,
                        new ElasticsearchStatusException(
                            format("Contextual AI request failed with status code %d", statusCode),
                            RestStatus.fromCode(statusCode)
                        )
                    );
                }
            }
        };
    }

    private static String buildErrorMessage(TaskType taskType, String inferenceId) {
        return format("Failed to send Contextual AI %s request from inference entity id [%s]", taskType.toString(), inferenceId);
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private Configuration() {}

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The URL endpoint to use for Contextual AI requests."
                    )
                        .setLabel("URL")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

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
