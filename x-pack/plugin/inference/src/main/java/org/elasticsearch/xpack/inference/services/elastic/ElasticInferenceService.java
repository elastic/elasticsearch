/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.EmptySettingsConfiguration;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskSettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.action.elastic.ElasticInferenceServiceActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class ElasticInferenceService extends SenderService {

    public static final String NAME = "elastic";
    public static final String ELASTIC_INFERENCE_SERVICE_IDENTIFIER = "Elastic Inference Service";

    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;

    private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.SPARSE_EMBEDDING);

    public ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(factory, serviceComponents);
        this.elasticInferenceServiceComponents = elasticInferenceServiceComponents;
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof ElasticInferenceServiceModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        // We extract the trace context here as it's sufficient to propagate the trace information of the REST request,
        // which handles the request to the inference API overall (including the outgoing request, which is started in a new thread
        // generating a different "traceparent" as every task and every REST request creates a new span).
        var currentTraceInfo = getCurrentTraceInfo();

        ElasticInferenceServiceModel elasticInferenceServiceModel = (ElasticInferenceServiceModel) model;
        var actionCreator = new ElasticInferenceServiceActionCreator(getSender(), getServiceComponents(), currentTraceInfo);

        var action = elasticInferenceServiceModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        DocumentsOnlyInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        // Pass-through without actually performing chunking (result will have a single chunk per input)
        ActionListener<InferenceServiceResults> inferListener = listener.delegateFailureAndWrap(
            (delegate, response) -> delegate.onResponse(translateToChunkedResults(inputs, response))
        );

        doInfer(model, inputs, taskSettings, inputType, timeout, inferListener);
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

            ElasticInferenceServiceModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                serviceSettingsMap,
                elasticInferenceServiceComponents,
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
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return supportedTaskTypes;
    }

    private static ElasticInferenceServiceModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        ElasticInferenceServiceComponents eisServiceComponents,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new ElasticInferenceServiceSparseEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                eisServiceComponents,
                context
            );
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
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
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_EIS_INTEGRATION_ADDED;
    }

    private ElasticInferenceServiceModel createModelFromPersistent(
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
            elasticInferenceServiceComponents,
            failureMessage,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof ElasticInferenceServiceSparseEmbeddingsModel embeddingsModel) {
            listener.onResponse(updateModelWithEmbeddingDetails(embeddingsModel));
        } else {
            listener.onResponse(model);
        }
    }

    private static List<ChunkedInferenceServiceResults> translateToChunkedResults(
        InferenceInputs inputs,
        InferenceServiceResults inferenceResults
    ) {
        if (inferenceResults instanceof SparseEmbeddingResults sparseEmbeddingResults) {
            return InferenceChunkedSparseEmbeddingResults.listOf(DocumentsOnlyInput.of(inputs).getInputs(), sparseEmbeddingResults);
        } else if (inferenceResults instanceof ErrorInferenceResults error) {
            return List.of(new ErrorChunkedInferenceResults(error.getException()));
        } else {
            String expectedClass = Strings.format("%s", SparseEmbeddingResults.class.getSimpleName());
            throw createInvalidChunkedResultException(expectedClass, inferenceResults.getWriteableName());
        }
    }

    private ElasticInferenceServiceSparseEmbeddingsModel updateModelWithEmbeddingDetails(
        ElasticInferenceServiceSparseEmbeddingsModel model
    ) {
        ElasticInferenceServiceSparseEmbeddingsServiceSettings serviceSettings = new ElasticInferenceServiceSparseEmbeddingsServiceSettings(
            model.getServiceSettings().modelId(),
            model.getServiceSettings().maxInputTokens(),
            model.getServiceSettings().rateLimitSettings()
        );

        return new ElasticInferenceServiceSparseEmbeddingsModel(model, serviceSettings);
    }

    private TraceContext getCurrentTraceInfo() {
        var threadPool = getServiceComponents().threadPool();

        var traceParent = threadPool.getThreadContext().getHeader(Task.TRACE_PARENT);
        var traceState = threadPool.getThreadContext().getHeader(Task.TRACE_STATE);

        return new TraceContext(traceParent, traceState);
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
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("Model ID")
                        .setOrder(2)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The name of the model to use for the inference task.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    MAX_INPUT_TOKENS,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Maximum Input Tokens")
                        .setOrder(3)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Allows you to specify the maximum number of tokens per input.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration());

                return new InferenceServiceConfiguration.Builder().setProvider(NAME).setTaskTypes(supportedTaskTypes.stream().map(t -> {
                    Map<String, SettingsConfiguration> taskSettingsConfig;
                    switch (t) {
                        // SPARSE_EMBEDDING task type has no task settings
                        default -> taskSettingsConfig = EmptySettingsConfiguration.get();
                    }
                    return new TaskSettingsConfiguration.Builder().setTaskType(t).setConfiguration(taskSettingsConfig).build();
                }).toList()).setConfiguration(configurationMap).build();
            }
        );
    }
}
