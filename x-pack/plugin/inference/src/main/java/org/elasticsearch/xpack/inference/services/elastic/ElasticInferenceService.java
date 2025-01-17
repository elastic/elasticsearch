/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingSparse;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.elastic.ElasticInferenceServiceActionCreator;
import org.elasticsearch.xpack.inference.external.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.ElasticInferenceServiceUnifiedCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceAclRequest;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceAclResponseEntity;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.DEFAULT_TIMEOUT;
import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.useChatCompletionUrlMessage;

public class ElasticInferenceService extends SenderService {

    public static final String NAME = "elastic";
    public static final String ELASTIC_INFERENCE_SERVICE_IDENTIFIER = "Elastic Inference Service";

    private static final Logger logger = LogManager.getLogger(ElasticInferenceService.class);
    private static final EnumSet<TaskType> IMPLEMENTED_TASK_TYPES = EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION);
    private static final String SERVICE_NAME = "Elastic";
    private static final ResponseHandler aclResponseHandler = createAclResponseHandler();

    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(TaskType.SPARSE_EMBEDDING);

    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;
    private Configuration configuration;
    private EnumSet<TaskType> enabledTaskTypes;
    private final ModelRegistry modelRegistry;

    public ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ModelRegistry modelRegistry
    ) {
        super(factory, serviceComponents);
        this.elasticInferenceServiceComponents = Objects.requireNonNull(elasticInferenceServiceComponents);
        this.modelRegistry = Objects.requireNonNull(modelRegistry);

        enabledTaskTypes = EnumSet.noneOf(TaskType.class);
        configuration = new Configuration(enabledTaskTypes);

        init();

        getAcl(elasticInferenceServiceComponents.elasticInferenceServiceUrl());
    }

    // TODO consider removing this should only be used for testing maybe mock getAcl instead?
    ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ModelRegistry modelRegistry,
        ElasticInferenceServiceACL acl
    ) {
        super(factory, serviceComponents);
        this.elasticInferenceServiceComponents = Objects.requireNonNull(elasticInferenceServiceComponents);
        this.modelRegistry = Objects.requireNonNull(modelRegistry);

        setEnabledTaskTypes(acl);
    }

    private void getAcl(String baseEISUrl) {
        ActionListener<InferenceServiceResults> listener = ActionListener.wrap(results -> {
            if (results instanceof ElasticInferenceServiceAclResponseEntity aclResponseEntity) {
                setEnabledTaskTypes(ElasticInferenceServiceACL.of(aclResponseEntity));
            } else {
                logger.warn(
                    Strings.format(
                        "Failed to retrieve ACL information for the Elastic Inference Service gateway. "
                            + "Received an invalid response type: %s",
                        results.getClass().getSimpleName()
                    )
                );
            }
        }, e -> logger.warn(Strings.format("Failed to retrieve ACL information for the Elastic Inference Service gateway: %s", e)));

        var request = new ElasticInferenceServiceAclRequest(baseEISUrl, getCurrentTraceInfo());

        getSender().sendWithoutQueuing(logger, request, aclResponseHandler, DEFAULT_TIMEOUT, listener);
    }

    private static ResponseHandler createAclResponseHandler() {
        return new ElasticInferenceServiceResponseHandler(
            String.format(Locale.ROOT, "%s sparse embeddings", ELASTIC_INFERENCE_SERVICE_IDENTIFIER),
            ElasticInferenceServiceAclResponseEntity::fromResponse
        );
    }

    private synchronized void setEnabledTaskTypes(ElasticInferenceServiceACL acl) {
        enabledTaskTypes = filterTaskTypesByAcl(acl);
        configuration = new Configuration(enabledTaskTypes);

        defaultConfigIds().forEach(modelRegistry::addDefaultIds);
    }

    private static EnumSet<TaskType> filterTaskTypesByAcl(ElasticInferenceServiceACL acl) {
        var implementedTaskTypes = EnumSet.copyOf(IMPLEMENTED_TASK_TYPES);
        implementedTaskTypes.retainAll(acl.enabledTaskTypes());
        return implementedTaskTypes;
    }

    @Override
    public synchronized Set<TaskType> supportedStreamingTasks() {
        var enabledStreamingTaskTypes = EnumSet.of(TaskType.CHAT_COMPLETION);
        enabledStreamingTaskTypes.retainAll(enabledTaskTypes);

        if (enabledStreamingTaskTypes.isEmpty() == false) {
            enabledStreamingTaskTypes.add(TaskType.ANY);
        }

        return enabledStreamingTaskTypes;
    }

    @Override
    public synchronized List<DefaultConfigId> defaultConfigIds() {
        // TODO once we have the enabledTaskTypes figure out which default endpoints we should expose
        return List.of();
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof ElasticInferenceServiceCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        // We extract the trace context here as it's sufficient to propagate the trace information of the REST request,
        // which handles the request to the inference API overall (including the outgoing request, which is started in a new thread
        // generating a different "traceparent" as every task and every REST request creates a new span).
        var currentTraceInfo = getCurrentTraceInfo();

        var completionModel = (ElasticInferenceServiceCompletionModel) model;
        var overriddenModel = ElasticInferenceServiceCompletionModel.of(completionModel, inputs.getRequest());
        var errorMessage = constructFailedToSendRequestMessage(
            overriddenModel.uri(),
            String.format(Locale.ROOT, "%s completions", ELASTIC_INFERENCE_SERVICE_IDENTIFIER)
        );

        var requestManager = ElasticInferenceServiceUnifiedCompletionRequestManager.of(
            overriddenModel,
            getServiceComponents().threadPool(),
            currentTraceInfo
        );
        var action = new SenderExecutableAction(getSender(), requestManager, errorMessage);

        action.execute(inputs, timeout, listener);
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
        if (SUPPORTED_INFERENCE_ACTION_TASK_TYPES.contains(model.getTaskType()) == false) {
            var responseString = ServiceUtils.unsupportedTaskTypeForInference(model, SUPPORTED_INFERENCE_ACTION_TASK_TYPES);

            if (model.getTaskType() == TaskType.CHAT_COMPLETION) {
                responseString = responseString + " " + useChatCompletionUrlMessage(model);
            }
            listener.onFailure(new ElasticsearchStatusException(responseString, RestStatus.BAD_REQUEST));
        }

        if (model instanceof ElasticInferenceServiceExecutableActionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        // We extract the trace context here as it's sufficient to propagate the trace information of the REST request,
        // which handles the request to the inference API overall (including the outgoing request, which is started in a new thread
        // generating a different "traceparent" as every task and every REST request creates a new span).
        var currentTraceInfo = getCurrentTraceInfo();

        ElasticInferenceServiceExecutableActionModel elasticInferenceServiceModel = (ElasticInferenceServiceExecutableActionModel) model;
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
        ActionListener<List<ChunkedInference>> listener
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
    public synchronized InferenceServiceConfiguration getConfiguration() {
        return configuration.get();
    }

    @Override
    public synchronized EnumSet<TaskType> supportedTaskTypes() {
        return enabledTaskTypes;
    }

    @Override
    public synchronized boolean hideFromConfigurationApi() {
        return enabledTaskTypes.isEmpty();
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
            case COMPLETION -> new ElasticInferenceServiceCompletionModel(
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
        return TransportVersions.V_8_16_0;
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

    private static List<ChunkedInference> translateToChunkedResults(InferenceInputs inputs, InferenceServiceResults inferenceResults) {
        if (inferenceResults instanceof SparseEmbeddingResults sparseEmbeddingResults) {
            var inputsAsList = DocumentsOnlyInput.of(inputs).getInputs();
            return ChunkedInferenceEmbeddingSparse.listOf(inputsAsList, sparseEmbeddingResults);
        } else if (inferenceResults instanceof ErrorInferenceResults error) {
            return List.of(new ChunkedInferenceError(error.getException()));
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

        private final EnumSet<TaskType> enabledTaskTypes;
        private final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration;

        public Configuration(EnumSet<TaskType> enabledTaskTypes) {
            this.enabledTaskTypes = enabledTaskTypes;
            configuration = initConfiguration();
        }

        private LazyInitializable<InferenceServiceConfiguration, RuntimeException> initConfiguration() {
            return new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder().setDescription("The name of the model to use for the inference task.")
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    MAX_INPUT_TOKENS,
                    new SettingsConfiguration.Builder().setDescription("Allows you to specify the maximum number of tokens per input.")
                        .setLabel("Maximum Input Tokens")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration());

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(enabledTaskTypes)
                    .setConfigurations(configurationMap)
                    .build();
            });
        }

        public InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }
    }
}
