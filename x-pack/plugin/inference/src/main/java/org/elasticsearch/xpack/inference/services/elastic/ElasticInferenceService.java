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
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elastic.action.ElasticInferenceServiceActionCreator;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationHandler;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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

    private static final EnumSet<TaskType> IMPLEMENTED_TASK_TYPES = EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION);
    private static final String SERVICE_NAME = "Elastic";

    // rainbow-sprinkles
    static final String DEFAULT_CHAT_COMPLETION_MODEL_ID_V1 = "rainbow-sprinkles";
    static final String DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1 = defaultEndpointId(DEFAULT_CHAT_COMPLETION_MODEL_ID_V1);

    // elser-v2
    static final String DEFAULT_ELSER_MODEL_ID_V2 = "elser-v2";
    static final String DEFAULT_ELSER_ENDPOINT_ID_V2 = defaultEndpointId(DEFAULT_ELSER_MODEL_ID_V2);

    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(TaskType.SPARSE_EMBEDDING);

    public static String defaultEndpointId(String modelId) {
        return Strings.format(".%s-elastic", modelId);
    }

    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;
    private final ElasticInferenceServiceAuthorizationHandler authorizationHandler;

    public ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ModelRegistry modelRegistry,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler
    ) {
        super(factory, serviceComponents);
        this.elasticInferenceServiceComponents = new ElasticInferenceServiceComponents(
            elasticInferenceServiceSettings.getElasticInferenceServiceUrl()
        );
        authorizationHandler = new ElasticInferenceServiceAuthorizationHandler(
            serviceComponents,
            modelRegistry,
            authorizationRequestHandler,
            initDefaultEndpoints(elasticInferenceServiceComponents),
            IMPLEMENTED_TASK_TYPES,
            this,
            getSender(),
            elasticInferenceServiceSettings
        );
    }

    private static Map<String, DefaultModelConfig> initDefaultEndpoints(
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        return Map.of(
            DEFAULT_CHAT_COMPLETION_MODEL_ID_V1,
            new DefaultModelConfig(
                new ElasticInferenceServiceCompletionModel(
                    DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                    TaskType.CHAT_COMPLETION,
                    NAME,
                    new ElasticInferenceServiceCompletionServiceSettings(DEFAULT_CHAT_COMPLETION_MODEL_ID_V1, null),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    elasticInferenceServiceComponents
                ),
                MinimalServiceSettings.chatCompletion(NAME)
            ),
            DEFAULT_ELSER_MODEL_ID_V2,
            new DefaultModelConfig(
                new ElasticInferenceServiceSparseEmbeddingsModel(
                    DEFAULT_ELSER_ENDPOINT_ID_V2,
                    TaskType.SPARSE_EMBEDDING,
                    NAME,
                    new ElasticInferenceServiceSparseEmbeddingsServiceSettings(DEFAULT_ELSER_MODEL_ID_V2, null, null),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    elasticInferenceServiceComponents
                ),
                MinimalServiceSettings.sparseEmbedding(NAME)
            )
        );
    }

    @Override
    public void onNodeStarted() {
        authorizationHandler.init();
    }

    /**
     * Only use this in tests.
     *
     * Waits the specified amount of time for the authorization call to complete. This is mainly to make testing easier.
     * @param waitTime the max time to wait
     * @throws IllegalStateException if the wait time is exceeded or the call receives an {@link InterruptedException}
     */
    public void waitForFirstAuthorizationToComplete(TimeValue waitTime) {
        authorizationHandler.waitForAuthorizationToComplete(waitTime);
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return authorizationHandler.supportedStreamingTasks();
    }

    @Override
    public List<DefaultConfigId> defaultConfigIds() {
        return authorizationHandler.defaultConfigIds();
    }

    @Override
    public void defaultConfigs(ActionListener<List<Model>> defaultsListener) {
        authorizationHandler.defaultConfigs(defaultsListener);
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
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (SUPPORTED_INFERENCE_ACTION_TASK_TYPES.contains(model.getTaskType()) == false) {
            var responseString = ServiceUtils.unsupportedTaskTypeForInference(model, SUPPORTED_INFERENCE_ACTION_TASK_TYPES);

            if (model.getTaskType() == TaskType.CHAT_COMPLETION) {
                responseString = responseString + " " + useChatCompletionUrlMessage(model);
            }
            listener.onFailure(new ElasticsearchStatusException(responseString, RestStatus.BAD_REQUEST));
            return;
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
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {}

    @Override
    protected void doChunkedInfer(
        Model model,
        EmbeddingsInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        // Pass-through without actually performing chunking (result will have a single chunk per input)
        ActionListener<InferenceServiceResults> inferListener = listener.delegateFailureAndWrap(
            (delegate, response) -> delegate.onResponse(translateToChunkedResults(inputs, response))
        );

        doInfer(model, inputs, taskSettings, timeout, inferListener);
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
        return authorizationHandler.getConfiguration();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return authorizationHandler.supportedTaskTypes();
    }

    @Override
    public boolean hideFromConfigurationApi() {
        return authorizationHandler.hideFromConfigurationApi();
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
            case CHAT_COMPLETION -> new ElasticInferenceServiceCompletionModel(
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

    private static List<ChunkedInference> translateToChunkedResults(InferenceInputs inputs, InferenceServiceResults inferenceResults) {
        if (inferenceResults instanceof SparseEmbeddingResults sparseEmbeddingResults) {
            var inputsAsList = EmbeddingsInput.of(inputs).getStringInputs();
            return ChunkedInferenceEmbedding.listOf(inputsAsList, sparseEmbeddingResults);
        } else if (inferenceResults instanceof ErrorInferenceResults error) {
            return List.of(new ChunkedInferenceError(error.getException()));
        } else {
            String expectedClass = Strings.format("%s", SparseEmbeddingResults.class.getSimpleName());
            throw createInvalidChunkedResultException(expectedClass, inferenceResults.getWriteableName());
        }
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
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)).setDescription(
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
                    MAX_INPUT_TOKENS,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.SPARSE_EMBEDDING)).setDescription(
                        "Allows you to specify the maximum number of tokens per input."
                    )
                        .setLabel("Maximum Input Tokens")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.putAll(
                    RateLimitSettings.toSettingsConfiguration(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION))
                );

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
