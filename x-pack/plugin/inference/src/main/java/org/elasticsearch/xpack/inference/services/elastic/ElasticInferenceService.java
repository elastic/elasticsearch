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
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingSparse;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.elastic.ElasticInferenceServiceActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.ElasticInferenceServiceUnifiedCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorization;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationHandler;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
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

    private static String defaultEndpointId(String modelId) {
        return Strings.format(".%s-elastic", modelId);
    }

    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;
    private Configuration configuration;
    private final AtomicReference<AuthorizedContent> authRef = new AtomicReference<>(AuthorizedContent.empty());
    private final ModelRegistry modelRegistry;
    private final ElasticInferenceServiceAuthorizationHandler authorizationHandler;
    private final CountDownLatch authorizationCompletedLatch = new CountDownLatch(1);
    // model ids to model information, used for the default config methods to return the list of models and default
    // configs
    private final Map<String, DefaultModelConfig> defaultModelsConfigs;

    public ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ModelRegistry modelRegistry,
        ElasticInferenceServiceAuthorizationHandler authorizationHandler
    ) {
        super(factory, serviceComponents);
        this.elasticInferenceServiceComponents = Objects.requireNonNull(elasticInferenceServiceComponents);
        this.modelRegistry = Objects.requireNonNull(modelRegistry);
        this.authorizationHandler = Objects.requireNonNull(authorizationHandler);

        configuration = new Configuration(authRef.get().taskTypesAndModels.getAuthorizedTaskTypes());
        defaultModelsConfigs = initDefaultEndpoints(elasticInferenceServiceComponents);

        getAuthorization();
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
                MinimalServiceSettings.chatCompletion()
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
                MinimalServiceSettings.sparseEmbedding()
            )
        );
    }

    private record DefaultModelConfig(Model model, MinimalServiceSettings settings) {}

    private record AuthorizedContent(
        ElasticInferenceServiceAuthorization taskTypesAndModels,
        List<DefaultConfigId> configIds,
        List<DefaultModelConfig> defaultModelConfigs
    ) {
        static AuthorizedContent empty() {
            return new AuthorizedContent(ElasticInferenceServiceAuthorization.newDisabledService(), List.of(), List.of());
        }
    }

    private void getAuthorization() {
        try {
            ActionListener<ElasticInferenceServiceAuthorization> listener = ActionListener.wrap(this::setAuthorizedContent, e -> {
                // we don't need to do anything if there was a failure, everything is disabled by default
                authorizationCompletedLatch.countDown();
            });

            authorizationHandler.getAuthorization(listener, getSender());
        } catch (Exception e) {
            // we don't need to do anything if there was a failure, everything is disabled by default
            authorizationCompletedLatch.countDown();
        }
    }

    private synchronized void setAuthorizedContent(ElasticInferenceServiceAuthorization auth) {
        var authorizedTaskTypesAndModels = auth.newLimitedToTaskTypes(EnumSet.copyOf(IMPLEMENTED_TASK_TYPES));

        // recalculate which default config ids and models are authorized now
        var authorizedDefaultModelIds = getAuthorizedDefaultModelIds(auth);

        var authorizedDefaultConfigIds = getAuthorizedDefaultConfigIds(authorizedDefaultModelIds, auth);
        var authorizedDefaultModelObjects = getAuthorizedDefaultModelsObjects(authorizedDefaultModelIds);
        authRef.set(new AuthorizedContent(authorizedTaskTypesAndModels, authorizedDefaultConfigIds, authorizedDefaultModelObjects));

        configuration = new Configuration(authRef.get().taskTypesAndModels.getAuthorizedTaskTypes());

        defaultConfigIds().forEach(modelRegistry::putDefaultIdIfAbsent);
        handleRevokedDefaultConfigs(authorizedDefaultModelIds);
    }

    private Set<String> getAuthorizedDefaultModelIds(ElasticInferenceServiceAuthorization auth) {
        var authorizedModels = auth.getAuthorizedModelIds();
        var authorizedDefaultModelIds = new TreeSet<>(defaultModelsConfigs.keySet());
        authorizedDefaultModelIds.retainAll(authorizedModels);

        return authorizedDefaultModelIds;
    }

    private List<DefaultConfigId> getAuthorizedDefaultConfigIds(
        Set<String> authorizedDefaultModelIds,
        ElasticInferenceServiceAuthorization auth
    ) {
        var authorizedConfigIds = new ArrayList<DefaultConfigId>();
        for (var id : authorizedDefaultModelIds) {
            var modelConfig = defaultModelsConfigs.get(id);
            if (modelConfig != null) {
                if (auth.getAuthorizedTaskTypes().contains(modelConfig.model.getTaskType()) == false) {
                    logger.warn(
                        Strings.format(
                            "The authorization response included the default model: %s, "
                                + "but did not authorize the assumed task type of the model: %s. Enabling model.",
                            id,
                            modelConfig.model.getTaskType()
                        )
                    );
                }
                authorizedConfigIds.add(new DefaultConfigId(modelConfig.model.getInferenceEntityId(), modelConfig.settings(), this));
            }
        }

        authorizedConfigIds.sort(Comparator.comparing(DefaultConfigId::inferenceId));
        return authorizedConfigIds;
    }

    private List<DefaultModelConfig> getAuthorizedDefaultModelsObjects(Set<String> authorizedDefaultModelIds) {
        var authorizedModels = new ArrayList<DefaultModelConfig>();
        for (var id : authorizedDefaultModelIds) {
            var modelConfig = defaultModelsConfigs.get(id);
            if (modelConfig != null) {
                authorizedModels.add(modelConfig);
            }
        }

        authorizedModels.sort(Comparator.comparing(modelConfig -> modelConfig.model.getInferenceEntityId()));
        return authorizedModels;
    }

    private void handleRevokedDefaultConfigs(Set<String> authorizedDefaultModelIds) {
        // if a model was initially returned in the authorization response but is absent, then we'll assume authorization was revoked
        var unauthorizedDefaultModelIds = new HashSet<>(defaultModelsConfigs.keySet());
        unauthorizedDefaultModelIds.removeAll(authorizedDefaultModelIds);

        // get all the default inference endpoint ids for the unauthorized model ids
        var unauthorizedDefaultInferenceEndpointIds = unauthorizedDefaultModelIds.stream()
            .map(defaultModelsConfigs::get) // get all the model configs
            .filter(Objects::nonNull) // limit to only non-null
            .map(modelConfig -> modelConfig.model.getInferenceEntityId()) // get the inference ids
            .collect(Collectors.toSet());

        var deleteInferenceEndpointsListener = ActionListener.<Boolean>wrap(result -> {
            logger.trace(Strings.format("Successfully revoked access to default inference endpoint IDs: %s", unauthorizedDefaultModelIds));
            authorizationCompletedLatch.countDown();
        }, e -> {
            logger.warn(
                Strings.format("Failed to revoke access to default inference endpoint IDs: %s, error: %s", unauthorizedDefaultModelIds, e)
            );
            authorizationCompletedLatch.countDown();
        });

        getServiceComponents().threadPool()
            .executor(UTILITY_THREAD_POOL_NAME)
            .execute(() -> modelRegistry.removeDefaultConfigs(unauthorizedDefaultInferenceEndpointIds, deleteInferenceEndpointsListener));
    }

    /**
     * Waits the specified amount of time for the authorization call to complete. This is mainly to make testing easier.
     * @param waitTime the max time to wait
     * @throws IllegalStateException if the wait time is exceeded or the call receives an {@link InterruptedException}
     */
    public void waitForAuthorizationToComplete(TimeValue waitTime) {
        try {
            if (authorizationCompletedLatch.await(waitTime.getSeconds(), TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("The wait time has expired for authorization to complete.");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Waiting for authorization to complete was interrupted");
        }
    }

    @Override
    public synchronized Set<TaskType> supportedStreamingTasks() {
        var authorizedStreamingTaskTypes = EnumSet.of(TaskType.CHAT_COMPLETION);
        authorizedStreamingTaskTypes.retainAll(authRef.get().taskTypesAndModels.getAuthorizedTaskTypes());

        return authorizedStreamingTaskTypes;
    }

    @Override
    public synchronized List<DefaultConfigId> defaultConfigIds() {
        return authRef.get().configIds;
    }

    @Override
    public synchronized void defaultConfigs(ActionListener<List<Model>> defaultsListener) {
        var models = authRef.get().defaultModelConfigs.stream().map(config -> config.model).toList();
        defaultsListener.onResponse(models);
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
        var actionCreator = new ElasticInferenceServiceActionCreator(getSender(), getServiceComponents(), currentTraceInfo, inputType);

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
        return authRef.get().taskTypesAndModels.getAuthorizedTaskTypes();
    }

    @Override
    public synchronized boolean hideFromConfigurationApi() {
        return authRef.get().taskTypesAndModels.isAuthorized() == false;
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
