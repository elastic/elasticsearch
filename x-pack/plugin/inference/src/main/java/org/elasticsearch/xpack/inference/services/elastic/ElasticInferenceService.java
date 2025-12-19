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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
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
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.useChatCompletionUrlMessage;

public class ElasticInferenceService extends SenderService {

    public static final String NAME = "elastic";
    public static final String ELASTIC_INFERENCE_SERVICE_IDENTIFIER = "Elastic Inference Service";
    public static final Integer DENSE_TEXT_EMBEDDINGS_DIMENSIONS = 1024;
    // The maximum batch size for sparse text embeddings is set to 16.
    // This value was reduced from 512 due to memory constraints; batch sizes above 32 can cause GPU out-of-memory errors.
    // A batch size of 16 provides optimal throughput and stability, especially on lower-tier instance types.
    public static final Integer SPARSE_TEXT_EMBEDDING_MAX_BATCH_SIZE = 16;

    private static final EnumSet<TaskType> IMPLEMENTED_TASK_TYPES = EnumSet.of(
        TaskType.SPARSE_EMBEDDING,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK,
        TaskType.TEXT_EMBEDDING
    );
    private static final String SERVICE_NAME = "Elastic";

    // TODO: revisit this value once EIS supports dense models
    // The maximum batch size for dense text embeddings is proactively set to 16.
    // This mirrors the memory constraints observed with sparse embeddings
    private static final Integer DENSE_TEXT_EMBEDDINGS_MAX_BATCH_SIZE = 16;

    // rainbow-sprinkles
    static final String DEFAULT_CHAT_COMPLETION_MODEL_ID_V1 = "rainbow-sprinkles";
    static final String DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1 = defaultEndpointId(DEFAULT_CHAT_COMPLETION_MODEL_ID_V1);

    // elser-2
    static final String DEFAULT_ELSER_2_MODEL_ID = "elser_model_2";
    static final String DEFAULT_ELSER_ENDPOINT_ID_V2 = defaultEndpointId("elser-2");

    // multilingual-text-embed
    static final String DEFAULT_MULTILINGUAL_EMBED_MODEL_ID = "jina-embeddings-v3";
    static final String DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID = ".jina-embeddings-v3";

    // rerank-v1
    static final String DEFAULT_RERANK_MODEL_ID_V1 = "elastic-rerank-v1";
    static final String DEFAULT_RERANK_ENDPOINT_ID_V1 = ".elastic-rerank-v1";

    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(
        TaskType.SPARSE_EMBEDDING,
        TaskType.RERANK,
        TaskType.TEXT_EMBEDDING
    );

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
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(
            factory,
            serviceComponents,
            elasticInferenceServiceSettings,
            modelRegistry,
            authorizationRequestHandler,
            context.clusterService()
        );
    }

    public ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ModelRegistry modelRegistry,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        ClusterService clusterService
    ) {
        super(factory, serviceComponents, clusterService);
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
                    new ElasticInferenceServiceCompletionServiceSettings(DEFAULT_CHAT_COMPLETION_MODEL_ID_V1),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    elasticInferenceServiceComponents
                ),
                MinimalServiceSettings.chatCompletion(NAME)
            ),
            DEFAULT_ELSER_2_MODEL_ID,
            new DefaultModelConfig(
                new ElasticInferenceServiceSparseEmbeddingsModel(
                    DEFAULT_ELSER_ENDPOINT_ID_V2,
                    TaskType.SPARSE_EMBEDDING,
                    NAME,
                    new ElasticInferenceServiceSparseEmbeddingsServiceSettings(DEFAULT_ELSER_2_MODEL_ID, null),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    elasticInferenceServiceComponents,
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                MinimalServiceSettings.sparseEmbedding(NAME)
            ),
            DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
            new DefaultModelConfig(
                new ElasticInferenceServiceDenseTextEmbeddingsModel(
                    DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                    TaskType.TEXT_EMBEDDING,
                    NAME,
                    new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
                        DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
                        defaultDenseTextEmbeddingsSimilarity(),
                        DENSE_TEXT_EMBEDDINGS_DIMENSIONS,
                        null
                    ),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    elasticInferenceServiceComponents,
                    ChunkingSettingsBuilder.DEFAULT_SETTINGS
                ),
                MinimalServiceSettings.textEmbedding(
                    NAME,
                    DENSE_TEXT_EMBEDDINGS_DIMENSIONS,
                    defaultDenseTextEmbeddingsSimilarity(),
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            ),
            DEFAULT_RERANK_MODEL_ID_V1,
            new DefaultModelConfig(
                new ElasticInferenceServiceRerankModel(
                    DEFAULT_RERANK_ENDPOINT_ID_V1,
                    TaskType.RERANK,
                    NAME,
                    new ElasticInferenceServiceRerankServiceSettings(DEFAULT_RERANK_MODEL_ID_V1),
                    EmptyTaskSettings.INSTANCE,
                    EmptySecretSettings.INSTANCE,
                    elasticInferenceServiceComponents
                ),
                MinimalServiceSettings.rerank(NAME)
            )
        );
    }

    @Override
    public void onNodeStarted() {
        authorizationHandler.init();
    }

    @Override
    protected void validateRerankParameters(Boolean returnDocuments, Integer topN, ValidationException validationException) {
        if (returnDocuments != null) {
            validationException.addValidationError(
                org.elasticsearch.core.Strings.format(
                    "Invalid return_documents [%s]. The return_documents option is not supported by this service",
                    returnDocuments
                )
            );
        }
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
        return EnumSet.of(TaskType.CHAT_COMPLETION);
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
        List<ChunkInferenceInput> inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        if (model instanceof ElasticInferenceServiceDenseTextEmbeddingsModel denseTextEmbeddingsModel) {
            var actionCreator = new ElasticInferenceServiceActionCreator(getSender(), getServiceComponents(), getCurrentTraceInfo());

            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
                inputs,
                DENSE_TEXT_EMBEDDINGS_MAX_BATCH_SIZE,
                denseTextEmbeddingsModel.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            for (var request : batchedRequests) {
                var action = denseTextEmbeddingsModel.accept(actionCreator, taskSettings);
                action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
            }

            return;
        }

        if (model instanceof ElasticInferenceServiceSparseEmbeddingsModel sparseTextEmbeddingsModel) {
            var actionCreator = new ElasticInferenceServiceActionCreator(getSender(), getServiceComponents(), getCurrentTraceInfo());

            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
                inputs,
                SPARSE_TEXT_EMBEDDING_MAX_BATCH_SIZE,
                model.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            for (var request : batchedRequests) {
                var action = sparseTextEmbeddingsModel.accept(actionCreator, taskSettings);
                action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
            }

            return;
        }

        // Model cannot perform chunked inference
        listener.onFailure(createInvalidModelException(model));
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
            if (TaskType.SPARSE_EMBEDDING.equals(taskType) || TaskType.TEXT_EMBEDDING.equals(taskType)) {
                chunkingSettings = ChunkingSettingsBuilder.fromMap(
                    removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
                );
            }

            ElasticInferenceServiceModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                chunkingSettings,
                serviceSettingsMap,
                elasticInferenceServiceComponents,
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

    /**
     * This shouldn't be called because the configuration changes based on the authorization.
     * Instead, retrieve the authorization directly from the EIS gateway and use the static method
     * {@link ElasticInferenceService#createConfiguration(EnumSet)} to create a configuration based on the authorization response.
     */
    @Override
    public InferenceServiceConfiguration getConfiguration() {
        throw new UnsupportedOperationException(
            "The EIS configuration changes depending on authorization, requests should be made directly to EIS instead"
        );
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return authorizationHandler.supportedTaskTypes();
    }

    @Override
    public boolean hideFromConfigurationApi() {
        // This shouldn't be called because the configuration changes based on the authorization
        // Instead, retrieve the authorization directly from the EIS gateway and use the response to determine if EIS is authorized
        throw new UnsupportedOperationException(
            "The EIS configuration changes depending on authorization, requests should be made directly to EIS instead"
        );
    }

    private static ElasticInferenceServiceModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
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
                elasticInferenceServiceComponents,
                context,
                chunkingSettings
            );
            case CHAT_COMPLETION -> new ElasticInferenceServiceCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                elasticInferenceServiceComponents,
                context
            );
            case RERANK -> new ElasticInferenceServiceRerankModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                elasticInferenceServiceComponents,
                context
            );
            case TEXT_EMBEDDING -> new ElasticInferenceServiceDenseTextEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                elasticInferenceServiceComponents,
                context,
                chunkingSettings
            );
            default -> throw createInvalidTaskTypeException(inferenceEntityId, NAME, taskType, context);
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

        ChunkingSettings chunkingSettings = null;
        if (TaskType.SPARSE_EMBEDDING.equals(taskType) || TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
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
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.SPARSE_EMBEDDING.equals(taskType) || TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(inferenceEntityId, taskType, serviceSettingsMap, taskSettingsMap, chunkingSettings, null);
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
            elasticInferenceServiceComponents,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof ElasticInferenceServiceDenseTextEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var modelId = serviceSettings.modelId();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? defaultDenseTextEmbeddingsSimilarity() : similarityFromModel;
            var maxInputTokens = serviceSettings.maxInputTokens();

            var updateServiceSettings = new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
                modelId,
                similarityToUse,
                embeddingSize,
                maxInputTokens
            );

            return new ElasticInferenceServiceDenseTextEmbeddingsModel(embeddingsModel, updateServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    public static SimilarityMeasure defaultDenseTextEmbeddingsSimilarity() {
        // TODO: double-check
        return SimilarityMeasure.COSINE;
    }

    private static List<ChunkedInference> translateToChunkedResults(InferenceInputs inputs, InferenceServiceResults inferenceResults) {
        if (inferenceResults instanceof SparseEmbeddingResults sparseEmbeddingResults) {
            var inputsAsList = inputs.castTo(EmbeddingsInput.class).getInputs();
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

        var traceParent = threadPool.getThreadContext().getHeader(Task.TRACE_PARENT_HTTP_HEADER);
        var traceState = threadPool.getThreadContext().getHeader(Task.TRACE_STATE);

        return new TraceContext(traceParent, traceState);
    }

    public static InferenceServiceConfiguration createConfiguration(EnumSet<TaskType> enabledTaskTypes) {
        var configurationMap = new HashMap<String, SettingsConfiguration>();

        configurationMap.put(
            MODEL_ID,
            new SettingsConfiguration.Builder(
                EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION, TaskType.RERANK, TaskType.TEXT_EMBEDDING)
            ).setDescription("The name of the model to use for the inference task.")
                .setLabel("Model ID")
                .setRequired(true)
                .setSensitive(false)
                .setUpdatable(false)
                .setType(SettingsConfigurationFieldType.STRING)
                .build()
        );

        configurationMap.put(
            MAX_INPUT_TOKENS,
            new SettingsConfiguration.Builder(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING)).setDescription(
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
            .setTaskTypes(enabledTaskTypes)
            .setConfigurations(configurationMap)
            .build();
    }
}
