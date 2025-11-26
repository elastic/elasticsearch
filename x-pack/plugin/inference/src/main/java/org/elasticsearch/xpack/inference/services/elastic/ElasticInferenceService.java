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
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elastic.action.ElasticInferenceServiceActionCreator;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.useChatCompletionUrlMessage;
import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.USER_ROLE;

public class ElasticInferenceService extends SenderService {

    public static final String NAME = "elastic";
    public static final String ELASTIC_INFERENCE_SERVICE_IDENTIFIER = "Elastic Inference Service";
    public static final Integer DENSE_TEXT_EMBEDDINGS_DIMENSIONS = 1024;
    // The maximum batch size for sparse text embeddings is set to 16.
    // This value was reduced from 512 due to memory constraints; batch sizes above 32 can cause GPU out-of-memory errors.
    // A batch size of 16 provides optimal throughput and stability, especially on lower-tier instance types.
    public static final Integer SPARSE_TEXT_EMBEDDING_MAX_BATCH_SIZE = 16;

    public static final EnumSet<TaskType> IMPLEMENTED_TASK_TYPES = EnumSet.of(
        TaskType.SPARSE_EMBEDDING,
        TaskType.CHAT_COMPLETION,
        TaskType.COMPLETION,
        TaskType.RERANK,
        TaskType.TEXT_EMBEDDING
    );
    private static final String SERVICE_NAME = "Elastic";

    // TODO: revisit this value once EIS supports dense models
    // The maximum batch size for dense text embeddings is proactively set to 16.
    // This mirrors the memory constraints observed with sparse embeddings
    private static final Integer DENSE_TEXT_EMBEDDINGS_MAX_BATCH_SIZE = 16;

    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(
        TaskType.SPARSE_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.RERANK,
        TaskType.TEXT_EMBEDDING
    );

    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;
    private final CCMAuthenticationApplierFactory ccmAuthenticationApplierFactory;
    private ElasticInferenceServiceActionCreator actionCreator;

    public ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        InferenceServiceExtension.InferenceServiceFactoryContext context,
        CCMAuthenticationApplierFactory ccmAuthApplierFactory
    ) {
        this(factory, serviceComponents, elasticInferenceServiceSettings, context.clusterService(), ccmAuthApplierFactory);
    }

    public ElasticInferenceService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ClusterService clusterService,
        CCMAuthenticationApplierFactory ccmAuthApplierFactory
    ) {
        super(factory, serviceComponents, clusterService);
        this.elasticInferenceServiceComponents = new ElasticInferenceServiceComponents(
            elasticInferenceServiceSettings.getElasticInferenceServiceUrl()
        );
        this.ccmAuthenticationApplierFactory = ccmAuthApplierFactory;
    }

    public void init() {
        // Wait to initialize the action creator until the sender is constructed
        this.actionCreator = new ElasticInferenceServiceActionCreator(getSender(), getServiceComponents(), ccmAuthenticationApplierFactory);
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

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.CHAT_COMPLETION);
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof ElasticInferenceServiceCompletionModel == false
            || (model.getTaskType() != TaskType.CHAT_COMPLETION && model.getTaskType() != TaskType.COMPLETION)) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        // We extract the trace context here as it's sufficient to propagate the trace information of the REST request,
        // which handles the request to the inference API overall (including the outgoing request, which is started in a new thread
        // generating a different "traceparent" as every task and every REST request creates a new span).
        var currentTraceInfo = getCurrentTraceInfo();

        var completionModel = (ElasticInferenceServiceCompletionModel) model;
        var overriddenModel = ElasticInferenceServiceCompletionModel.of(completionModel, inputs.getRequest());

        actionCreator.create(
            overriddenModel,
            currentTraceInfo,
            listener.delegateFailureAndWrap((delegate, action) -> action.execute(inputs, timeout, delegate))
        );
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

        if (model instanceof ElasticInferenceServiceModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        // We extract the trace context here as it's sufficient to propagate the trace information of the REST request,
        // which handles the request to the inference API overall (including the outgoing request, which is started in a new thread
        // generating a different "traceparent" as every task and every REST request creates a new span).
        var currentTraceInfo = getCurrentTraceInfo();

        var elasticInferenceServiceModel = (ElasticInferenceServiceModel) model;

        // For ElasticInferenceServiceCompletionModel, convert ChatCompletionInput to UnifiedChatInput
        // since the request manager expects UnifiedChatInput
        final InferenceInputs finalInputs = (elasticInferenceServiceModel instanceof ElasticInferenceServiceCompletionModel
            && inputs instanceof ChatCompletionInput) ? new UnifiedChatInput((ChatCompletionInput) inputs, USER_ROLE) : inputs;

        actionCreator.create(
            elasticInferenceServiceModel,
            currentTraceInfo,
            listener.delegateFailureAndWrap((delegate, action) -> action.execute(finalInputs, timeout, delegate))
        );
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
            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
                inputs,
                DENSE_TEXT_EMBEDDINGS_MAX_BATCH_SIZE,
                denseTextEmbeddingsModel.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            for (var request : batchedRequests) {
                actionCreator.create(
                    denseTextEmbeddingsModel,
                    getCurrentTraceInfo(),
                    request.listener()
                        .delegateFailureAndWrap(
                            (delegate, action) -> action.execute(
                                new EmbeddingsInput(request.batch().inputs(), inputType),
                                timeout,
                                delegate
                            )
                        )
                );
            }

            return;
        }

        if (model instanceof ElasticInferenceServiceSparseEmbeddingsModel sparseTextEmbeddingsModel) {
            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
                inputs,
                SPARSE_TEXT_EMBEDDING_MAX_BATCH_SIZE,
                model.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            for (var request : batchedRequests) {
                actionCreator.create(
                    sparseTextEmbeddingsModel,
                    getCurrentTraceInfo(),
                    request.listener()
                        .delegateFailureAndWrap(
                            (delegate, action) -> action.execute(
                                new EmbeddingsInput(request.batch().inputs(), inputType),
                                timeout,
                                delegate
                            )
                        )
                );
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
        throw new UnsupportedOperationException(
            "The EIS supported task types change depending on authorization, requests should be made directly to EIS instead"
        );
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
            case CHAT_COMPLETION, COMPLETION -> new ElasticInferenceServiceCompletionModel(
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
