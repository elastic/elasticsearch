/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmbeddingRequest;
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
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
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
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModelCreator;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUnsupportedTaskTypeStatusException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.USER_ROLE;

public class ElasticInferenceService extends SenderService {

    public static final String NAME = "elastic";
    public static final String ELASTIC_INFERENCE_SERVICE_IDENTIFIER = "Elastic Inference Service";

    // The default maximum batch size for sparse text embeddings is set to 16.
    // This value was reduced from 512 due to memory constraints; batch sizes above 32 can cause GPU out-of-memory errors.
    // A batch size of 16 provides optimal throughput and stability, especially on lower-tier instance types.
    public static final Integer DEFAULT_SPARSE_TEXT_EMBEDDING_MAX_BATCH_SIZE = 16;

    public static final EnumSet<TaskType> IMPLEMENTED_TASK_TYPES = EnumSet.of(
        SPARSE_EMBEDDING,
        CHAT_COMPLETION,
        COMPLETION,
        RERANK,
        TEXT_EMBEDDING,
        EMBEDDING
    );
    private static final String SERVICE_NAME = "Elastic";

    // TODO: revisit this value once EIS supports dense models
    // The default maximum batch size for dense text embeddings is proactively set to 16.
    // This mirrors the memory constraints observed with sparse embeddings
    private static final Integer DEFAULT_DENSE_TEXT_EMBEDDINGS_MAX_BATCH_SIZE = 16;

    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(
        SPARSE_EMBEDDING,
        COMPLETION,
        RERANK,
        TEXT_EMBEDDING
    );

    /**
     * The task types that support chunking settings
     */
    private static final EnumSet<TaskType> CHUNKING_TASK_TYPES = EnumSet.of(SPARSE_EMBEDDING, TEXT_EMBEDDING, EMBEDDING);

    private final Map<TaskType, ElasticInferenceServiceModelCreator<? extends ElasticInferenceServiceModel>> modelCreators;
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
        this.ccmAuthenticationApplierFactory = ccmAuthApplierFactory;
        var elasticInferenceServiceComponents = new ElasticInferenceServiceComponents(
            elasticInferenceServiceSettings.getElasticInferenceServiceUrl()
        );
        var denseEmbeddingsModelCreator = new ElasticInferenceServiceDenseEmbeddingsModelCreator(elasticInferenceServiceComponents);
        var completionModelCreator = new ElasticInferenceServiceCompletionModelCreator(elasticInferenceServiceComponents);
        this.modelCreators = Map.of(
            TaskType.TEXT_EMBEDDING,
            denseEmbeddingsModelCreator,
            TaskType.EMBEDDING,
            denseEmbeddingsModelCreator,
            TaskType.SPARSE_EMBEDDING,
            new ElasticInferenceServiceSparseEmbeddingsModelCreator(elasticInferenceServiceComponents),
            TaskType.COMPLETION,
            completionModelCreator,
            TaskType.CHAT_COMPLETION,
            completionModelCreator,
            TaskType.RERANK,
            new ElasticInferenceServiceRerankModelCreator(elasticInferenceServiceComponents)
        );
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
        return EnumSet.of(CHAT_COMPLETION);
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof ElasticInferenceServiceCompletionModel == false
            || (model.getTaskType() != CHAT_COMPLETION && model.getTaskType() != COMPLETION)) {
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
            listener.onFailure(createUnsupportedTaskTypeStatusException(model, SUPPORTED_INFERENCE_ACTION_TASK_TYPES));
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
    protected void doEmbeddingInfer(
        Model model,
        EmbeddingRequest request,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model.getTaskType() == EMBEDDING) {
            actionCreator.create(
                (ElasticInferenceServiceDenseEmbeddingsModel) model,
                getCurrentTraceInfo(),
                listener.delegateFailureAndWrap(
                    (delegate, action) -> action.execute(new EmbeddingsInput(request::inputs, request.inputType()), timeout, delegate)
                )
            );
        } else {
            listener.onFailure(createUnsupportedTaskTypeStatusException(model, EnumSet.of(EMBEDDING)));
        }
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
        EmbeddingRequestChunker<?> embeddingRequestChunker = createEmbeddingRequestChunker(model, inputs);
        if (embeddingRequestChunker == null) {
            // Model cannot perform chunked inference
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var batchedRequests = embeddingRequestChunker.batchRequestsWithListeners(listener);
        for (var request : batchedRequests) {
            actionCreator.create(
                (ElasticInferenceServiceModel) model,
                getCurrentTraceInfo(),
                request.listener()
                    .delegateFailureAndWrap(
                        (delegate, action) -> action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, delegate)
                    )
            );
        }
    }

    EmbeddingRequestChunker<?> createEmbeddingRequestChunker(Model model, List<ChunkInferenceInput> inputs) {
        return switch (model) {
            case ElasticInferenceServiceDenseEmbeddingsModel denseModel -> new EmbeddingRequestChunker<>(
                inputs,
                DEFAULT_DENSE_TEXT_EMBEDDINGS_MAX_BATCH_SIZE,
                denseModel.getConfigurations().getChunkingSettings()
            );
            case ElasticInferenceServiceSparseEmbeddingsModel sparseModel -> new EmbeddingRequestChunker<>(
                inputs,
                Optional.ofNullable(sparseModel.getServiceSettings().maxBatchSize()).orElse(DEFAULT_SPARSE_TEXT_EMBEDDING_MAX_BATCH_SIZE),
                sparseModel.getConfigurations().getChunkingSettings()
            );
            default -> null;
        };
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
            if (CHUNKING_TASK_TYPES.contains(taskType)) {
                chunkingSettings = ChunkingSettingsBuilder.fromMap(
                    removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
                );
            }

            ElasticInferenceServiceModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                chunkingSettings,
                ConfigurationParseContext.REQUEST,
                null
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

    private ElasticInferenceServiceModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ChunkingSettings chunkingSettings,
        ConfigurationParseContext context,
        @Nullable EndpointMetadata endpointMetadata
    ) {

        return retrieveModelCreatorFromMapOrThrow(modelCreators, inferenceEntityId, taskType, NAME, context).createFromMaps(
            inferenceEntityId,
            taskType,
            serviceSettings,
            chunkingSettings,
            context,
            endpointMetadata
        );
    }

    @Override
    public Model parsePersistedConfigWithSecrets(UnparsedModel unparsedModel) {
        var config = unparsedModel.settings();
        var secrets = unparsedModel.secrets();
        var taskType = unparsedModel.taskType();

        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        // These aren't used by EIS endpoints so we'll remove them to avoid potential validation issues
        removeFromMap(config, ModelConfigurations.TASK_SETTINGS);
        removeFromMap(secrets, ModelSecrets.SECRET_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (CHUNKING_TASK_TYPES.contains(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(
            unparsedModel.inferenceEntityId(),
            taskType,
            serviceSettingsMap,
            chunkingSettings,
            unparsedModel.endpointMetadata()
        );
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        // Once the inference api logic is switched to using the UnparsedModel variants of methods, this method can simply throw
        // an exception. Then once all services use the UnparsedModel we can remove this method entirely.
        return parsePersistedConfigWithSecrets(new UnparsedModel(inferenceEntityId, taskType, NAME, config, secrets));
    }

    @Override
    public ElasticInferenceServiceModel buildModelFromConfigAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return retrieveModelCreatorFromMapOrThrow(
            modelCreators,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.PERSISTENT
        ).createFromModelConfigurationsAndSecrets(config, secrets);
    }

    @Override
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        // Once the inference api logic is switched to using the UnparsedModel variants of methods, this method can simply throw
        // an exception. Then once all services use the UnparsedModel we can remove this method entirely.
        return parsePersistedConfigWithSecrets(inferenceEntityId, taskType, config, new HashMap<>());
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    private ElasticInferenceServiceModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ChunkingSettings chunkingSettings,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            chunkingSettings,
            ConfigurationParseContext.PERSISTENT,
            endpointMetadata
        );
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof ElasticInferenceServiceDenseEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var modelId = serviceSettings.modelId();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? defaultDenseEmbeddingsSimilarity() : similarityFromModel;
            var maxInputTokens = serviceSettings.maxInputTokens();

            var updateServiceSettings = new ElasticInferenceServiceDenseEmbeddingsServiceSettings(
                modelId,
                similarityToUse,
                embeddingSize,
                maxInputTokens
            );

            return new ElasticInferenceServiceDenseEmbeddingsModel(embeddingsModel, updateServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    public static SimilarityMeasure defaultDenseEmbeddingsSimilarity() {
        // TODO: double-check
        return SimilarityMeasure.COSINE;
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
            new SettingsConfiguration.Builder(EnumSet.of(SPARSE_EMBEDDING, CHAT_COMPLETION, RERANK, TEXT_EMBEDDING, EMBEDDING))
                .setDescription("The name of the model to use for the inference task.")
                .setLabel("Model ID")
                .setRequired(true)
                .setSensitive(false)
                .setUpdatable(false)
                .setType(SettingsConfigurationFieldType.STRING)
                .build()
        );

        configurationMap.put(
            MAX_INPUT_TOKENS,
            new SettingsConfiguration.Builder(EnumSet.of(SPARSE_EMBEDDING, TEXT_EMBEDDING, EMBEDDING)).setDescription(
                "Allows you to specify the maximum number of tokens per input."
            )
                .setLabel("Maximum Input Tokens")
                .setRequired(false)
                .setSensitive(false)
                .setUpdatable(false)
                .setType(SettingsConfigurationFieldType.INTEGER)
                .build()
        );

        configurationMap.put(
            ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE,
            new SettingsConfiguration.Builder(EnumSet.of(TaskType.SPARSE_EMBEDDING)).setDescription(
                "Allows you to specify the maximum number of chunks per batch."
            )
                .setLabel("Maximum Batch Size")
                .setRequired(false)
                .setSensitive(false)
                .setUpdatable(true)
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
