/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModelCreator;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs.fromRerankRequest;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;

/**
 * Inference service integration for the TencentCloud AI Gateway (OpenAI-compatible), supporting {@code text_embedding},
 * {@code completion}, {@code chat_completion}, and {@code rerank} task types.
 */
public class TencentCloudService extends SenderService<TencentCloudModel> implements RerankingInferenceService {

    public static final String NAME = "tencentcloud";
    private static final String SERVICE_NAME = "TencentCloud AI Gateway";

    public static final TransportVersion TENCENT_CLOUD_INFERENCE_SERVICE_ADDED = TransportVersion.fromName(
        "ml_inference_tencentcloud_added"
    );

    // Batch limit for embedding chunking. TencentCloud AI Gateway does not document a hard cap; use a conservative value.
    private static final int EMBEDDING_MAX_BATCH_SIZE = 32;

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK
    );
    private static final EnumSet<TaskType> SUPPORTED_STREAMING_TASKS = EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);

    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new OpenAiUnifiedChatCompletionResponseHandler(
        "tencentcloud chat completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private static final Map<TaskType, ModelCreator<? extends TencentCloudModel>> MODEL_CREATORS = initModelCreators();

    private static Map<TaskType, ModelCreator<? extends TencentCloudModel>> initModelCreators() {
        var completionCreator = new TencentCloudChatCompletionModelCreator();
        return Map.of(
            TaskType.TEXT_EMBEDDING,
            new TencentCloudEmbeddingsModelCreator(),
            TaskType.COMPLETION,
            completionCreator,
            TaskType.CHAT_COMPLETION,
            completionCreator,
            TaskType.RERANK,
            new TencentCloudRerankModelCreator()
        );
    }

    /**
     * Constructor for creating a TencentCloudService with the specified HTTP request sender factory, service components,
     * and factory context.
     *
     * @param factory the factory to create HTTP request senders
     * @param serviceComponents the components required for the inference service
     * @param context the context for the inference service factory
     */
    public TencentCloudService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    /**
     * Constructor for creating a TencentCloudService with the specified HTTP request sender factory, service components,
     * and cluster service.
     *
     * @param factory the factory to create HTTP request senders
     * @param serviceComponents the components required for the inference service
     * @param clusterService the cluster service used to resolve cluster state
     */
    public TencentCloudService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService, MODEL_CREATORS);
    }

    @Override
    public String name() {
        return NAME;
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
    public Set<TaskType> supportedStreamingTasks() {
        return SUPPORTED_STREAMING_TASKS;
    }

    /**
     * Performs inference for a TencentCloud model (embeddings or completion) by creating an executable action via the
     * {@link TencentCloudActionCreator} and executing it.
     *
     * @param model the model to run inference against
     * @param inputs the inference inputs
     * @param taskSettings the task-level settings for this request
     * @param timeout the request timeout
     * @param listener the listener to notify with results or failure
     */
    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof TencentCloudModel tencentCloudModel) {
            var actionCreator = new TencentCloudActionCreator(getSender(), getServiceComponents());
            var action = tencentCloudModel.accept(actionCreator, taskSettings);
            action.execute(inputs, timeout, listener);
            return;
        }

        listener.onFailure(createInvalidModelException(model));
    }

    /**
     * Performs unified chat completion inference for a TencentCloud chat completion model.
     *
     * @param model the model to run inference against
     * @param inputs the unified chat input
     * @param timeout the request timeout
     * @param listener the listener to notify with results or failure
     */
    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof TencentCloudChatCompletionModel chatModel) {
            var requestManager = new GenericRequestManager<>(
                getServiceComponents().threadPool(),
                chatModel,
                UNIFIED_CHAT_COMPLETION_HANDLER,
                (unifiedChatInput) -> new TencentCloudChatCompletionRequest(unifiedChatInput, chatModel),
                UnifiedChatInput.class
            );
            var errorMessage = constructFailedToSendRequestMessage("TencentCloud chat completions");
            var action = new SenderExecutableAction(getSender(), requestManager, errorMessage);
            action.execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    /**
     * Performs rerank inference for a TencentCloud rerank model by creating an executable action via the
     * {@link TencentCloudActionCreator} and executing it.
     *
     * @param model the model to run inference against
     * @param request the rerank request containing the query, documents, and task settings
     * @param timeout the request timeout
     * @param listener the listener to notify with results or failure
     */
    @Override
    protected void doRerankInfer(Model model, RerankRequest request, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (model instanceof TencentCloudRerankModel rerankModel) {
            var actionCreator = new TencentCloudActionCreator(getSender(), getServiceComponents());
            var action = rerankModel.accept(actionCreator, request.taskSettings());
            action.execute(fromRerankRequest(request), timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    /**
     * Performs chunked inference for a TencentCloud embeddings model, batching the inputs and executing one
     * executable action per batch.
     *
     * @param model the model to run inference against
     * @param inputs the chunked inference inputs
     * @param taskSettings the task-level settings for this request
     * @param inputType the input type for embeddings
     * @param timeout the request timeout
     * @param listener the listener to notify with results or failure
     */
    @Override
    protected void doChunkedInfer(
        Model model,
        List<ChunkInferenceInput> inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        if (model instanceof TencentCloudEmbeddingsModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var embeddingsModel = (TencentCloudEmbeddingsModel) model;
        var actionCreator = new TencentCloudActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            embeddingsModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = embeddingsModel.accept(actionCreator, taskSettings);
            action.execute(
                new EmbeddingsInput(request.batch().inputs(), request.batch().ramBytesUsed(), inputType),
                timeout,
                request.listener()
            );
        }
    }

    /**
     * Validates that the input type is permitted for TencentCloud models.
     *
     * @param inputType the input type to validate
     * @param model the model the inference is run against
     * @param validationException the exception to collect validation errors into
     */
    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    /**
     * Updates an embeddings model with the embedding size (and derived similarity) returned by the service, returning a
     * new model when the service settings changed.
     *
     * @param model the model to update
     * @param embeddingSize the embedding size reported by the service
     * @return a new model with updated service settings, or the original model when no change is needed
     */
    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof TencentCloudEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarity = serviceSettings.similarity() != null ? serviceSettings.similarity() : SimilarityMeasure.DOT_PRODUCT;

            var updatedServiceSettings = serviceSettings.updateEmbeddingDetails(embeddingSize, similarity);
            if (updatedServiceSettings.equals(serviceSettings)) {
                return model;
            }

            return new TencentCloudEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        }
        throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TENCENT_CLOUD_INFERENCE_SERVICE_ADDED;
    }

    @Override
    public boolean usesParserForServiceSettings() {
        return true;
    }

    /**
     * Returns the conservative static rerank context window (in tokens) used for all TencentCloud rerank models, since
     * the gateway does not publish a fixed rerank context window or validate the model id at configuration time.
     *
     * @param modelId the rerank model identifier
     * @return the reranker window size in tokens
     */
    @Override
    public int rerankerWindowSize(String modelId) {
        // The TencentCloud AI Gateway does not publish a fixed rerank context window and does not validate the model id
        // at configuration time, so a single conservative static window is used for all rerank models. 350 tokens leaves
        // comfortable headroom for the gateway's reranker models, which accept several hundred tokens of context.
        return 350;
    }

    /**
     * Configuration class for the TencentCloud inference service.
     * It provides the settings and configurations required for the service.
     */
    public static class Configuration {
        /**
         * Returns the lazily computed {@link InferenceServiceConfiguration} for the TencentCloud service.
         *
         * @return the inference service configuration
         */
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The name of the model to use for the inference task, e.g. bge-m3 (embeddings),"
                            + " deepseek-v3 (chat/completions), bge-reranker-v2-m3 (rerank)."
                            + " The gateway supports additional models; check the TencentCloud AI Gateway documentation for the full list."
                    )
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    "region",
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDefaultValue("bj")
                        .setDescription(
                            "The TencentCloud AI Gateway region, e.g. bj, sh, gz. "
                                + "The endpoint URL is constructed as https://{region}.aisearch.tencentelasticsearch.com/v1/<task-path>."
                        )
                        .setLabel("Region")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(
                    DefaultSecretSettings.toSettingsConfigurationWithDescription(
                        "The TencentCloud AI Gateway API key. Contact the administrator to obtain a token in the format sk-<your-api-key>.",
                        SUPPORTED_TASK_TYPES
                    )
                );
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
