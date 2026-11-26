/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
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
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudMultimodalModelLimits;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModelCreator;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs.fromRerankRequest;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUnsupportedTaskTypeStatusException;

/**
 * Inference service integration for the TencentCloud AI Gateway (OpenAI-compatible), supporting {@code text_embedding},
 * {@code embedding}, {@code completion}, {@code chat_completion}, and {@code rerank} task types. The {@code embedding} task calls the
 * multimodal embeddings API and accepts text, image and video inputs.
 */
public class TencentCloudService extends SenderService<TencentCloudModel> implements RerankingInferenceService {

    public static final String NAME = "tencentcloud";
    private static final String SERVICE_NAME = "TencentCloud AI Gateway";

    public static final TransportVersion TENCENT_CLOUD_INFERENCE_SERVICE_ADDED = TransportVersion.fromName("inference_tencentcloud_added");

    private static final List<String> SUPPORTED_BASE64_IMAGE_PREFIXES = List.of(
        "data:image/png;base64,",
        "data:image/jpeg;base64,",
        "data:image/gif;base64,",
        "data:image/webp;base64,"
    );
    private static final String SUPPORTED_BASE64_VIDEO_PREFIX = "data:video/mp4;base64,";

    public static final TransportVersion TENCENT_CLOUD_EMBEDDING_TASK_ADDED = TransportVersion.fromName(
        "tencentcloud_embedding_task_added"
    );

    // Batch limit for embedding chunking. TencentCloud AI Gateway does not document a hard cap; use a conservative value.
    private static final int EMBEDDING_MAX_BATCH_SIZE = 32;

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK
    );
    private static final EnumSet<TaskType> SUPPORTED_STREAMING_TASKS = EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);

    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION);

    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new OpenAiUnifiedChatCompletionResponseHandler(
        "tencentcloud chat completion",
        OpenAiCompletionResponseEntity::fromResponse
    );

    private static final Map<TaskType, ModelCreator<? extends TencentCloudModel>> MODEL_CREATORS = initModelCreators();

    private final ClusterService clusterService;

    private static Map<TaskType, ModelCreator<? extends TencentCloudModel>> initModelCreators() {
        var embeddingsCreator = new TencentCloudEmbeddingsModelCreator();
        var completionCreator = new TencentCloudChatCompletionModelCreator();
        return Map.of(
            TaskType.TEXT_EMBEDDING,
            embeddingsCreator,
            TaskType.EMBEDDING,
            embeddingsCreator,
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
        this.clusterService = clusterService;
    }

    /**
     * Parses the configuration of a new endpoint. An {@code embedding} endpoint is only created once every node in the cluster can
     * read its settings.
     */
    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        if (taskType == TaskType.EMBEDDING
            && clusterService.state().getMinTransportVersion().supports(TENCENT_CLOUD_EMBEDDING_TASK_ADDED) == false) {
            parsedModelListener.onFailure(
                new ElasticsearchStatusException(
                    Strings.format(
                        "All nodes in the cluster are not aware of the [%s] task type of the [%s] service. "
                            + "Wait for the cluster to finish upgrading and try again.",
                        TaskType.EMBEDDING,
                        NAME
                    ),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }
        super.parseRequestConfig(inferenceEntityId, taskType, config, parsedModelListener);
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
        if (SUPPORTED_INFERENCE_ACTION_TASK_TYPES.contains(model.getTaskType()) == false) {
            listener.onFailure(createUnsupportedTaskTypeStatusException(model, SUPPORTED_INFERENCE_ACTION_TASK_TYPES));
            return;
        }

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
     * Performs multimodal embedding inference for a TencentCloud {@code embedding} model. The inputs are split into batches that fit
     * the per-request limits of the model, the batches are sent concurrently, and the embeddings are returned in input order.
     *
     * @param model the model to run inference against
     * @param request the embedding request
     * @param timeout the request timeout
     * @param listener the listener to notify with results or failure
     */
    @Override
    protected void doEmbeddingInfer(
        Model model,
        EmbeddingRequest request,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof TencentCloudEmbeddingsModel embeddingsModel && model.getTaskType() == TaskType.EMBEDDING) {
            var unsupportedInputError = findUnsupportedEmbeddingInput(request.inputs());
            if (unsupportedInputError != null) {
                listener.onFailure(new ElasticsearchStatusException(unsupportedInputError, RestStatus.BAD_REQUEST));
                return;
            }

            var actionCreator = new TencentCloudActionCreator(getSender(), getServiceComponents());
            var action = embeddingsModel.accept(actionCreator, request.taskSettings());
            executeMultimodalEmbeddings(embeddingsModel, action, request.inputs(), request.inputType(), timeout, listener);
            return;
        }

        listener.onFailure(createInvalidModelException(model));
    }

    /**
     * Splits the inputs into batches that fit the per-request limits of the model, sends the batches concurrently, and returns the
     * embeddings in input order.
     */
    private static void executeMultimodalEmbeddings(
        TencentCloudEmbeddingsModel model,
        ExecutableAction action,
        List<InferenceStringGroup> inputs,
        @Nullable InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var batches = TencentCloudMultimodalModelLimits.forModel(model.getServiceSettings().modelId()).partition(inputs);
        var batchResults = new AtomicReferenceArray<InferenceServiceResults>(batches.size());
        try (var refs = new RefCountingListener(listener.map(ignored -> mergeEmbeddingResults(batchResults)))) {
            for (int i = 0; i < batches.size(); i++) {
                int batchIndex = i;
                action.execute(
                    new EmbeddingsInput(batches.get(i), inputType),
                    timeout,
                    refs.acquire(results -> batchResults.set(batchIndex, results))
                );
            }
        }
    }

    /**
     * Returns an error message for the first input the multimodal embeddings API cannot accept, or {@code null} if all inputs are
     * supported.
     */
    @Nullable
    static String findUnsupportedEmbeddingInput(List<InferenceStringGroup> inputs) {
        for (int i = 0; i < inputs.size(); i++) {
            var input = inputs.get(i).value();
            var isBase64 = input.dataFormat() == DataFormat.BASE64;
            switch (input.dataType()) {
                case TEXT -> {
                }
                case IMAGE -> {
                    if (isBase64 && SUPPORTED_BASE64_IMAGE_PREFIXES.stream().noneMatch(input.value()::startsWith)) {
                        return Strings.format(
                            "The %s service only supports base64 images of type [png, jpeg, gif, webp], found an unsupported image at "
                                + "$.%s[%d]",
                            NAME,
                            EmbeddingRequest.INPUT_FIELD,
                            i
                        );
                    }
                }
                case VIDEO -> {
                    if (isBase64 && input.value().startsWith(SUPPORTED_BASE64_VIDEO_PREFIX) == false) {
                        return Strings.format(
                            "The %s service only supports base64 videos of type [mp4], found an unsupported video at $.%s[%d]",
                            NAME,
                            EmbeddingRequest.INPUT_FIELD,
                            i
                        );
                    }
                }
                default -> {
                    return Strings.format(
                        "The %s service does not support [%s] inputs, found at $.%s[%d]",
                        NAME,
                        input.dataType(),
                        EmbeddingRequest.INPUT_FIELD,
                        i
                    );
                }
            }
        }
        return null;
    }

    static InferenceServiceResults mergeEmbeddingResults(AtomicReferenceArray<InferenceServiceResults> batchResults) {
        if (batchResults.length() == 1) {
            return batchResults.get(0);
        }
        List<EmbeddingFloatResults.Embedding> embeddings = new ArrayList<>();
        for (int i = 0; i < batchResults.length(); i++) {
            embeddings.addAll(((GenericDenseEmbeddingFloatResults) batchResults.get(i)).embeddings());
        }
        return new GenericDenseEmbeddingFloatResults(embeddings);
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
        var isMultimodal = model.getTaskType() == TaskType.EMBEDDING;
        if (isMultimodal) {
            var unsupportedInputError = findUnsupportedEmbeddingInput(ChunkInferenceInput.inputs(inputs));
            if (unsupportedInputError != null) {
                listener.onFailure(new ElasticsearchStatusException(unsupportedInputError, RestStatus.BAD_REQUEST));
                return;
            }
        }
        var actionCreator = new TencentCloudActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            embeddingsModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = embeddingsModel.accept(actionCreator, taskSettings);
            if (isMultimodal) {
                // A chunk batch can exceed the per-modality limits or mix base64 and URL inputs, so split it further
                executeMultimodalEmbeddings(
                    embeddingsModel,
                    action,
                    request.batch().inputs().get(),
                    inputType,
                    timeout,
                    request.listener()
                );
            } else {
                action.execute(
                    new EmbeddingsInput(request.batch().inputs(), request.batch().ramBytesUsed(), inputType),
                    timeout,
                    request.listener()
                );
            }
        }
    }

    /**
     * Validates that the input type is permitted for TencentCloud models. Only the {@code embedding} task accepts input types other
     * than unspecified or internal ones.
     *
     * @param inputType the input type to validate
     * @param model the model the inference is run against
     * @param validationException the exception to collect validation errors into
     */
    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        // The embedding task maps every input type to a prompt name
        if (model.getTaskType() != TaskType.EMBEDDING) {
            ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
        }
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
            var similarity = serviceSettings.similarity() != null ? serviceSettings.similarity() : SimilarityMeasure.COSINE;

            var updatedServiceSettings = serviceSettings.updateEmbeddingDetails(embeddingSize, similarity);
            if (updatedServiceSettings.equals(serviceSettings)) {
                return model;
            }

            return new TencentCloudEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        }
        throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
    }

    @Override
    public boolean supportsNonTextEmbeddingContent() {
        return true;
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
                        "The name of the model to use for the inference task, e.g. bge-m3 (text embeddings),"
                            + " WeMM-Embedding-2B (multimodal embeddings),"
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
