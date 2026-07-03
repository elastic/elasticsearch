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
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModelCreator;
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
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;

/**
 * Inference service integration for the TencentCloud AI Gateway (OpenAI-compatible), supporting {@code text_embedding},
 * {@code chat_completion}, and {@code rerank} task types.
 */
public class TencentCloudService extends SenderService<TencentCloudModel> implements RerankingInferenceService {

    public static final String NAME = "tencentcloud";
    private static final String SERVICE_NAME = "TencentCloud AI Gateway";

    // Batch limit for embedding chunking. TencentCloud AI Gateway does not document a hard cap; use a conservative value.
    private static final int EMBEDDING_MAX_BATCH_SIZE = 32;

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK
    );
    private static final EnumSet<TaskType> SUPPORTED_STREAMING_TASKS = EnumSet.of(TaskType.CHAT_COMPLETION);

    private static final Map<TaskType, ModelCreator<? extends TencentCloudModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        new TencentCloudEmbeddingsModelCreator(),
        TaskType.CHAT_COMPLETION,
        new TencentCloudChatCompletionModelCreator(),
        TaskType.RERANK,
        new TencentCloudRerankModelCreator()
    );

    public TencentCloudService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

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

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof TencentCloudChatCompletionModel chatModel) {
            var requestManager = new TencentCloudChatCompletionRequestManager(chatModel, getServiceComponents().threadPool());
            var errorMessage = constructFailedToSendRequestMessage("TencentCloud completions");
            var action = new SenderExecutableAction(getSender(), requestManager, errorMessage);
            action.execute(inputs, timeout, listener);
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

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof TencentCloudChatCompletionModel chatModel) {
            var requestManager = new TencentCloudChatCompletionRequestManager(chatModel, getServiceComponents().threadPool());
            var errorMessage = constructFailedToSendRequestMessage("TencentCloud chat completions");
            var action = new SenderExecutableAction(getSender(), requestManager, errorMessage);
            action.execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

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
            action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
        }
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        // TencentCloud AI Gateway does not distinguish input types; accept any.
    }

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
        return TransportVersion.minimumCompatible();
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        // BGE reranker models (bge-reranker-large, bge-reranker-v2-m3) support up to 512-token inputs.
        // Using 1 token = 0.75 words, that is roughly 384 words. Use a conservative value with headroom.
        return 350;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The name of the model to use for the inference task, e.g. bge-m3, deepseek-v3, bge-reranker-v2-m3."
                    )
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDefaultValue(
                        "http://bj.aisearch.tencentelasticsearch.com/v1/embeddings"
                    )
                        .setDescription(
                            "The full URL endpoint to use for the requests. Defaults to the AI Gateway endpoint corresponding to the "
                                + "task type."
                        )
                        .setLabel("URL")
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
