/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

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
import org.elasticsearch.inference.configuration.InferenceServiceFeatures;
import org.elasticsearch.inference.configuration.NonStreamingChatFeature;
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
import org.elasticsearch.xpack.inference.services.ocigenai.action.OciGenAiActionCreator;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.request.completion.OciGenAiChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModel;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModelCreator;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs.fromRerankRequest;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.REGION;

/**
 * Inference service for <a href="https://docs.oracle.com/en-us/iaas/Content/generative-ai/home.htm">OCI Generative AI</a>. Requests are
 * authenticated with an OCI API signing key using the OCI request signature scheme (implemented in
 * {@link org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiRequestSigner}) and sent to the regional inference
 * endpoint, or to a dedicated AI cluster endpoint when one is configured.
 */
public class OciGenAiService extends SenderService<OciGenAiModel> implements RerankingInferenceService {

    public static final String NAME = "ocigenai";
    private static final String SERVICE_NAME = "OCI Generative AI";

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK
    );

    /**
     * The input types accepted by the embedding models (translated to the OCI {@code inputType}).
     */
    public static final EnumSet<InputType> VALID_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    /**
     * The Cohere rerank models served by OCI Generative AI truncate at 4096 tokens. Using 1 token = 0.75 words as a rough estimate we
     * get 3072 words; allowing for some headroom the window size is set below that.
     */
    public static final int RERANK_WINDOW_SIZE = 2800;

    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new OciGenAiUnifiedChatCompletionResponseHandler(
        "OCI Generative AI chat completions"
    );

    private static final OciGenAiChatCompletionModelCreator CHAT_COMPLETION_MODEL_CREATOR = new OciGenAiChatCompletionModelCreator();
    private static final Map<TaskType, ModelCreator<? extends OciGenAiModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        new OciGenAiEmbeddingsModelCreator(),
        TaskType.COMPLETION,
        CHAT_COMPLETION_MODEL_CREATOR,
        TaskType.CHAT_COMPLETION,
        CHAT_COMPLETION_MODEL_CREATOR,
        TaskType.RERANK,
        new OciGenAiRerankModelCreator()
    );

    public OciGenAiService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public OciGenAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
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
    public TransportVersion getMinimalSupportedVersion() {
        return OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED;
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    @Override
    public boolean supportsNonStreamingChatCompletion() {
        return true;
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        return RERANK_WINDOW_SIZE;
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof OciGenAiEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            var updatedServiceSettings = new OciGenAiEmbeddingsServiceSettings(
                serviceSettings.common(),
                serviceSettings.dimensionsSetByUser(),
                embeddingSize,
                serviceSettings.maxInputTokens(),
                similarityToUse
            );

            return new OciGenAiEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof OciGenAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var ociGenAiModel = (OciGenAiModel) model;
        var action = ociGenAiModel.accept(new OciGenAiActionCreator(getSender(), getServiceComponents()), taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeAgainstAllowlist(inputType, VALID_INPUT_TYPE_VALUES, SERVICE_NAME, validationException);
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof OciGenAiChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var overriddenModel = OciGenAiChatCompletionModel.of((OciGenAiChatCompletionModel) model, inputs.getRequest());
        var requestManager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new OciGenAiChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );
        var errorMessage = OciGenAiActionCreator.buildErrorMessage(TaskType.CHAT_COMPLETION, model.getInferenceEntityId());
        var action = new SenderExecutableAction(getSender(), requestManager, errorMessage);

        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void doRerankInfer(Model model, RerankRequest request, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (model instanceof OciGenAiRerankModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var rerankModel = (OciGenAiRerankModel) model;
        var action = rerankModel.accept(new OciGenAiActionCreator(getSender(), getServiceComponents()), request.taskSettings());
        action.execute(fromRerankRequest(request), timeout, listener);
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
        if (model instanceof OciGenAiEmbeddingsModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var embeddingsModel = (OciGenAiEmbeddingsModel) model;
        var actionCreator = new OciGenAiActionCreator(getSender(), getServiceComponents());

        var batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            getRegexReadLimitFactor(),
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

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    REGION,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The OCI region identifier of the Generative AI inference endpoint, for example us-chicago-1. "
                            + "Either the region or the url must be provided."
                    )
                        .setLabel("Region")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    COMPARTMENT_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The OCID of the compartment the inference requests are authorized against."
                    )
                        .setLabel("Compartment OCID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The OCI Generative AI model to use for the inference task, for example cohere.embed-v4.0 or "
                            + "meta.llama-3.3-70b-instruct."
                    )
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    ENDPOINT_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The OCID of a dedicated AI cluster endpoint hosting the model. When omitted the model is served on-demand."
                    )
                        .setLabel("Endpoint OCID")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The base URL of the Generative AI inference endpoint, overriding the public regional endpoint derived from "
                            + "the region (for example a private endpoint or a different OCI realm)."
                    )
                        .setLabel("URL")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    DIMENSIONS,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING)).setDescription(
                        "The number of dimensions of the embeddings. Passed to the model as the output dimensions when supported "
                            + "(cohere.embed-v4.0); otherwise discovered from the model."
                    )
                        .setLabel("Dimensions")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.put(
                    MAX_INPUT_TOKENS,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING)).setDescription(
                        "Allows you to specify the maximum number of tokens per input."
                    )
                        .setLabel("Maximum Input Tokens")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(true)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.putAll(OciGenAiSecretSettings.Configuration.get());
                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration(SUPPORTED_TASK_TYPES));

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES)
                    .setConfigurations(configurationMap)
                    .setFeatures(InferenceServiceFeatures.of(NonStreamingChatFeature.SUPPORTED_INSTANCE))
                    .build();
            }
        );
    }
}
