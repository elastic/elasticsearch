/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.huggingface.action.HuggingFaceActionCreator;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModel;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.huggingface.request.completion.HuggingFaceUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;

/**
 * This class is responsible for managing the Hugging Face inference service.
 * It manages model creation, as well as chunked, non-chunked, and unified completion inference.
 */
public class HuggingFaceService extends HuggingFaceBaseService {
    public static final String NAME = "hugging_face";

    private static final String SERVICE_NAME = "Hugging Face";
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.SPARSE_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION
    );
    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new HuggingFaceChatCompletionResponseHandler(
        "hugging face chat completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    public HuggingFaceService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    protected HuggingFaceModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new HuggingFaceEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case SPARSE_EMBEDDING -> new HuggingFaceElserModel(inferenceEntityId, taskType, NAME, serviceSettings, secretSettings, context);
            case CHAT_COMPLETION, COMPLETION -> new HuggingFaceChatCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                secretSettings,
                context
            );
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof HuggingFaceEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.COSINE : similarityFromModel;

            var updatedServiceSettings = new HuggingFaceServiceSettings(
                serviceSettings.uri(),
                similarityToUse,
                embeddingSize,
                embeddingsModel.getTokenLimit(),
                serviceSettings.rateLimitSettings()
            );

            return new HuggingFaceEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        EmbeddingsInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        if (model instanceof HuggingFaceModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var huggingFaceModel = (HuggingFaceModel) model;
        var actionCreator = new HuggingFaceActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs.getInputs(),
            EMBEDDING_MAX_BATCH_SIZE,
            huggingFaceModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = huggingFaceModel.accept(actionCreator);
            action.execute(EmbeddingsInput.fromStrings(request.batch().inputs().get(), inputType), timeout, request.listener());
        }
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof HuggingFaceChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        HuggingFaceChatCompletionModel huggingFaceChatCompletionModel = (HuggingFaceChatCompletionModel) model;
        var overriddenModel = HuggingFaceChatCompletionModel.of(huggingFaceChatCompletionModel, inputs.getRequest());
        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new HuggingFaceUnifiedChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );
        var errorMessage = HuggingFaceActionCreator.buildErrorMessage(TaskType.CHAT_COMPLETION, model.getInferenceEntityId());
        var action = new SenderExecutableAction(getSender(), manager, errorMessage);

        action.execute(inputs, timeout, listener);
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
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
    public String name() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private Configuration() {}

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("The URL endpoint to use for the requests.")
                        .setLabel("URL")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(DefaultSecretSettings.toSettingsConfiguration(SUPPORTED_TASK_TYPES));
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
