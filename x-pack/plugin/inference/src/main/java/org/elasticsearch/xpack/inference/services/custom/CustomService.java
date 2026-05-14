/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Strings;
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
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.validation.CustomServiceIntegrationValidator;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs.fromRerankRequest;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedUnifiedCompletionOperation;

public class CustomService extends SenderService<CustomModel> implements RerankingInferenceService {

    public static final String NAME = "custom";
    private static final String SERVICE_NAME = "Custom";

    private static final TransportVersion INFERENCE_CUSTOM_SERVICE_ADDED = TransportVersion.fromName("inference_custom_service_added");

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.SPARSE_EMBEDDING,
        TaskType.RERANK,
        TaskType.COMPLETION
    );
    private static final CustomModelCreator MODEL_CREATOR = new CustomModelCreator();
    private static final Map<TaskType, ModelCreator<? extends CustomModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        MODEL_CREATOR,
        TaskType.SPARSE_EMBEDDING,
        MODEL_CREATOR,
        TaskType.COMPLETION,
        MODEL_CREATOR,
        TaskType.RERANK,
        MODEL_CREATOR
    );

    public CustomService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public CustomService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
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
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throwUnsupportedUnifiedCompletionOperation(NAME);
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof CustomModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        CustomModel customModel = (CustomModel) model;

        var overriddenModel = CustomModel.of(customModel, taskSettings);

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(SERVICE_NAME);
        var manager = CustomRequestManager.of(overriddenModel, getServiceComponents().threadPool());
        var action = new SenderExecutableAction(getSender(), manager, failedToSendRequestErrorMessage);

        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        // The custom service doesn't do any validation for the input type because if the input type is supported a default
        // must be supplied within the service settings.
    }

    @Override
    protected void doRerankInfer(Model model, RerankRequest request, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (!(model instanceof CustomModel customModel)) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }
        var overriddenModel = CustomModel.of(customModel, request.taskSettings());

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(SERVICE_NAME);
        var manager = CustomRequestManager.of(overriddenModel, getServiceComponents().threadPool());
        var action = new SenderExecutableAction(getSender(), manager, failedToSendRequestErrorMessage);

        action.execute(fromRerankRequest(request), timeout, listener);
    }

    @Override
    public boolean supportsNewRerankCodePath() {
        return true;
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
        if (model instanceof CustomModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var customModel = (CustomModel) model;
        var overriddenModel = CustomModel.of(customModel, taskSettings);

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(SERVICE_NAME);
        var manager = CustomRequestManager.of(overriddenModel, getServiceComponents().threadPool());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            customModel.getServiceSettings().getBatchSize(),
            customModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = new SenderExecutableAction(getSender(), manager, failedToSendRequestErrorMessage);
            action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
        }
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof CustomModel customModel && customModel.getTaskType() == TaskType.TEXT_EMBEDDING) {
            var newServiceSettings = getCustomServiceSettings(customModel, embeddingSize);

            return new CustomModel(customModel, newServiceSettings);
        } else {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "Can't update embedding details for model of type: [%s], task type: [%s]",
                    model.getClass().getSimpleName(),
                    model.getTaskType()
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }

    private static CustomServiceSettings getCustomServiceSettings(CustomModel customModel, int embeddingSize) {
        var serviceSettings = customModel.getServiceSettings();
        var similarityFromModel = serviceSettings.similarity();
        var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

        return new CustomServiceSettings(
            new CustomServiceSettings.TextEmbeddingSettings(similarityToUse, embeddingSize, serviceSettings.getMaxInputTokens()),
            serviceSettings.getUrl(),
            serviceSettings.getHeaders(),
            serviceSettings.getQueryParameters(),
            serviceSettings.getRequestContentString(),
            serviceSettings.getResponseJsonParser(),
            serviceSettings.rateLimitSettings(),
            serviceSettings.getBatchSize(),
            serviceSettings.getInputTypeTranslator()
        );
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_CUSTOM_SERVICE_ADDED;
    }

    @Override
    public boolean hideFromConfigurationApi() {
        // The Custom service is very configurable so we're going to hide it from being exposed in the service API.
        return true;
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        // The model's max input length is not known at this point,
        // return a small default that will work with the smallest models
        // TODO add a way to configure this setting
        return RerankingInferenceService.CONSERVATIVE_DEFAULT_WINDOW_SIZE;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();
                // TODO revisit this
                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }

    @Override
    public ServiceIntegrationValidator getServiceIntegrationValidator(TaskType taskType) {
        if (taskType == TaskType.RERANK) {
            return new CustomServiceIntegrationValidator();
        }

        return null;
    }
}
