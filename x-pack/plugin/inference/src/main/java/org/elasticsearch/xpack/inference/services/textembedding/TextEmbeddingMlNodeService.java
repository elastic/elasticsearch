/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.textembedding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.inference.services.settings.MlNodeServiceSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.settings.MlNodeServiceSettings.MODEL_VERSION;

public class TextEmbeddingMlNodeService implements InferenceService {

    public static final String NAME = "text_embedding";

    static final String MULTILINGUAL_E5_SMALL_MODEL_ID = ".multilingual-e5-small";
    static final String MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86 = ".multilingual-e5-small_linux-x86_64";

    private final OriginSettingClient client;

    private static final Logger logger = LogManager.getLogger(TextEmbeddingMlNodeService.class);

    public TextEmbeddingMlNodeService(InferenceServiceExtension.InferenceServiceFactoryContext context) {
        this.client = new OriginSettingClient(context.client(), ClientHelper.INFERENCE_ORIGIN);
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        ActionListener<Model> modelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            String modelId = (String) serviceSettingsMap.get(MODEL_VERSION);
            if (modelId == null) {
                throw new IllegalArgumentException("Error parsing request config, model id is missing");
            }
            if (MultilingualE5SmallMlNodeServiceSettings.MODEL_VARIANTS.contains(modelId)) {
                e5Case(inferenceEntityId, taskType, config, platformArchitectures, serviceSettingsMap, modelListener);
            } else {
                customElandCase(inferenceEntityId, taskType, config, platformArchitectures, serviceSettingsMap, modelListener);
            }
        } catch (Exception e) {
            modelListener.onFailure(e);
        }
    }

    private void customElandCase(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        Map<String, Object> serviceSettingsMap,
        ActionListener<Model> modelListener
    ) {
        String modelId = (String) serviceSettingsMap.get(MODEL_VERSION);
        var request = new GetTrainedModelsAction.Request(modelId);

        var getModelsListener = modelListener.<GetTrainedModelsAction.Response>delegateFailureAndWrap((delegate, response) -> {
            if (response.getResources().count() < 1) {
                throw new IllegalArgumentException(
                    "Error parsing request config, model id does not match any models available on this platform. Was ["
                        + modelId
                        + "]. You may need to load it into the cluster using eland."
                );
            } else {
                serviceSettingsMap.put(MODEL_VERSION, response.getResources().results().get(0).getModelId());
                delegate.onResponse(
                    new CustomElandModel(
                        inferenceEntityId,
                        taskType,
                        name(),
                        (CustomElandMlNodeServiceSettings) CustomElandMlNodeServiceSettings.fromMap(serviceSettingsMap).build()
                    )
                );
            }
        });

        client.execute(GetTrainedModelsAction.INSTANCE, request, getModelsListener);
    }

    private void e5Case(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        Map<String, Object> serviceSettingsMap,
        ActionListener<Model> modelListener
    ) {
        var e5ServiceSettings = MultilingualE5SmallMlNodeServiceSettings.fromMap(serviceSettingsMap);

        if (e5ServiceSettings.getModelVariant() == null) {
            e5ServiceSettings.setModelVariant(selectDefaultModelVersionBasedOnClusterArchitecture(platformArchitectures));
        }

        if (modelVariantDoesNotMatchArchitecturesAndIsNotPlatformAgnostic(platformArchitectures, e5ServiceSettings)) {
            throw new IllegalArgumentException(
                "Error parsing request config, model id does not match any models available on this platform. Was ["
                    + e5ServiceSettings.getModelVariant()
                    + "]"
            );
        }

        throwIfNotEmptyMap(config, name());
        throwIfNotEmptyMap(serviceSettingsMap, name());

        modelListener.onResponse(
            new MultilingualE5SmallModel(
                inferenceEntityId,
                taskType,
                NAME,
                (MultilingualE5SmallMlNodeServiceSettings) e5ServiceSettings.build()
            )
        );
    }

    private static boolean modelVariantDoesNotMatchArchitecturesAndIsNotPlatformAgnostic(
        Set<String> platformArchitectures,
        MlNodeServiceSettings.Builder e5ServiceSettings
    ) {
        return e5ServiceSettings.getModelVariant()
            .equals(selectDefaultModelVersionBasedOnClusterArchitecture(platformArchitectures)) == false
            && e5ServiceSettings.getModelVariant().equals(MULTILINGUAL_E5_SMALL_MODEL_ID) == false;
    }

    @Override
    public TextEmbeddingModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parsePersistedConfig(inferenceEntityId, taskType, config);
    }

    @Override
    public TextEmbeddingModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);

        String modelId = (String) serviceSettingsMap.get(MODEL_VERSION);
        if (modelId == null) {
            throw new IllegalArgumentException("Error parsing request config, model id is missing");
        }

        if (MultilingualE5SmallMlNodeServiceSettings.MODEL_VARIANTS.contains(modelId)) {
            return new MultilingualE5SmallModel(
                inferenceEntityId,
                taskType,
                NAME,
                (MultilingualE5SmallMlNodeServiceSettings) MultilingualE5SmallMlNodeServiceSettings.fromMap(serviceSettingsMap).build()
            );
        } else {
            return new CustomElandModel(
                inferenceEntityId,
                taskType,
                name(),
                (CustomElandMlNodeServiceSettings) CustomElandMlNodeServiceSettings.fromMap(serviceSettingsMap).build()
            );
        }

    }

    @Override
    public void infer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (TaskType.TEXT_EMBEDDING.isAnyOrSame(model.getConfigurations().getTaskType()) == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

        var request = InferTrainedModelDeploymentAction.Request.forTextInput(
            model.getConfigurations().getInferenceEntityId(),
            TextEmbeddingConfigUpdate.EMPTY_INSTANCE,
            input,
            TimeValue.timeValueSeconds(10)  // TODO get timeout from request
        );

        client.execute(
            InferTrainedModelDeploymentAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap((l, inferenceResult) -> l.onResponse(TextEmbeddingResults.of(inferenceResult.getResults())))
        );
    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        if (model instanceof TextEmbeddingModel == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Error starting model, [" + model.getConfigurations().getInferenceEntityId() + "] is not a text embedding model model"
                )
            );
            return;
        }

        if (model.getConfigurations().getTaskType() != TaskType.TEXT_EMBEDDING) {
            listener.onFailure(
                new IllegalStateException(TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME))
            );
            return;
        }

        var startRequest = ((TextEmbeddingModel) model).getStartTrainedModelDeploymentActionRequest();
        var responseListener = ((TextEmbeddingModel) model).getCreateTrainedModelAssignmentActionListener(model, listener);

        client.execute(StartTrainedModelDeploymentAction.INSTANCE, startRequest, responseListener);
    }

    @Override
    public void stop(String inferenceEntityId, ActionListener<Boolean> listener) {
        client.execute(
            StopTrainedModelDeploymentAction.INSTANCE,
            new StopTrainedModelDeploymentAction.Request(inferenceEntityId),
            listener.delegateFailureAndWrap((delegatedResponseListener, response) -> delegatedResponseListener.onResponse(Boolean.TRUE))
        );
    }

    @Override
    public void putModel(Model model, ActionListener<Boolean> listener) {
        if (model instanceof TextEmbeddingModel == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Error starting model, [" + model.getConfigurations().getInferenceEntityId() + "] is not a TextEmbedding model"
                )
            );
            return;
        } else if (model instanceof MultilingualE5SmallModel e5Model) {
            String modelVariant = e5Model.getServiceSettings().getModelVariant();
            var fieldNames = List.<String>of();
            var input = new TrainedModelInput(fieldNames);
            var config = TrainedModelConfig.builder().setInput(input).setModelId(modelVariant).build();
            PutTrainedModelAction.Request putRequest = new PutTrainedModelAction.Request(config, false, true);
            executeAsyncWithOrigin(
                client,
                INFERENCE_ORIGIN,
                PutTrainedModelAction.INSTANCE,
                putRequest,
                listener.delegateFailure((l, r) -> {
                    l.onResponse(Boolean.TRUE);
                })
            );
        } else if (model instanceof CustomElandModel elandModel) {
            logger.info("Custom eland model detected, model must have been already loaded into the cluster with eland.");
            listener.onResponse(Boolean.TRUE);
        } else {
            listener.onFailure(
                new IllegalArgumentException(
                    "Can not download model automatically, ["
                        + model.getConfigurations().getInferenceEntityId()
                        + "] you may need to download it through the trained models API or with eland."
                )
            );
            return;
        }
    }

    @Override
    public boolean isInClusterService() {
        return true;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public String name() {
        return NAME;
    }

    private static String selectDefaultModelVersionBasedOnClusterArchitecture(Set<String> modelArchitectures) {
        // choose a default model version based on the cluster architecture
        boolean homogenous = modelArchitectures.size() == 1;
        if (homogenous && modelArchitectures.iterator().next().equals("linux-x86_64")) {
            // Use the hardware optimized model
            return MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86;
        } else {
            // default to the platform-agnostic model
            return MULTILINGUAL_E5_SMALL_MODEL_ID;
        }
    }

}
