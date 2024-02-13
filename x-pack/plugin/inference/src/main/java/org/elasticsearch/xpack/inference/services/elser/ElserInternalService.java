/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.STARTED;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class ElserInternalService implements InferenceService {

    public static final String NAME = "elser";

    static final String ELSER_V1_MODEL = ".elser_model_1";
    // Default non platform specific v2 model
    static final String ELSER_V2_MODEL = ".elser_model_2";
    static final String ELSER_V2_MODEL_LINUX_X86 = ".elser_model_2_linux-x86_64";

    public static Set<String> VALID_ELSER_MODEL_IDS = Set.of(
        ElserInternalService.ELSER_V1_MODEL,
        ElserInternalService.ELSER_V2_MODEL,
        ElserInternalService.ELSER_V2_MODEL_LINUX_X86
    );

    private static final String OLD_MODEL_ID_FIELD_NAME = "model_version";

    private final OriginSettingClient client;

    public ElserInternalService(InferenceServiceExtension.InferenceServiceFactoryContext context) {
        this.client = new OriginSettingClient(context.client(), ClientHelper.INFERENCE_ORIGIN);
    }

    public boolean isInClusterService() {
        return true;
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> modelArchitectures,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            var serviceSettingsBuilder = ElserInternalServiceSettings.fromMap(serviceSettingsMap);

            if (serviceSettingsBuilder.getModelId() == null) {
                serviceSettingsBuilder.setModelId(selectDefaultModelVersionBasedOnClusterArchitecture(modelArchitectures));
            }

            Map<String, Object> taskSettingsMap;
            // task settings are optional
            if (config.containsKey(ModelConfigurations.TASK_SETTINGS)) {
                taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
            } else {
                taskSettingsMap = Map.of();
            }

            var taskSettings = taskSettingsFromMap(taskType, taskSettingsMap);

            throwIfNotEmptyMap(config, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(
                new ElserInternalModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    (ElserInternalServiceSettings) serviceSettingsBuilder.build(),
                    taskSettings
                )
            );
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    private static String selectDefaultModelVersionBasedOnClusterArchitecture(Set<String> modelArchitectures) {
        // choose a default model ID based on the cluster architecture
        boolean homogenous = modelArchitectures.size() == 1;
        if (homogenous && modelArchitectures.iterator().next().equals("linux-x86_64")) {
            // Use the hardware optimized model
            return ELSER_V2_MODEL_LINUX_X86;
        } else {
            // default to the platform-agnostic model
            return ELSER_V2_MODEL;
        }
    }

    @Override
    public ElserInternalModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parsePersistedConfig(inferenceEntityId, taskType, config);
    }

    @Override
    public ElserInternalModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);

        // Change from old model_version field name to new model_id field name as of
        // TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED
        if (serviceSettingsMap.containsKey(OLD_MODEL_ID_FIELD_NAME)) {
            String modelId = ServiceUtils.removeAsType(serviceSettingsMap, OLD_MODEL_ID_FIELD_NAME, String.class);
            serviceSettingsMap.put(ElserInternalServiceSettings.MODEL_ID, modelId);
        }

        var serviceSettingsBuilder = ElserInternalServiceSettings.fromMap(serviceSettingsMap);

        Map<String, Object> taskSettingsMap;
        // task settings are optional
        if (config.containsKey(ModelConfigurations.TASK_SETTINGS)) {
            taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        } else {
            taskSettingsMap = Map.of();
        }

        var taskSettings = taskSettingsFromMap(taskType, taskSettingsMap);

        return new ElserInternalModel(
            inferenceEntityId,
            taskType,
            NAME,
            (ElserInternalServiceSettings) serviceSettingsBuilder.build(),
            taskSettings
        );
    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        if (model instanceof ElserInternalModel == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Error starting model, [" + model.getConfigurations().getInferenceEntityId() + "] is not an ELSER model"
                )
            );
            return;
        }

        if (model.getConfigurations().getTaskType() != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(
                new IllegalStateException(TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME))
            );
            return;
        }

        client.execute(StartTrainedModelDeploymentAction.INSTANCE, startDeploymentRequest(model), elserNotDownloadedListener(listener));
    }

    private static StartTrainedModelDeploymentAction.Request startDeploymentRequest(Model model) {
        var elserModel = (ElserInternalModel) model;
        var serviceSettings = elserModel.getServiceSettings();

        var startRequest = new StartTrainedModelDeploymentAction.Request(
            serviceSettings.getModelId(),
            model.getConfigurations().getInferenceEntityId()
        );
        startRequest.setNumberOfAllocations(serviceSettings.getNumAllocations());
        startRequest.setThreadsPerAllocation(serviceSettings.getNumThreads());
        startRequest.setWaitForState(STARTED);
        return startRequest;
    }

    private static ActionListener<CreateTrainedModelAssignmentAction.Response> elserNotDownloadedListener(
        ActionListener<Boolean> listener
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(CreateTrainedModelAssignmentAction.Response response) {
                listener.onResponse(Boolean.TRUE);
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    listener.onFailure(
                        new ResourceNotFoundException(
                            "Could not start the ELSER service as the ELSER model for this platform cannot be found."
                                + " ELSER needs to be downloaded before it can be started."
                        )
                    );
                    return;
                }
                listener.onFailure(e);
            }
        };
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
    public void infer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ActionListener<InferenceServiceResults> listener
    ) {
        // No task settings to override with requestTaskSettings

        try {
            checkCompatibleTaskType(model.getConfigurations().getTaskType());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        var request = InferTrainedModelDeploymentAction.Request.forTextInput(
            model.getConfigurations().getInferenceEntityId(),
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            input,
            TimeValue.timeValueSeconds(10)  // TODO get timeout from request
        );
        client.execute(
            InferTrainedModelDeploymentAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap((l, inferenceResult) -> l.onResponse(SparseEmbeddingResults.of(inferenceResult.getResults())))
        );
    }

    @Override
    public void chunkedInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        try {
            checkCompatibleTaskType(model.getConfigurations().getTaskType());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        var configUpdate = chunkingOptions.settingsArePresent()
            ? new TokenizationConfigUpdate(chunkingOptions.windowSize(), chunkingOptions.span())
            : TextExpansionConfigUpdate.EMPTY_UPDATE;

        var request = InferTrainedModelDeploymentAction.Request.forTextInput(
            model.getConfigurations().getInferenceEntityId(),
            configUpdate,
            input,
            TimeValue.timeValueSeconds(10)  // TODO get timeout from request
        );
        request.setChunkResults(true);

        client.execute(
            InferTrainedModelDeploymentAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap((l, inferenceResult) -> l.onResponse(translateChunkedResults(inferenceResult.getResults())))
        );
    }

    private void checkCompatibleTaskType(TaskType taskType) {
        if (TaskType.SPARSE_EMBEDDING.isAnyOrSame(taskType) == false) {
            throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        }
    }

    @Override
    public void putModel(Model model, ActionListener<Boolean> listener) {
        if (model instanceof ElserInternalModel == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Error starting model, [" + model.getConfigurations().getInferenceEntityId() + "] is not an ELSER model"
                )
            );
            return;
        } else {
            String modelId = ((ElserInternalModel) model).getServiceSettings().getModelId();
            var fieldNames = List.<String>of();
            var input = new TrainedModelInput(fieldNames);
            var config = TrainedModelConfig.builder().setInput(input).setModelId(modelId).build();
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
        }
    }

    @Override
    public void isModelDownloaded(Model model, ActionListener<Boolean> listener) {
        ActionListener<GetTrainedModelsAction.Response> getModelsResponseListener = listener.delegateFailure((delegate, response) -> {
            if (response.getResources().count() < 1) {
                delegate.onResponse(Boolean.FALSE);
            } else {
                delegate.onResponse(Boolean.TRUE);
            }
        });

        if (model instanceof ElserInternalModel elserModel) {
            String modelId = elserModel.getServiceSettings().getModelId();
            GetTrainedModelsAction.Request getRequest = new GetTrainedModelsAction.Request(modelId);
            executeAsyncWithOrigin(client, INFERENCE_ORIGIN, GetTrainedModelsAction.INSTANCE, getRequest, getModelsResponseListener);
        } else {
            listener.onFailure(
                new IllegalArgumentException(
                    "Can not download model automatically for ["
                        + model.getConfigurations().getInferenceEntityId()
                        + "] you may need to download it through the trained models API or with eland."
                )
            );
        }
    }

    private static ElserMlNodeTaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.SPARSE_EMBEDDING) {
            throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        }

        // no config options yet
        return ElserMlNodeTaskSettings.DEFAULT;
    }

    private List<ChunkedInferenceServiceResults> translateChunkedResults(List<InferenceResults> inferenceResults) {
        var translated = new ArrayList<ChunkedInferenceServiceResults>();

        for (var inferenceResult : inferenceResults) {
            if (inferenceResult instanceof ChunkedTextExpansionResults mlChunkedResult) {
                translated.add(ChunkedSparseEmbeddingResults.ofMlResult(mlChunkedResult));
            } else {
                throw new ElasticsearchStatusException(
                    "Expected a chunked inference [{}] received [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    ChunkedTextExpansionResults.NAME,
                    inferenceResult.getWriteableName()
                );
            }
        }
        return translated;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }
}
