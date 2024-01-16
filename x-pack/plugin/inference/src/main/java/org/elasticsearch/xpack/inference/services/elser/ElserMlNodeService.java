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
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.STARTED;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class ElserMlNodeService implements InferenceService {

    public static final String NAME = "elser";

    static final String ELSER_V1_MODEL = ".elser_model_1";
    // Default non platform specific v2 model
    static final String ELSER_V2_MODEL = ".elser_model_2";
    static final String ELSER_V2_MODEL_LINUX_X86 = ".elser_model_2_linux-x86_64";

    public static Set<String> VALID_ELSER_MODELS = Set.of(
        ElserMlNodeService.ELSER_V1_MODEL,
        ElserMlNodeService.ELSER_V2_MODEL,
        ElserMlNodeService.ELSER_V2_MODEL_LINUX_X86
    );

    private final OriginSettingClient client;

    public ElserMlNodeService(InferenceServiceExtension.InferenceServiceFactoryContext context) {
        this.client = new OriginSettingClient(context.client(), ClientHelper.INFERENCE_ORIGIN);
    }

    public boolean isInClusterService() {
        return true;
    }

    @Override
    public ElserMlNodeModel parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> modelArchitectures
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        var serviceSettingsBuilder = ElserMlNodeServiceSettings.fromMap(serviceSettingsMap);

        if (serviceSettingsBuilder.getModelVariant() == null) {
            serviceSettingsBuilder.setModelVariant(selectDefaultModelVersionBasedOnClusterArchitecture(modelArchitectures));
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

        return new ElserMlNodeModel(modelId, taskType, NAME, serviceSettingsBuilder.build(), taskSettings);
    }

    private static String selectDefaultModelVersionBasedOnClusterArchitecture(Set<String> modelArchitectures) {
        // choose a default model version based on the cluster architecture
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
    public ElserMlNodeModel parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parsePersistedConfig(modelId, taskType, config);
    }

    @Override
    public ElserMlNodeModel parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        var serviceSettingsBuilder = ElserMlNodeServiceSettings.fromMap(serviceSettingsMap);

        Map<String, Object> taskSettingsMap;
        // task settings are optional
        if (config.containsKey(ModelConfigurations.TASK_SETTINGS)) {
            taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        } else {
            taskSettingsMap = Map.of();
        }

        var taskSettings = taskSettingsFromMap(taskType, taskSettingsMap);

        return new ElserMlNodeModel(modelId, taskType, NAME, serviceSettingsBuilder.build(), taskSettings);
    }

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        if (model instanceof ElserMlNodeModel == false) {
            listener.onFailure(
                new IllegalStateException("Error starting model, [" + model.getConfigurations().getModelId() + "] is not an elser model")
            );
            return;
        }

        if (model.getConfigurations().getTaskType() != TaskType.SPARSE_EMBEDDING) {
            listener.onFailure(
                new IllegalStateException(TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME))
            );
            return;
        }

        var elserModel = (ElserMlNodeModel) model;
        var serviceSettings = elserModel.getServiceSettings();

        var startRequest = new StartTrainedModelDeploymentAction.Request(
            serviceSettings.getModelVariant(),
            model.getConfigurations().getModelId()
        );
        startRequest.setNumberOfAllocations(serviceSettings.getNumAllocations());
        startRequest.setThreadsPerAllocation(serviceSettings.getNumThreads());
        startRequest.setWaitForState(STARTED);

        client.execute(StartTrainedModelDeploymentAction.INSTANCE, startRequest, elserNotDownloadedListener(model, listener));
    }

    private static ActionListener<CreateTrainedModelAssignmentAction.Response> elserNotDownloadedListener(
        Model model,
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
                                + " ELSER needs to be downloaded before it can be started"
                        )
                    );
                    return;
                }
                listener.onFailure(e);
            }
        };
    }

    @Override
    public void stop(String modelId, ActionListener<Boolean> listener) {
        client.execute(
            StopTrainedModelDeploymentAction.INSTANCE,
            new StopTrainedModelDeploymentAction.Request(modelId),
            listener.delegateFailureAndWrap((delegatedResponseListener, response) -> delegatedResponseListener.onResponse(Boolean.TRUE))
        );
    }

    @Override
    public void infer(Model model, List<String> input, Map<String, Object> taskSettings, ActionListener<InferenceServiceResults> listener) {
        // No task settings to override with requestTaskSettings

        if (TaskType.SPARSE_EMBEDDING.isAnyOrSame(model.getConfigurations().getTaskType()) == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), NAME),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

        var request = InferTrainedModelDeploymentAction.Request.forTextInput(
            model.getConfigurations().getModelId(),
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
    public void putModel(Model model, ActionListener<Boolean> listener) {
        if (model instanceof ElserMlNodeModel == false) {
            listener.onFailure(
                new IllegalStateException("Error starting model, [" + model.getConfigurations().getModelId() + "] is not an elser model")
            );
            return;
        } else {
            String modelVariant = ((ElserMlNodeModel) model).getServiceSettings().getModelVariant();
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
        }
    }

    private static ElserMlNodeTaskSettings taskSettingsFromMap(TaskType taskType, Map<String, Object> config) {
        if (taskType != TaskType.SPARSE_EMBEDDING) {
            throw new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        }

        // no config options yet
        return ElserMlNodeTaskSettings.DEFAULT;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ELSER_SERVICE_MODEL_VERSION_ADDED;
    }
}
