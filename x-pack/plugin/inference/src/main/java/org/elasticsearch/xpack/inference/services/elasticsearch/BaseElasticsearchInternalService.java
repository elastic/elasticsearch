/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.MlPlatformArchitecturesUtil;
import org.elasticsearch.xpack.inference.DefaultElserFeatureFlag;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public abstract class BaseElasticsearchInternalService implements InferenceService {

    protected final OriginSettingClient client;
    protected final ExecutorService inferenceExecutor;
    protected final Consumer<ActionListener<Set<String>>> platformArch;

    private static final Logger logger = LogManager.getLogger(BaseElasticsearchInternalService.class);

    public BaseElasticsearchInternalService(InferenceServiceExtension.InferenceServiceFactoryContext context) {
        this.client = new OriginSettingClient(context.client(), ClientHelper.INFERENCE_ORIGIN);
        this.inferenceExecutor = context.threadPool().executor(InferencePlugin.UTILITY_THREAD_POOL_NAME);
        this.platformArch = this::platformArchitecture;
    }

    // For testing.
    // platformArchFn enables similating different architectures
    // without extensive mocking on the client to simulate the nodes info response.
    // TODO make package private once the elser service is moved to the Elasticsearch
    // service package.
    public BaseElasticsearchInternalService(
        InferenceServiceExtension.InferenceServiceFactoryContext context,
        Consumer<ActionListener<Set<String>>> platformArchFn
    ) {
        this.client = new OriginSettingClient(context.client(), ClientHelper.INFERENCE_ORIGIN);
        this.inferenceExecutor = context.threadPool().executor(InferencePlugin.UTILITY_THREAD_POOL_NAME);
        this.platformArch = platformArchFn;
    }

    /**
     * The task types supported by the service
     * @return Set of supported.
     */
    protected abstract EnumSet<TaskType> supportedTaskTypes();

    @Override
    public void start(Model model, ActionListener<Boolean> finalListener) {
        if (model instanceof ElasticsearchInternalModel esModel) {
            if (supportedTaskTypes().contains(model.getTaskType()) == false) {
                finalListener.onFailure(
                    new IllegalStateException(TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()))
                );
                return;
            }

            SubscribableListener.<Boolean>newForked(forkedListener -> { isBuiltinModelPut(model, forkedListener); })
                .<Boolean>andThen((l, modelConfigExists) -> {
                    if (modelConfigExists == false) {
                        putModel(model, l);
                    } else {
                        l.onResponse(true);
                    }
                })
                .<Boolean>andThen((l2, modelDidPut) -> {
                    var startRequest = esModel.getStartTrainedModelDeploymentActionRequest();
                    var responseListener = esModel.getCreateTrainedModelAssignmentActionListener(model, finalListener);
                    client.execute(StartTrainedModelDeploymentAction.INSTANCE, startRequest, responseListener);
                })
                .addListener(finalListener);

        } else {
            finalListener.onFailure(notElasticsearchModelException(model));
        }
    }

    @Override
    public void stop(String inferenceEntityId, ActionListener<Boolean> listener) {
        var request = new StopTrainedModelDeploymentAction.Request(inferenceEntityId);
        request.setForce(true);
        client.execute(
            StopTrainedModelDeploymentAction.INSTANCE,
            request,
            listener.delegateFailureAndWrap((delegatedResponseListener, response) -> delegatedResponseListener.onResponse(Boolean.TRUE))
        );
    }

    protected static IllegalStateException notElasticsearchModelException(Model model) {
        return new IllegalStateException(
            "Error starting model, [" + model.getConfigurations().getInferenceEntityId() + "] is not an Elasticsearch service model"
        );
    }

    @Override
    public void putModel(Model model, ActionListener<Boolean> listener) {
        if (model instanceof ElasticsearchInternalModel == false) {
            listener.onFailure(notElasticsearchModelException(model));
            return;
        } else if (model instanceof MultilingualE5SmallModel e5Model) {
            putBuiltInModel(e5Model.getServiceSettings().modelId(), listener);
        } else if (model instanceof ElserInternalModel elserModel) {
            putBuiltInModel(elserModel.getServiceSettings().modelId(), listener);
        } else if (model instanceof CustomElandModel) {
            logger.info("Custom eland model detected, model must have been already loaded into the cluster with eland.");
            listener.onResponse(Boolean.TRUE);
        } else {
            listener.onFailure(
                new IllegalArgumentException(
                    "Can not download model automatically for ["
                        + model.getConfigurations().getInferenceEntityId()
                        + "] you may need to download it through the trained models API or with eland."
                )
            );
            return;
        }
    }

    protected void putBuiltInModel(String modelId, ActionListener<Boolean> listener) {
        var input = new TrainedModelInput(List.<String>of("text_field")); // by convention text_field is used
        var config = TrainedModelConfig.builder().setInput(input).setModelId(modelId).validate(true).build();
        PutTrainedModelAction.Request putRequest = new PutTrainedModelAction.Request(config, false, true);
        executeAsyncWithOrigin(
            client,
            INFERENCE_ORIGIN,
            PutTrainedModelAction.INSTANCE,
            putRequest,
            ActionListener.wrap(response -> listener.onResponse(Boolean.TRUE), e -> {
                if (e instanceof ElasticsearchStatusException esException
                    && esException.getMessage().contains(PutTrainedModelAction.MODEL_ALREADY_EXISTS_ERROR_MESSAGE_FRAGMENT)) {
                    listener.onResponse(Boolean.TRUE);
                } else {
                    listener.onFailure(e);
                }
            })
        );
    }

    protected void isBuiltinModelPut(Model model, ActionListener<Boolean> listener) {
        ActionListener<GetTrainedModelsAction.Response> getModelsResponseListener = ActionListener.wrap(response -> {
            if (response.getResources().count() < 1) {
                listener.onResponse(Boolean.FALSE);
            } else {
                listener.onResponse(Boolean.TRUE);
            }
        }, exception -> {
            if (exception instanceof ResourceNotFoundException) {
                listener.onResponse(Boolean.FALSE);
            } else {
                listener.onFailure(exception);
            }
        });

        if (model instanceof ElasticsearchInternalModel == false) {
            listener.onFailure(notElasticsearchModelException(model));
        } else if (model.getServiceSettings() instanceof ElasticsearchInternalServiceSettings internalServiceSettings) {
            String modelId = internalServiceSettings.modelId();
            GetTrainedModelsAction.Request getRequest = new GetTrainedModelsAction.Request(modelId);
            executeAsyncWithOrigin(client, INFERENCE_ORIGIN, GetTrainedModelsAction.INSTANCE, getRequest, getModelsResponseListener);
        } else {
            listener.onFailure(
                new IllegalStateException(
                    "Can not check the download status of the model used by ["
                        + model.getConfigurations().getInferenceEntityId()
                        + "] as the model_id cannot be found."
                )
            );
        }
    }

    @Override
    public void close() throws IOException {}

    public static String selectDefaultModelVariantBasedOnClusterArchitecture(
        Set<String> modelArchitectures,
        String linuxX86OptimisedModel,
        String platformAgnosticModel
    ) {
        // choose a default model version based on the cluster architecture
        boolean homogenous = modelArchitectures.size() == 1;
        if (homogenous && modelArchitectures.iterator().next().equals("linux-x86_64")) {
            // Use the hardware optimized model
            return linuxX86OptimisedModel;
        } else {
            // default to the platform-agnostic model
            return platformAgnosticModel;
        }
    }

    private void platformArchitecture(ActionListener<Set<String>> platformArchitectureListener) {
        // Find the cluster platform as the service may need that
        // information when creating the model
        MlPlatformArchitecturesUtil.getMlNodesArchitecturesSet(
            platformArchitectureListener.delegateFailureAndWrap((delegate, architectures) -> {
                if (architectures.isEmpty() && clusterIsInElasticCloud()) {
                    // In Elastic cloud ml nodes run on Linux x86
                    delegate.onResponse(Set.of("linux-x86_64"));
                } else {
                    delegate.onResponse(architectures);
                }
            }),
            client,
            inferenceExecutor
        );
    }

    static boolean clusterIsInElasticCloud() {
        // use a heuristic to determine if in Elastic cloud.
        return true; // TODO
    }

    public static InferModelAction.Request buildInferenceRequest(
        String id,
        InferenceConfigUpdate update,
        List<String> inputs,
        InputType inputType,
        TimeValue timeout,
        boolean chunk
    ) {
        var request = InferModelAction.Request.forTextInput(id, update, inputs, true, timeout);
        request.setPrefixType(
            InputType.SEARCH == inputType ? TrainedModelPrefixStrings.PrefixType.SEARCH : TrainedModelPrefixStrings.PrefixType.INGEST
        );
        request.setHighPriority(InputType.SEARCH == inputType);
        request.setChunked(chunk);
        return request;
    }

    protected abstract boolean isDefaultId(String inferenceId);

    protected void maybeStartDeployment(
        ElasticsearchInternalModel model,
        Exception e,
        InferModelAction.Request request,
        ActionListener<InferModelAction.Response> listener
    ) {
        if (DefaultElserFeatureFlag.isEnabled() == false) {
            listener.onFailure(e);
            return;
        }

        if (isDefaultId(model.getInferenceEntityId()) && ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
            this.start(
                model,
                listener.delegateFailureAndWrap((l, started) -> { client.execute(InferModelAction.INSTANCE, request, listener); })
            );
        } else {
            listener.onFailure(e);
        }
    }
}
