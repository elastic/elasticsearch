/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.inference.InferenceLicenceCheck;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.validation.ModelValidatorBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.resolveTaskType;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS;

public class TransportUpdateInferenceModelAction extends TransportMasterNodeAction<
    UpdateInferenceModelAction.Request,
    UpdateInferenceModelAction.Response> {

    private static final Logger LOGGER = LogManager.getLogger(TransportUpdateInferenceModelAction.class);

    private final XPackLicenseState licenseState;
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final Client client;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportUpdateInferenceModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        Client client,
        ProjectResolver projectResolver
    ) {
        super(
            UpdateInferenceModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateInferenceModelAction.Request::new,
            UpdateInferenceModelAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseState = licenseState;
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.client = client;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateInferenceModelAction.Request request,
        ClusterState state,
        ActionListener<UpdateInferenceModelAction.Response> masterListener
    ) {
        var bodyTaskType = request.getContentAsSettings().taskType();
        var resolvedTaskType = resolveTaskType(request.getTaskType(), bodyTaskType != null ? bodyTaskType.toString() : null);

        AtomicReference<InferenceService> service = new AtomicReference<>();

        var inferenceEntityId = request.getInferenceEntityId();

        SubscribableListener.<UnparsedModel>newForked(listener -> checkEndpointExists(inferenceEntityId, listener))
            .<UnparsedModel>andThen((listener, unparsedModel) -> {

                Optional<InferenceService> optionalService = serviceRegistry.getService(unparsedModel.service());
                if (optionalService.isEmpty()) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Service [{}] not found",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            unparsedModel.service()
                        )
                    );
                    return;
                }

                if (InferenceLicenceCheck.isServiceLicenced(optionalService.get().name(), licenseState) == false) {
                    listener.onFailure(InferenceLicenceCheck.complianceException(optionalService.get().name()));
                    return;
                }

                service.set(optionalService.get());
                listener.onResponse(unparsedModel);
            })
            .<Boolean>andThen((listener, existingUnparsedModel) -> {

                Model existingParsedModel = service.get()
                    .parsePersistedConfigWithSecrets(
                        existingUnparsedModel.inferenceEntityId(),
                        existingUnparsedModel.taskType(),
                        new HashMap<>(existingUnparsedModel.settings()),
                        new HashMap<>(existingUnparsedModel.secrets())
                    );

                validateResolvedTaskType(existingParsedModel, resolvedTaskType);

                ModelConfigurations mergedModelConfigurations = combineExistingModelConfigurationsWithNewSettings(
                    existingParsedModel,
                    request.getContentAsSettings(),
                    service.get().name()
                );

                ModelSecrets mergedModelSecrets = combineExistingSecretsWithNewSecrets(
                    existingParsedModel,
                    request.getContentAsSettings().serviceSettings()
                );

                Model mergedParsedModel = service.get().buildModelFromConfigAndSecrets(mergedModelConfigurations, mergedModelSecrets);
                if (mergedParsedModel.equals(existingParsedModel)) {
                    // if there are no changes to the model, return early without updating
                    listener.onResponse(true);
                    return;
                }

                if (isInClusterService(service.get().name())) {
                    updateInClusterEndpoint(request, mergedParsedModel, existingParsedModel, listener);
                } else {
                    ActionListener<Model> updateModelListener = listener.delegateFailureAndWrap(
                        (delegate, verifiedModel) -> modelRegistry.updateModelTransaction(verifiedModel, existingParsedModel, delegate)
                    );
                    ModelValidatorBuilder.buildModelValidator(mergedParsedModel.getTaskType(), service.get())
                        .validate(service.get(), mergedParsedModel, TimeValue.THIRTY_SECONDS, updateModelListener);
                }
            })
            .<ModelConfigurations>andThen((listener, didUpdate) -> {
                if (didUpdate) {
                    modelRegistry.getModel(inferenceEntityId, ActionListener.wrap(unparsedModel -> {
                        if (unparsedModel == null) {
                            listener.onFailure(
                                new ElasticsearchStatusException(
                                    "Failed to update model, updated model not found",
                                    RestStatus.INTERNAL_SERVER_ERROR
                                )
                            );
                        } else {
                            listener.onResponse(
                                service.get()
                                    .parsePersistedConfig(inferenceEntityId, resolvedTaskType, new HashMap<>(unparsedModel.settings()))
                                    .getConfigurations()
                            );
                        }
                    }, listener::onFailure));
                } else {
                    listener.onFailure(new ElasticsearchStatusException("Failed to update model", RestStatus.INTERNAL_SERVER_ERROR));
                }

            }).<UpdateInferenceModelAction.Response>andThen(
                (listener, modelConfig) -> listener.onResponse(new UpdateInferenceModelAction.Response(modelConfig))
            )
            .addListener(masterListener);
    }

    protected static void validateResolvedTaskType(Model existingParsedModel, TaskType resolvedTaskType) {
        if (existingParsedModel.getTaskType().equals(resolvedTaskType) == false) {
            throw new ElasticsearchStatusException("Task type must match the task type of the existing endpoint", RestStatus.BAD_REQUEST);
        }
    }

    /**
     * Combines the existing model configurations with the new settings to create a new model configuration
     * @param existingParsedModel the Model representing a third-party service endpoint
     * @param newSettings  new settings to update
     * @param serviceName the name of the service
     * @return a new object representing the updated model configurations
     */
    protected ModelConfigurations combineExistingModelConfigurationsWithNewSettings(
        Model existingParsedModel,
        UpdateInferenceModelAction.Settings newSettings,
        String serviceName
    ) {
        ModelConfigurations existingConfigs = existingParsedModel.getConfigurations();
        TaskSettings existingTaskSettings = existingConfigs.getTaskSettings();
        ServiceSettings existingServiceSettings = existingConfigs.getServiceSettings();

        TaskSettings mergedTaskSettings = existingTaskSettings;
        ServiceSettings mergedServiceSettings = existingServiceSettings;

        if (newSettings.serviceSettings() != null) {
            mergedServiceSettings = mergedServiceSettings.updateServiceSettings(newSettings.serviceSettings());
        }
        if (newSettings.taskSettings() != null) {
            mergedTaskSettings = mergedTaskSettings.updatedTaskSettings(newSettings.taskSettings());
        }

        return new ModelConfigurations(
            existingParsedModel.getInferenceEntityId(),
            existingParsedModel.getTaskType(),
            serviceName,
            mergedServiceSettings,
            mergedTaskSettings,
            existingConfigs.getChunkingSettings()
        );
    }

    /**
     * Combines the existing model secrets with the new secrets to create a new model secrets
     * @param existingParsedModel the Model representing a third-party service endpoint
     * @param newSettingsMap new secrets to update
     * @return a new object representing the updated model secrets
     */
    protected ModelSecrets combineExistingSecretsWithNewSecrets(Model existingParsedModel, Map<String, Object> newSettingsMap) {
        SecretSettings existingSecretSettings = existingParsedModel.getSecretSettings();
        SecretSettings mergedSecretSettings = existingSecretSettings;

        if (newSettingsMap != null && existingSecretSettings != null) {
            mergedSecretSettings = existingSecretSettings.newSecretSettings(newSettingsMap);
        }

        return new ModelSecrets(mergedSecretSettings);
    }

    private void updateInClusterEndpoint(
        UpdateInferenceModelAction.Request request,
        Model newModel,
        Model existingParsedModel,
        ActionListener<Boolean> listener
    ) {
        // The model we are trying to update must have a trained model associated with it if it is an in-cluster deployment
        var deploymentId = getDeploymentIdForInClusterEndpoint(existingParsedModel);
        var inferenceEntityId = request.getInferenceEntityId();
        throwIfTrainedModelDoesntExist(inferenceEntityId, deploymentId);

        if (inferenceEntityId.equals(deploymentId) == false) {
            modelRegistry.getModel(deploymentId, ActionListener.wrap(unparsedModel -> {
                // if this deployment was created by another inference endpoint, then it must be updated using that inference endpoint
                listener.onFailure(
                    new ElasticsearchStatusException(
                        Messages.INFERENCE_REFERENCE_CANNOT_UPDATE_ANOTHER_ENDPOINT,
                        RestStatus.CONFLICT,
                        inferenceEntityId,
                        deploymentId,
                        unparsedModel.inferenceEntityId()
                    )
                );
            }, e -> {
                if (e instanceof ResourceNotFoundException) {
                    // if this deployment was created by the trained models API, then it must be updated by the trained models API
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            Messages.INFERENCE_CAN_ONLY_UPDATE_MODELS_IT_CREATED,
                            RestStatus.CONFLICT,
                            inferenceEntityId,
                            deploymentId
                        )
                    );
                    return;
                }
                listener.onFailure(e);
            }));
            return;
        }

        if (newModel.getServiceSettings() instanceof ElasticsearchInternalServiceSettings elasticServiceSettings) {

            var updateRequest = new UpdateTrainedModelDeploymentAction.Request(deploymentId);
            updateRequest.setNumberOfAllocations(elasticServiceSettings.getNumAllocations());
            updateRequest.setAdaptiveAllocationsSettings(elasticServiceSettings.getAdaptiveAllocationsSettings());
            updateRequest.setSource(UpdateTrainedModelDeploymentAction.Request.Source.INFERENCE_API);

            var delegate = listener.<CreateTrainedModelAssignmentAction.Response>delegateFailure(
                (l2, response) -> modelRegistry.updateModelTransaction(newModel, existingParsedModel, l2)
            );

            LOGGER.info(
                "Updating trained model deployment [{}] for inference entity [{}] with [{}] num_allocations and adaptive allocations [{}]",
                deploymentId,
                inferenceEntityId,
                elasticServiceSettings.getNumAllocations(),
                elasticServiceSettings.getAdaptiveAllocationsSettings()
            );
            client.execute(UpdateTrainedModelDeploymentAction.INSTANCE, updateRequest, delegate);

        } else {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Failed to parse [{}] of update request [{}]",
                    RestStatus.BAD_REQUEST,
                    NUM_ALLOCATIONS,
                    request.getContent().utf8ToString()
                )
            );
        }

    }

    private boolean isInClusterService(String name) {
        return List.of(ElasticsearchInternalService.NAME, ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME).contains(name);
    }

    private String getDeploymentIdForInClusterEndpoint(Model model) {
        if (model instanceof ElasticsearchInternalModel esModel) {
            return esModel.mlNodeDeploymentId();
        } else {
            throw new IllegalStateException(
                Strings.format(
                    "Cannot update inference endpoint [%s]. Class [%s] is not an Elasticsearch internal model",
                    model.getInferenceEntityId(),
                    model.getClass().getSimpleName()
                )
            );
        }
    }

    private void throwIfTrainedModelDoesntExist(String inferenceEntityId, String deploymentId) throws ElasticsearchStatusException {
        var assignments = TrainedModelAssignmentUtils.modelAssignments(deploymentId, clusterService.state());
        if ((assignments == null || assignments.isEmpty())) {
            throw ExceptionsHelper.entityNotFoundException(
                Messages.MODEL_ID_DOES_NOT_MATCH_EXISTING_MODEL_IDS_BUT_MUST_FOR_IN_CLUSTER_SERVICE,
                inferenceEntityId
            );
        }
    }

    private void checkEndpointExists(String inferenceEntityId, ActionListener<UnparsedModel> listener) {
        modelRegistry.getModelWithSecrets(inferenceEntityId, ActionListener.wrap(model -> {
            if (model == null) {
                listener.onFailure(
                    ExceptionsHelper.entityNotFoundException(Messages.INFERENCE_ENTITY_NON_EXISTANT_NO_UPDATE, inferenceEntityId)
                );
            } else {
                listener.onResponse(model);
            }
        }, e -> {
            if (e instanceof ResourceNotFoundException) {
                listener.onFailure(
                    // provide a more specific error message if the inference entity does not exist
                    ExceptionsHelper.entityNotFoundException(Messages.INFERENCE_ENTITY_NON_EXISTANT_NO_UPDATE, inferenceEntityId)
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateInferenceModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
