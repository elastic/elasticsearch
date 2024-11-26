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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings;

import java.io.IOException;
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

    private static final Logger logger = LogManager.getLogger(TransportUpdateInferenceModelAction.class);

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final Client client;

    @Inject
    public TransportUpdateInferenceModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        Client client,
        Settings settings
    ) {
        super(
            UpdateInferenceModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateInferenceModelAction.Request::new,
            indexNameExpressionResolver,
            UpdateInferenceModelAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.client = client;
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

        SubscribableListener.<UnparsedModel>newForked(listener -> { checkEndpointExists(inferenceEntityId, listener); })
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
                } else {
                    service.set(optionalService.get());
                    listener.onResponse(unparsedModel);
                }
            })
            .<Boolean>andThen((listener, existingUnparsedModel) -> {

                Model existingParsedModel = service.get()
                    .parsePersistedConfigWithSecrets(
                        request.getInferenceEntityId(),
                        existingUnparsedModel.taskType(),
                        new HashMap<>(existingUnparsedModel.settings()),
                        new HashMap<>(existingUnparsedModel.secrets())
                    );

                Model newModel = combineExistingModelWithNewSettings(
                    existingParsedModel,
                    request.getContentAsSettings(),
                    service.get().name(),
                    resolvedTaskType
                );

                if (isInClusterService(service.get().name())) {
                    updateInClusterEndpoint(request, newModel, existingParsedModel, listener);
                } else {
                    modelRegistry.updateModelTransaction(newModel, existingParsedModel, listener);
                }
            })
            .<ModelConfigurations>andThen((listener, didUpdate) -> {
                if (didUpdate) {
                    modelRegistry.getModel(inferenceEntityId, ActionListener.wrap((unparsedModel) -> {
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
                                    .parsePersistedConfig(
                                        request.getInferenceEntityId(),
                                        resolvedTaskType,
                                        new HashMap<>(unparsedModel.settings())
                                    )
                                    .getConfigurations()
                            );
                        }
                    }, listener::onFailure));
                } else {
                    listener.onFailure(new ElasticsearchStatusException("Failed to update model", RestStatus.INTERNAL_SERVER_ERROR));
                }

            }).<UpdateInferenceModelAction.Response>andThen((listener, modelConfig) -> {
                listener.onResponse(new UpdateInferenceModelAction.Response(modelConfig));
            })
            .addListener(masterListener);
    }

    /**
     * Combines the existing model with the new settings to create a new model using the
     * SecretSettings and TaskSettings implementations for each service, as well as specifically handling NUM_ALLOCATIONS.
     *
     * @param existingParsedModel the Model representing a third-party service endpoint
     * @param settingsToUpdate    new settings
     * @param serviceName
     * @return a new object representing the updated model
     */
    private Model combineExistingModelWithNewSettings(
        Model existingParsedModel,
        UpdateInferenceModelAction.Settings settingsToUpdate,
        String serviceName,
        TaskType resolvedTaskType
    ) {
        ModelConfigurations existingConfigs = existingParsedModel.getConfigurations();
        TaskSettings existingTaskSettings = existingConfigs.getTaskSettings();
        SecretSettings existingSecretSettings = existingParsedModel.getSecretSettings();

        SecretSettings newSecretSettings = existingSecretSettings;
        TaskSettings newTaskSettings = existingTaskSettings;
        ServiceSettings newServiceSettings = existingConfigs.getServiceSettings();

        if (settingsToUpdate.serviceSettings() != null && existingSecretSettings != null) {
            newSecretSettings = existingSecretSettings.newSecretSettings(settingsToUpdate.serviceSettings());
        }
        if (settingsToUpdate.serviceSettings() != null && settingsToUpdate.serviceSettings().containsKey(NUM_ALLOCATIONS)) {
            // In cluster services can only have their num_allocations updated, so this is a special case
            if (newServiceSettings instanceof ElasticsearchInternalServiceSettings elasticServiceSettings) {
                newServiceSettings = new ElasticsearchInternalServiceSettings(
                    elasticServiceSettings,
                    (Integer) settingsToUpdate.serviceSettings().get(NUM_ALLOCATIONS)
                );
            }
        }
        if (settingsToUpdate.taskSettings() != null && existingTaskSettings != null) {
            newTaskSettings = existingTaskSettings.updatedTaskSettings(settingsToUpdate.taskSettings());
        }

        if (existingParsedModel.getTaskType().equals(resolvedTaskType) == false) {
            throw new ElasticsearchStatusException("Task type must match the task type of the existing endpoint", RestStatus.BAD_REQUEST);
        }

        ModelConfigurations newModelConfigs = new ModelConfigurations(
            existingParsedModel.getInferenceEntityId(),
            existingParsedModel.getTaskType(),
            serviceName,
            newServiceSettings,
            newTaskSettings
        );

        return new Model(newModelConfigs, new ModelSecrets(newSecretSettings));
    }

    private void updateInClusterEndpoint(
        UpdateInferenceModelAction.Request request,
        Model newModel,
        Model existingParsedModel,
        ActionListener<Boolean> listener
    ) throws IOException {
        // The model we are trying to update must have a trained model associated with it if it is an in-cluster deployment
        throwIfTrainedModelDoesntExist(request);

        Map<String, Object> serviceSettings = request.getContentAsSettings().serviceSettings();
        if (serviceSettings != null && serviceSettings.get(NUM_ALLOCATIONS) instanceof Integer numAllocations) {

            UpdateTrainedModelDeploymentAction.Request updateRequest = new UpdateTrainedModelDeploymentAction.Request(
                request.getInferenceEntityId()
            );
            updateRequest.setNumberOfAllocations(numAllocations);

            var delegate = listener.<CreateTrainedModelAssignmentAction.Response>delegateFailure((l2, response) -> {
                modelRegistry.updateModelTransaction(newModel, existingParsedModel, l2);
            });

            logger.info(
                "Updating trained model deployment for inference entity [{}] with [{}] num_allocations",
                request.getInferenceEntityId(),
                numAllocations
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

    private void throwIfTrainedModelDoesntExist(UpdateInferenceModelAction.Request request) throws ElasticsearchStatusException {
        var assignments = TrainedModelAssignmentUtils.modelAssignments(request.getInferenceEntityId(), clusterService.state());
        if ((assignments == null || assignments.isEmpty())) {
            throw ExceptionsHelper.entityNotFoundException(
                Messages.MODEL_ID_DOES_NOT_MATCH_EXISTING_MODEL_IDS_BUT_MUST_FOR_IN_CLUSTER_SERVICE,
                request.getInferenceEntityId()

            );
        }
    }

    private void checkEndpointExists(String inferenceEntityId, ActionListener<UnparsedModel> listener) {
        modelRegistry.getModelWithSecrets(inferenceEntityId, ActionListener.wrap((model) -> {
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

    private static XContentParser getParser(UpdateInferenceModelAction.Request request) throws IOException {
        return XContentHelper.createParser(XContentParserConfiguration.EMPTY, request.getContent(), request.getContentType());
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateInferenceModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
