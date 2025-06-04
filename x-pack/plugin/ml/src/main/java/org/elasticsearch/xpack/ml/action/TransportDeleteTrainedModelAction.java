/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.InferenceProcessorInfoExtractor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ml.utils.TaskRetriever.getDownloadTaskInfo;

/**
 * The action is a master node action to ensure it reads an up-to-date cluster
 * state in order to determine if there is a processor referencing the trained model
 */
public class TransportDeleteTrainedModelAction extends AcknowledgedTransportMasterNodeAction<DeleteTrainedModelAction.Request> {
    private static final Logger logger = LogManager.getLogger(TransportDeleteTrainedModelAction.class);

    private final Client client;
    private final TrainedModelProvider trainedModelProvider;
    private final InferenceAuditor auditor;
    private final IngestService ingestService;

    @Inject
    public TransportDeleteTrainedModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        ActionFilters actionFilters,
        TrainedModelProvider configProvider,
        InferenceAuditor auditor,
        IngestService ingestService
    ) {
        super(
            DeleteTrainedModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteTrainedModelAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.trainedModelProvider = configProvider;
        this.ingestService = ingestService;
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteTrainedModelAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug(() -> format("[%s] Request to delete trained model%s", request.getId(), request.isForce() ? " (force)" : ""));

        String id = request.getId();
        cancelDownloadTask(
            client,
            id,
            listener.delegateFailureAndWrap((l, ignored) -> deleteModel(request, state, l)),
            request.ackTimeout()
        );
    }

    // package-private for testing
    static void cancelDownloadTask(Client client, String modelId, ActionListener<ListTasksResponse> listener, TimeValue timeout) {
        logger.debug(() -> format("[%s] Checking if download task exists and cancelling it", modelId));

        OriginSettingClient mlClient = new OriginSettingClient(client, ML_ORIGIN);

        ActionListener<TaskInfo> taskListener = ActionListener.wrap(
            taskInfo -> executeTaskCancellation(mlClient, modelId, taskInfo, listener, timeout),
            e -> listener.onFailure(
                new ElasticsearchStatusException(
                    "Unable to retrieve existing task information for model id [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e,
                    modelId
                )
            )
        );

        // setting waitForCompletion to false here so that we don't block waiting for an existing task to complete before returning it
        getDownloadTaskInfo(mlClient, modelId, false, timeout, () -> null, taskListener);
    }

    static List<String> getModelAliases(ClusterState clusterState, String modelId) {
        final ModelAliasMetadata currentMetadata = ModelAliasMetadata.fromState(clusterState);
        final List<String> modelAliases = new ArrayList<>();
        for (Map.Entry<String, ModelAliasMetadata.ModelAliasEntry> modelAliasEntry : currentMetadata.modelAliases().entrySet()) {
            if (modelAliasEntry.getValue().getModelId().equals(modelId)) {
                modelAliases.add(modelAliasEntry.getKey());
            }
        }
        return modelAliases;
    }

    private void deleteModel(DeleteTrainedModelAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        String id = request.getId();
        IngestMetadata currentIngestMetadata = state.metadata().getProject().custom(IngestMetadata.TYPE);
        Set<String> referencedModels = InferenceProcessorInfoExtractor.getModelIdsFromInferenceProcessors(currentIngestMetadata);

        if (request.isForce() == false && referencedModels.contains(id)) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot delete model [{}] as it is still referenced by ingest processors; use force to delete the model",
                    RestStatus.CONFLICT,
                    id
                )
            );
            return;
        }

        final List<String> modelAliases = getModelAliases(state, id);
        if (request.isForce() == false) {
            Optional<String> referencedModelAlias = modelAliases.stream().filter(referencedModels::contains).findFirst();
            if (referencedModelAlias.isPresent()) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cannot delete model [{}] as it has a model_alias [{}] that is still referenced by ingest processors;"
                            + " use force to delete the model",
                        RestStatus.CONFLICT,
                        id,
                        referencedModelAlias.get()
                    )
                );
                return;
            }
        }

        if (TrainedModelAssignmentMetadata.fromState(state).modelIsDeployed(request.getId())) {
            if (request.isForce()) {
                forceStopDeployment(
                    request.getId(),
                    listener.delegateFailureAndWrap((l, stopDeploymentResponse) -> deleteAliasesAndModel(request, modelAliases, l))
                );
            } else {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cannot delete model [{}] as it is currently deployed; use force to delete the model",
                        RestStatus.CONFLICT,
                        id
                    )
                );
            }
        } else {
            deleteAliasesAndModel(request, modelAliases, listener);
        }
    }

    private void forceStopDeployment(String modelId, ActionListener<StopTrainedModelDeploymentAction.Response> listener) {
        StopTrainedModelDeploymentAction.Request request = new StopTrainedModelDeploymentAction.Request(modelId);
        request.setForce(true);
        ClientHelper.executeAsyncWithOrigin(client, ML_ORIGIN, StopTrainedModelDeploymentAction.INSTANCE, request, listener);
    }

    private void deleteAliasesAndModel(
        DeleteTrainedModelAction.Request request,
        List<String> modelAliases,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug(() -> "[" + request.getId() + "] Deleting model");

        ActionListener<AcknowledgedResponse> nameDeletionListener = listener.delegateFailureAndWrap(
            (delegate, ack) -> trainedModelProvider.deleteTrainedModel(request.getId(), delegate.delegateFailureAndWrap((l, r) -> {
                auditor.info(request.getId(), "trained model deleted");
                l.onResponse(AcknowledgedResponse.TRUE);
            }))
        );

        // No reason to update cluster state, simply delete the model
        if (modelAliases.isEmpty()) {
            nameDeletionListener.onResponse(AcknowledgedResponse.of(true));
            return;
        }

        submitUnbatchedTask("delete-trained-model-alias", new AckedClusterStateUpdateTask(request, nameDeletionListener) {
            @Override
            public ClusterState execute(final ClusterState currentState) {
                final ClusterState.Builder builder = ClusterState.builder(currentState);
                final ModelAliasMetadata currentMetadata = ModelAliasMetadata.fromState(currentState);
                if (currentMetadata.modelAliases().isEmpty()) {
                    return currentState;
                }
                final Map<String, ModelAliasMetadata.ModelAliasEntry> newMetadata = new HashMap<>(currentMetadata.modelAliases());
                logger.info("[{}] delete model model_aliases {}", request.getId(), modelAliases);
                modelAliases.forEach(newMetadata::remove);
                final ModelAliasMetadata modelAliasMetadata = new ModelAliasMetadata(newMetadata);
                builder.metadata(
                    Metadata.builder(currentState.getMetadata()).putCustom(ModelAliasMetadata.NAME, modelAliasMetadata).build()
                );
                return builder.build();
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private static void executeTaskCancellation(
        Client client,
        String modelId,
        TaskInfo taskInfo,
        ActionListener<ListTasksResponse> listener,
        TimeValue timeout
    ) {
        if (taskInfo != null) {
            ActionListener<ListTasksResponse> cancelListener = ActionListener.wrap(listener::onResponse, e -> {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof ResourceNotFoundException) {
                    logger.debug(() -> format("[%s] Task no longer exists when attempting to cancel it", modelId));
                    listener.onResponse(null);
                } else {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Unable to cancel task for model id [{}]",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e,
                            modelId
                        )
                    );
                }
            });

            logger.debug(() -> format("[%s] Download task exists, cancelling it", modelId));

            // setting waitForCompletion here to wait for the cancellation to complete before executing the listener
            client.admin()
                .cluster()
                .prepareCancelTasks()
                .setTargetTaskId(taskInfo.taskId())
                .setTimeout(timeout)
                .waitForCompletion(true)
                .execute(cancelListener);
        } else {
            logger.debug(() -> format("[%s] No download task exists, proceeding with deletion", modelId));
            listener.onResponse(null);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteTrainedModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
