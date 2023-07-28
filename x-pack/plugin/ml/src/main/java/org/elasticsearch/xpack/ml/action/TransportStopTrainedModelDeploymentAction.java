/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentClusterService;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ml.action.TransportDeleteTrainedModelAction.getModelAliases;
import static org.elasticsearch.xpack.ml.action.TransportDeleteTrainedModelAction.getReferencedModelKeys;

/**
 * Class for transporting stop trained model deployment requests.
 *
 * NOTE: this class gets routed to each individual deployment running on the nodes. This way when the request returns, we are assured
 * that the model is not running any longer on any node.
 */
public class TransportStopTrainedModelDeploymentAction extends TransportTasksAction<
    TrainedModelDeploymentTask,
    StopTrainedModelDeploymentAction.Request,
    StopTrainedModelDeploymentAction.Response,
    StopTrainedModelDeploymentAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStopTrainedModelDeploymentAction.class);

    private final Client client;
    private final IngestService ingestService;
    private final TrainedModelAssignmentClusterService trainedModelAssignmentClusterService;
    private final InferenceAuditor auditor;

    @Inject
    public TransportStopTrainedModelDeploymentAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        IngestService ingestService,
        TrainedModelAssignmentClusterService trainedModelAssignmentClusterService,
        InferenceAuditor auditor
    ) {
        super(
            StopTrainedModelDeploymentAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            StopTrainedModelDeploymentAction.Request::new,
            StopTrainedModelDeploymentAction.Response::new,
            StopTrainedModelDeploymentAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.ingestService = ingestService;
        this.trainedModelAssignmentClusterService = trainedModelAssignmentClusterService;
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected void doExecute(
        Task task,
        StopTrainedModelDeploymentAction.Request request,
        ActionListener<StopTrainedModelDeploymentAction.Response> listener
    ) {
        ClusterState state = clusterService.state();
        DiscoveryNodes nodes = state.nodes();
        // Master node is required for initial pre-checks and deletion preparation
        if (nodes.isLocalNodeElectedMaster() == false) {
            redirectToMasterNode(nodes.getMasterNode(), request, listener);
            return;
        }

        logger.debug(() -> format("[%s] Received request to undeploy%s", request.getId(), request.isForce() ? " (force)" : ""));

        // TODO check for alias
        // TODO model or deployment?
        Optional<TrainedModelAssignment> maybeAssignment = TrainedModelAssignmentMetadata.assignmentForDeploymentId(
            clusterService.state(),
            request.getId()
        );

        if (maybeAssignment.isEmpty()) {
            if (request.isAllowNoMatch() == false) {
                listener.onFailure(ExceptionsHelper.missingModelDeployment(request.getId()));
            } else {
                listener.onResponse(new StopTrainedModelDeploymentAction.Response(true));
            }
            return;
        }

        IngestMetadata currentIngestMetadata = state.metadata().custom(IngestMetadata.TYPE);
        Set<String> referencedModels = getReferencedModelKeys(currentIngestMetadata, ingestService);

        if (request.isForce() == false) {
            if (referencedModels.contains(request.getId())) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cannot stop deployment [{}] as it is referenced by ingest processors; " + "use force to stop the deployment",
                        RestStatus.CONFLICT,
                        request.getId()
                    )
                );
                return;
            }
            List<String> modelAliases = getModelAliases(state, request.getId());
            Optional<String> referencedModelAlias = modelAliases.stream().filter(referencedModels::contains).findFirst();
            if (referencedModelAlias.isPresent()) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cannot stop deployment [{}] as it has a model_alias [{}] that is still referenced"
                            + " by ingest processors; use force to stop the deployment",
                        RestStatus.CONFLICT,
                        request.getId(),
                        referencedModelAlias.get()
                    )
                );
                return;
            }
        }

        // NOTE, should only run on Master node
        assert clusterService.localNode().isMasterNode();
        trainedModelAssignmentClusterService.setModelAssignmentToStopping(
            request.getId(),
            ActionListener.wrap(
                setToStopping -> normalUndeploy(task, request.getId(), maybeAssignment.get(), request, listener),
                failure -> {
                    if (ExceptionsHelper.unwrapCause(failure) instanceof ResourceNotFoundException) {
                        listener.onResponse(new StopTrainedModelDeploymentAction.Response(true));
                        return;
                    }
                    listener.onFailure(failure);
                }
            )
        );
    }

    private void redirectToMasterNode(
        DiscoveryNode masterNode,
        StopTrainedModelDeploymentAction.Request request,
        ActionListener<StopTrainedModelDeploymentAction.Response> listener
    ) {
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException());
        } else {
            transportService.sendRequest(
                masterNode,
                actionName,
                request,
                new ActionListenerResponseHandler<>(listener, StopTrainedModelDeploymentAction.Response::new)
            );
        }
    }

    private void normalUndeploy(
        Task task,
        String modelId,
        TrainedModelAssignment modelAssignment,
        StopTrainedModelDeploymentAction.Request request,
        ActionListener<StopTrainedModelDeploymentAction.Response> listener
    ) {
        request.setNodes(modelAssignment.getNodeRoutingTable().keySet().toArray(String[]::new));
        ActionListener<StopTrainedModelDeploymentAction.Response> finalListener = ActionListener.wrap(r -> {
            assert clusterService.localNode().isMasterNode();
            trainedModelAssignmentClusterService.removeModelAssignment(modelId, ActionListener.wrap(deleted -> {
                auditor.info(modelId, Messages.INFERENCE_DEPLOYMENT_STOPPED);
                listener.onResponse(r);
            }, deletionFailed -> {
                logger.error(
                    () -> format("[%s] failed to delete model assignment after nodes unallocated the deployment", modelId),
                    deletionFailed
                );
                listener.onFailure(
                    ExceptionsHelper.serverError(
                        "failed to delete model assignment after nodes unallocated the deployment. Attempt to stop again",
                        deletionFailed
                    )
                );
            }));
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof FailedNodeException) {
                // A node has dropped out of the cluster since we started executing the requests.
                // Since undeploying an already undeployed trained model is not an error we can try again.
                // The tasks that were running on the node that dropped out of the cluster
                // will just have their persistent tasks cancelled. Tasks that were stopped
                // by the previous attempt will be noops in the subsequent attempt.
                doExecute(task, request, listener);
            } else {
                listener.onFailure(e);
            }
        });
        super.doExecute(task, request, finalListener);
    }

    @Override
    protected StopTrainedModelDeploymentAction.Response newResponse(
        StopTrainedModelDeploymentAction.Request request,
        List<StopTrainedModelDeploymentAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (taskOperationFailures.isEmpty() == false) {
            throw ExceptionsHelper.taskOperationFailureToStatusException(taskOperationFailures.get(0));
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw failedNodeExceptions.get(0);
        } else {
            return new StopTrainedModelDeploymentAction.Response(true);
        }
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        StopTrainedModelDeploymentAction.Request request,
        TrainedModelDeploymentTask task,
        ActionListener<StopTrainedModelDeploymentAction.Response> listener
    ) {
        task.stop(
            "undeploy_trained_model (api)",
            ActionListener.wrap(r -> listener.onResponse(new StopTrainedModelDeploymentAction.Response(true)), listener::onFailure)
        );
    }
}
