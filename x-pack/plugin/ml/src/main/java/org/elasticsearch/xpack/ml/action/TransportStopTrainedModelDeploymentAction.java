/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationMetadata;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationService;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Class for transporting stop trained model deloyment requests.
 *
 * NOTE: this class gets routed to each individual deployment running on the nodes. This way when the request returns, we are assured
 * that the model is not running any longer on any node.
 */
public class TransportStopTrainedModelDeploymentAction extends TransportTasksAction<TrainedModelDeploymentTask,
    StopTrainedModelDeploymentAction.Request, StopTrainedModelDeploymentAction.Response, StopTrainedModelDeploymentAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStopTrainedModelDeploymentAction.class);

    private final Client client;
    private final TrainedModelAllocationService trainedModelAllocationService;

    @Inject
    public TransportStopTrainedModelDeploymentAction(ClusterService clusterService, TransportService transportService,
                                                     ActionFilters actionFilters, Client client, ThreadPool threadPool,
                                                     TrainedModelAllocationService trainedModelAllocationService) {
        super(StopTrainedModelDeploymentAction.NAME, clusterService, transportService, actionFilters,
            StopTrainedModelDeploymentAction.Request::new, StopTrainedModelDeploymentAction.Response::new,
            StopTrainedModelDeploymentAction.Response::new, ThreadPool.Names.SAME);
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.trainedModelAllocationService = trainedModelAllocationService;
    }

    @Override
    protected void doExecute(Task task, StopTrainedModelDeploymentAction.Request request,
                             ActionListener<StopTrainedModelDeploymentAction.Response> listener) {
        ClusterState state = clusterService.state();
        DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster() == false) {
            redirectToMasterNode(nodes.getMasterNode(), request, listener);
            return;
        }

        logger.debug("[{}] Received request to undeploy", request.getId());

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(
            getModelsResponse -> {
                List<TrainedModelConfig> models = getModelsResponse.getResources().results();
                if (models.isEmpty()) {
                    listener.onResponse(new StopTrainedModelDeploymentAction.Response(true));
                    return;
                }
                if (models.size() > 1) {
                    listener.onFailure(ExceptionsHelper.badRequestException("cannot undeploy multiple models at the same time"));
                    return;
                }

                Optional<TrainedModelAllocation> maybeAllocation = TrainedModelAllocationMetadata.allocationForModelId(
                    clusterService.state(),
                    models.get(0).getModelId()
                );

                if (maybeAllocation.isEmpty()) {
                    listener.onResponse(new StopTrainedModelDeploymentAction.Response(true));
                    return;
                }
                final String modelId = models.get(0).getModelId();
                trainedModelAllocationService.stopModelAllocation(modelId, ActionListener.wrap(
                    response -> normalUndeploy(task, models.get(0).getModelId(), maybeAllocation.get(), request, listener),
                    failure -> {
                        if (ExceptionsHelper.unwrapCause(failure) instanceof ResourceNotFoundException) {
                            listener.onResponse(new StopTrainedModelDeploymentAction.Response(true));
                            return;
                        }
                        listener.onFailure(failure);
                    }
                ));
            },
            listener::onFailure
        );

        GetTrainedModelsAction.Request getModelRequest = new GetTrainedModelsAction.Request(
            request.getId(), null, Collections.emptySet());
        getModelRequest.setAllowNoResources(request.isAllowNoMatch());
        client.execute(GetTrainedModelsAction.INSTANCE, getModelRequest, getModelListener);
    }

    private void redirectToMasterNode(DiscoveryNode masterNode, StopTrainedModelDeploymentAction.Request request,
                                      ActionListener<StopTrainedModelDeploymentAction.Response> listener) {
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException());
        } else {
            transportService.sendRequest(masterNode, actionName, request,
                new ActionListenerResponseHandler<>(listener, StopTrainedModelDeploymentAction.Response::new));
        }
    }

    private void normalUndeploy(Task task,
                                String modelId,
                                TrainedModelAllocation modelAllocation,
                                StopTrainedModelDeploymentAction.Request request,
                                ActionListener<StopTrainedModelDeploymentAction.Response> listener) {
        request.setNodes(modelAllocation.getNodeRoutingTable().keySet().toArray(String[]::new));
        ActionListener<StopTrainedModelDeploymentAction.Response> finalListener = ActionListener.wrap(
            r -> {
                waitForTaskRemoved(modelId, modelAllocation, request, r, ActionListener.wrap(
                    waited -> {
                        trainedModelAllocationService.deleteModelAllocation(
                            modelId,
                            ActionListener.wrap(
                                deleted -> listener.onResponse(r),
                                deletionFailed -> {
                                    logger.error(
                                        () -> new ParameterizedMessage(
                                            "[{}] failed to delete model allocation after nodes unallocated the deployment",
                                            modelId
                                        ),deletionFailed);
                                    listener.onFailure(ExceptionsHelper.serverError(
                                        "failed to delete model allocation after nodes unallocated the deployment. Attempt to stop again",
                                        deletionFailed
                                    ));
                                }
                            )
                        );
                    },
                    // TODO should we attempt to delete the deployment here?
                    listener::onFailure
                ));

            },
            e -> {
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
            }
        );
        super.doExecute(task, request, finalListener);
    }

    void waitForTaskRemoved(String modelId,
                            TrainedModelAllocation trainedModelAllocation,
                            StopTrainedModelDeploymentAction.Request request,
                            StopTrainedModelDeploymentAction.Response response,
                            ActionListener<StopTrainedModelDeploymentAction.Response> listener) {
        final Set<String> nodesOfConcern = trainedModelAllocation.getNodeRoutingTable().keySet();
        client.admin()
            .cluster()
            .prepareListTasks(nodesOfConcern.toArray(String[]::new))
            .setDetailed(true)
            .setWaitForCompletion(true)
            .setActions(modelId)
            .setTimeout(request.getTimeout())
            .execute(ActionListener.wrap(
                complete -> listener.onResponse(response),
                listener::onFailure
            ));
    }

    @Override
    protected StopTrainedModelDeploymentAction.Response newResponse(StopTrainedModelDeploymentAction.Request request,
                                                                    List<StopTrainedModelDeploymentAction.Response> tasks,
                                                                    List<TaskOperationFailure> taskOperationFailures,
                                                                    List<FailedNodeException> failedNodeExceptions) {
        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        } else {
            return new StopTrainedModelDeploymentAction.Response(true);
        }
    }

    @Override
    protected void taskOperation(StopTrainedModelDeploymentAction.Request request, TrainedModelDeploymentTask task,
                                 ActionListener<StopTrainedModelDeploymentAction.Response> listener) {
        task.stop("undeploy_trained_model (api)");
        listener.onResponse(new StopTrainedModelDeploymentAction.Response(true));
    }
}
