/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.UndeployTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.deployment.DeployTrainedModelState;
import org.elasticsearch.xpack.core.ml.inference.deployment.DeployTrainedModelTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.deployment.DeployTrainedModelTask;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TransportUndeployTrainedModelAction extends TransportTasksAction<DeployTrainedModelTask, UndeployTrainedModelAction.Request,
    UndeployTrainedModelAction.Response, UndeployTrainedModelAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportUndeployTrainedModelAction.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportUndeployTrainedModelAction(String actionName, ClusterService clusterService, TransportService transportService,
                                               ActionFilters actionFilters, Client client, ThreadPool threadPool,
                                               PersistentTasksService persistentTasksService) {
        super(actionName, clusterService, transportService, actionFilters, UndeployTrainedModelAction.Request::new,
            UndeployTrainedModelAction.Response::new, UndeployTrainedModelAction.Response::new, ThreadPool.Names.SAME);
        this.client = client;
        this.threadPool = threadPool;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, UndeployTrainedModelAction.Request request,
                             ActionListener<UndeployTrainedModelAction.Response> listener) {
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
                    listener.onResponse(new UndeployTrainedModelAction.Response(true));
                    return;
                }
                if (models.size() > 1) {
                    listener.onFailure(ExceptionsHelper.badRequestException("cannot undeploy multiple models at the same time"));
                    return;
                }

                ClusterState clusterState = clusterService.state();
                PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
                PersistentTasksCustomMetadata.PersistentTask<?> deployTrainedModelTask =
                    MlTasks.getDeployTrainedModelTask(request.getId(), tasks);
                if (deployTrainedModelTask == null) {
                    listener.onResponse(new UndeployTrainedModelAction.Response(true));
                    return;
                }
                normalUndeploy(task, deployTrainedModelTask, request, listener);
            },
            listener::onFailure
        );

        GetTrainedModelsAction.Request getModelRequest = new GetTrainedModelsAction.Request(
            request.getId(), null, Collections.emptySet());
        getModelRequest.setAllowNoResources(request.isAllowNoMatch());
        client.execute(GetTrainedModelsAction.INSTANCE, getModelRequest, getModelListener);
    }

    private void redirectToMasterNode(DiscoveryNode masterNode, UndeployTrainedModelAction.Request request,
                                      ActionListener<UndeployTrainedModelAction.Response> listener) {
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException());
        } else {
            transportService.sendRequest(masterNode, actionName, request,
                new ActionListenerResponseHandler<>(listener, UndeployTrainedModelAction.Response::new));
        }
    }

    private void normalUndeploy(Task task, PersistentTasksCustomMetadata.PersistentTask<?> deployTrainedModelTask,
                                UndeployTrainedModelAction.Request request, ActionListener<UndeployTrainedModelAction.Response> listener) {
        request.setNodes(deployTrainedModelTask.getExecutorNode());

        ActionListener<UndeployTrainedModelAction.Response> finalListener = ActionListener.wrap(
            r -> waitForTaskRemoved(Collections.singleton(deployTrainedModelTask.getId()), request, r, listener),
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

    void waitForTaskRemoved(Set<String> taskIds, UndeployTrainedModelAction.Request request,
                            UndeployTrainedModelAction.Response response,
                            ActionListener<UndeployTrainedModelAction.Response> listener) {
        persistentTasksService.waitForPersistentTasksCondition(persistentTasks ->
                persistentTasks.findTasks(MlTasks.DEPLOY_TRAINED_MODEL_TASK_NAME, t -> taskIds.contains(t.getId())).isEmpty(),
            request.getTimeout(), ActionListener.wrap(
                booleanResponse -> {
                    listener.onResponse(response);
                },
                listener::onFailure
            )
        );
    }

    @Override
    protected UndeployTrainedModelAction.Response newResponse(UndeployTrainedModelAction.Request request,
                                                              List<UndeployTrainedModelAction.Response> tasks,
                                                              List<TaskOperationFailure> taskOperationFailures,
                                                              List<FailedNodeException> failedNodeExceptions) {
        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        } else {
            return new UndeployTrainedModelAction.Response(true);
        }
    }

    @Override
    protected void taskOperation(UndeployTrainedModelAction.Request request, DeployTrainedModelTask task,
                                 ActionListener<UndeployTrainedModelAction.Response> listener) {
        DeployTrainedModelTaskState undeployingState = new DeployTrainedModelTaskState(
            DeployTrainedModelState.UNDEPLOYING, task.getAllocationId(), "api");
        task.updatePersistentTaskState(undeployingState, ActionListener.wrap(
            updatedTask -> {
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        task.stop("undeploy_trained_model (api)");
                        listener.onResponse(new UndeployTrainedModelAction.Response(true));
                    }
                });
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    // the task has disappeared so must have stopped
                    listener.onResponse(new UndeployTrainedModelAction.Response(true));
                } else {
                    listener.onFailure(e);
                }
            }
        ));
    }
}
