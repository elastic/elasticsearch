/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class TrainedModelAllocationNodeService implements ClusterStateListener {

    private static final String TASK_NAME = "trained_model_allocation";
    private static final TimeValue MODEL_LOADING_CHECK_INTERVAL = TimeValue.timeValueSeconds(1);
    private static final Logger logger = LogManager.getLogger(TrainedModelAllocationNodeService.class);
    private final TrainedModelAllocationService trainedModelAllocationService;
    private final AtomicLong taskIdGenerator = new AtomicLong();
    private final DeploymentManager deploymentManager;
    private final TaskManager taskManager;
    private final Map<String, TrainedModelDeploymentTask> modelIdToTask;
    private final ThreadPool threadPool;
    private final Deque<TrainedModelDeploymentTask> loadingModels;
    private volatile Scheduler.Cancellable scheduledFuture;
    private volatile boolean stopped;
    private volatile String nodeId;

    public TrainedModelAllocationNodeService(
        TrainedModelAllocationService trainedModelAllocationService,
        ClusterService clusterService,
        DeploymentManager deploymentManager,
        TaskManager taskManager,
        ThreadPool threadPool
    ) {
        this.trainedModelAllocationService = trainedModelAllocationService;
        this.deploymentManager = deploymentManager;
        this.taskManager = taskManager;
        this.modelIdToTask = new ConcurrentHashMap<>();
        this.loadingModels = new ConcurrentLinkedDeque<>();
        this.threadPool = threadPool;
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                nodeId = clusterService.localNode().getId();
                start();
            }

            @Override
            public void beforeStop() {
                stop();
            }
        });
    }

    TrainedModelAllocationNodeService(
        TrainedModelAllocationService trainedModelAllocationService,
        ClusterService clusterService,
        DeploymentManager deploymentManager,
        TaskManager taskManager,
        ThreadPool threadPool,
        String nodeId
    ) {
        this.trainedModelAllocationService = trainedModelAllocationService;
        this.deploymentManager = deploymentManager;
        this.taskManager = taskManager;
        this.modelIdToTask = new ConcurrentHashMap<>();
        this.loadingModels = new ConcurrentLinkedDeque<>();
        this.threadPool = threadPool;
        this.nodeId = nodeId;
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                start();
            }

            @Override
            public void beforeStop() {
                stop();
            }
        });
    }

    void stopDeployment(TrainedModelDeploymentTask task) {
        if (stopped) {
            return;
        }
        deploymentManager.stopDeployment(task);
        taskManager.unregister(task);
        modelIdToTask.remove(task.getModelId());
    }

    void stopDeploymentAsync(TrainedModelDeploymentTask task, ActionListener<Void> listener) {
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                stopDeployment(task);
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public void start() {
        stopped = false;
        scheduledFuture = threadPool.scheduleWithFixedDelay(
            this::loadQueuedModels,
            MODEL_LOADING_CHECK_INTERVAL,
            MachineLearning.UTILITY_THREAD_POOL_NAME
        );
    }

    public void stop() {
        stopped = true;
        ThreadPool.Cancellable cancellable = this.scheduledFuture;
        if (cancellable != null) {
            cancellable.cancel();
        }
    }

    void loadQueuedModels() {
        TrainedModelDeploymentTask loadingTask;
        logger.trace("attempting to load all currently queued models");
        // NOTE: As soon as this method exits, the timer for the scheduler starts ticking
        while ((loadingTask = loadingModels.poll()) != null) {
            if (loadingTask.isStopped()) {
                continue;
            }
            if (stopped) {
                return;
            }
            final String modelId = loadingTask.getModelId();
            logger.trace(() -> new ParameterizedMessage("[{}] attempting to load model", modelId));
            final PlainActionFuture<TrainedModelDeploymentTask> listener = new PlainActionFuture<>();
            deploymentManager.startDeployment(loadingTask, listener);
            try {
                // This needs to be synchronous here in the utility thread to keep queueing order
                TrainedModelDeploymentTask deployedTask = listener.actionGet();
                // kicks off asynchronous cluster state update
                handleLoadSuccess(deployedTask);
            } catch (Exception ex) {
                // kicks off asynchronous cluster state update
                handleLoadFailure(loadingTask, ex);
            }
        }
    }

    public void stopDeploymentAndNotify(TrainedModelDeploymentTask task) {
        ActionListener<Void> notifyDeploymentOfStopped = ActionListener.wrap(
            stopped -> updateStoredState(
                task.getModelId(),
                new RoutingStateAndReason(RoutingState.STOPPED, ""),
                ActionListener.wrap(s -> {}, failure -> {})
            ),
            failed -> { // if we failed to stop the process, something strange is going on, but we should still notify of stop
                logger.warn(() -> new ParameterizedMessage("[{}] failed to stop due to error", task.getModelId()), failed);
                updateStoredState(
                    task.getModelId(),
                    new RoutingStateAndReason(RoutingState.STOPPED, ""),
                    ActionListener.wrap(s -> {}, failure -> {})
                );
            }
        );
        updateStoredState(
            task.getModelId(),
            new RoutingStateAndReason(RoutingState.STOPPING, "task locally canceled"),
            ActionListener.wrap(success -> stopDeploymentAsync(task, notifyDeploymentOfStopped), e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "[{}] failed to set routing state to stopping as allocation already removed",
                            task.getModelId()
                        ),
                        e
                    );
                } else {
                    // this is an unexpected error
                    // TODO this means requests may still be routed here, should we not stop deployment?
                    logger.warn(
                        () -> new ParameterizedMessage("[{}] failed to set routing state to stopping due to error", task.getModelId()),
                        e
                    );
                }
                stopDeploymentAsync(task, notifyDeploymentOfStopped);
            })
        );
    }

    public void infer(TrainedModelDeploymentTask task, String input, TimeValue timeout, ActionListener<InferenceResults> listener) {
        deploymentManager.infer(task, input, timeout, listener);
    }

    private TaskAwareRequest taskAwareRequest(StartTrainedModelDeploymentAction.TaskParams params) {
        final TrainedModelAllocationNodeService trainedModelAllocationNodeService = this;
        return new TaskAwareRequest() {
            final TaskId parentTaskId = new TaskId(nodeId, taskIdGenerator.incrementAndGet());

            @Override
            public void setParentTask(TaskId taskId) {
                throw new UnsupportedOperationException("parent task id for model allocation tasks shouldn't change");
            }

            @Override
            public TaskId getParentTask() {
                return parentTaskId;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new TrainedModelDeploymentTask(id, type, action, parentTaskId, headers, params, trainedModelAllocationNodeService);
            }
        };
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metadataChanged()) {
            TrainedModelAllocationMetadata modelAllocationMetadata = TrainedModelAllocationMetadata.metadata(event.state());
            final String currentNode = event.state().nodes().getLocalNodeId();
            for (TrainedModelAllocation trainedModelAllocation : modelAllocationMetadata.modelAllocations().values()) {
                RoutingStateAndReason routingStateAndReason = trainedModelAllocation.getNodeRoutingTable().get(currentNode);
                // Add new models to start loading
                if (routingStateAndReason != null
                    // periodic retries should be handled in a separate thread think
                    && routingStateAndReason.getState().equals(RoutingState.INITIALIZING)
                    // This means we don't already have a task and should attempt creating one and starting the model loading
                    && modelIdToTask.containsKey(trainedModelAllocation.getTaskParams().getModelId()) == false) {
                    prepareModelToLoad(trainedModelAllocation.getTaskParams());
                }
                // This mode is not routed to the current node at all
                if (routingStateAndReason == null) {
                    TrainedModelDeploymentTask task = modelIdToTask.remove(trainedModelAllocation.getTaskParams().getModelId());
                    if (task != null) {
                        task.stopWithoutNotification("node no longer referenced in model routing table");
                        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> stopDeployment(task));
                    }
                }
            }
            List<TrainedModelDeploymentTask> toCancel = new ArrayList<>();
            for (String modelIds : Sets.difference(modelIdToTask.keySet(), modelAllocationMetadata.modelAllocations().keySet())) {
                toCancel.add(modelIdToTask.remove(modelIds));
            }
            // should all be stopped in the same executor thread?
            for (TrainedModelDeploymentTask t : toCancel) {
                t.stopWithoutNotification("model allocation no longer exists");
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> stopDeployment(t));
            }
        }
    }

    // For testing purposes
    TrainedModelDeploymentTask getTask(String modelId) {
        return modelIdToTask.get(modelId);
    }

    void prepareModelToLoad(StartTrainedModelDeploymentAction.TaskParams taskParams) {
        TrainedModelDeploymentTask task = (TrainedModelDeploymentTask) taskManager.register(
            TASK_NAME,
            taskParams.getModelId(),
            taskAwareRequest(taskParams)
        );
        // threadsafe check to verify we are not loading/loaded the model
        if (modelIdToTask.putIfAbsent(taskParams.getModelId(), task) == null) {
            loadingModels.add(task);
        } else {
            // If there is already a task for the model, unregister the new task
            taskManager.unregister(task);
        }
    }

    private void handleLoadSuccess(TrainedModelDeploymentTask task) {
        final String modelId = task.getModelId();
        logger.debug(
            () -> new ParameterizedMessage("[{}] model successfully loaded and ready for inference. Notifying master node", modelId)
        );
        if (task.isStopped()) {
            logger.debug(
                () -> new ParameterizedMessage("[{}] model loaded successfully, but stopped before routing table was updated", modelId)
            );
            return;
        }
        updateStoredState(
            modelId,
            new RoutingStateAndReason(RoutingState.STARTED, ""),
            ActionListener.wrap(
                r -> logger.debug(() -> new ParameterizedMessage("[{}] model loaded and accepting routes", modelId)),
                e -> {
                    // This means that either the allocation has been deleted, or this node's particular route has been removed
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "[{}] model loaded but failed to start accepting routes as allocation to this node was removed",
                                modelId
                            ),
                            e
                        );
                    }
                    // this is an unexpected error
                    logger.warn(() -> new ParameterizedMessage("[{}] model loaded but failed to start accepting routes", modelId), e);
                }
            )
        );
    }

    private void updateStoredState(
        String modelId,
        RoutingStateAndReason routingStateAndReason,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (stopped) {
            return;
        }
        trainedModelAllocationService.updateModelAllocationState(
            new UpdateTrainedModelAllocationStateAction.Request(nodeId, modelId, routingStateAndReason),
            ActionListener.wrap(success -> {
                logger.debug(
                    () -> new ParameterizedMessage("[{}] model is [{}] and master notified", modelId, routingStateAndReason.getState())
                );
                listener.onResponse(AcknowledgedResponse.TRUE);
            },
                error -> {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "[{}] model is [{}] but failed to notify master",
                            modelId,
                            routingStateAndReason.getState()
                        ),
                        error
                    );
                    listener.onFailure(error);
                }
            )
        );
    }

    private void handleLoadFailure(TrainedModelDeploymentTask task, Exception ex) {
        logger.error(() -> new ParameterizedMessage("[{}] model failed to load", task.getModelId()), ex);
        if (task.isStopped()) {
            logger.debug(() -> new ParameterizedMessage("[{}] model failed to load, but is now stopped", task.getModelId()));
        }
        // TODO: Do we want to remove from the modelIdToTask map? This would cause it to be reloaded by state updates on INITIALIZING
        modelIdToTask.remove(task.getModelId());
        updateStoredState(
            task.getModelId(),
            new RoutingStateAndReason(RoutingState.FAILED, ExceptionsHelper.unwrapCause(ex).getMessage()),
            ActionListener.wrap(r -> {}, e -> {})
        );
    }
}
