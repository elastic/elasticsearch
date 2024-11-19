/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentRoutingInfoAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfoUpdate;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.ModelStats;
import org.elasticsearch.xpack.ml.inference.deployment.NlpInferenceInput;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.MlTasks.TRAINED_MODEL_ASSIGNMENT_TASK_ACTION;
import static org.elasticsearch.xpack.core.ml.MlTasks.TRAINED_MODEL_ASSIGNMENT_TASK_TYPE;
import static org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils.NODE_IS_SHUTTING_DOWN;
import static org.elasticsearch.xpack.ml.MachineLearning.ML_PYTORCH_MODEL_INFERENCE_FEATURE;

public class TrainedModelAssignmentNodeService implements ClusterStateListener {

    private static final String NODE_NO_LONGER_REFERENCED = "node no longer referenced in model routing table";
    private static final String ASSIGNMENT_NO_LONGER_EXISTS = "deployment assignment no longer exists";
    private static final TimeValue MODEL_LOADING_CHECK_INTERVAL = TimeValue.timeValueSeconds(1);
    private static final TimeValue CONTROL_MESSAGE_TIMEOUT = TimeValue.timeValueSeconds(60);
    private static final Logger logger = LogManager.getLogger(TrainedModelAssignmentNodeService.class);
    private final TrainedModelAssignmentService trainedModelAssignmentService;
    private final DeploymentManager deploymentManager;
    private final TaskManager taskManager;
    private final Map<String, TrainedModelDeploymentTask> deploymentIdToTask;
    private final ThreadPool threadPool;
    private final Deque<TrainedModelDeploymentTask> loadingModels;
    private final XPackLicenseState licenseState;
    private final IndexNameExpressionResolver expressionResolver;
    private volatile Scheduler.Cancellable scheduledFuture;
    private volatile ClusterState latestState;
    private volatile boolean stopped;
    private volatile String nodeId;

    public TrainedModelAssignmentNodeService(
        TrainedModelAssignmentService trainedModelAssignmentService,
        ClusterService clusterService,
        DeploymentManager deploymentManager,
        IndexNameExpressionResolver expressionResolver,
        TaskManager taskManager,
        ThreadPool threadPool,
        XPackLicenseState licenseState
    ) {
        this.trainedModelAssignmentService = trainedModelAssignmentService;
        this.deploymentManager = deploymentManager;
        this.taskManager = taskManager;
        this.deploymentIdToTask = new ConcurrentHashMap<>();
        this.loadingModels = new ConcurrentLinkedDeque<>();
        this.threadPool = threadPool;
        this.licenseState = licenseState;
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
        this.expressionResolver = expressionResolver;
    }

    TrainedModelAssignmentNodeService(
        TrainedModelAssignmentService trainedModelAssignmentService,
        ClusterService clusterService,
        DeploymentManager deploymentManager,
        IndexNameExpressionResolver expressionResolver,
        TaskManager taskManager,
        ThreadPool threadPool,
        String nodeId,
        XPackLicenseState licenseState
    ) {
        this.trainedModelAssignmentService = trainedModelAssignmentService;
        this.deploymentManager = deploymentManager;
        this.taskManager = taskManager;
        this.deploymentIdToTask = new ConcurrentHashMap<>();
        this.loadingModels = new ConcurrentLinkedDeque<>();
        this.threadPool = threadPool;
        this.nodeId = nodeId;
        this.licenseState = licenseState;
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
        this.expressionResolver = expressionResolver;
    }

    void start() {
        stopped = false;
        schedule(false);
    }

    private void schedule(boolean runImmediately) {
        if (stopped) {
            // do not schedule when stopped
            return;
        }

        var rescheduleListener = ActionListener.wrap(this::schedule, e -> this.schedule(false));
        Runnable loadQueuedModels = () -> loadQueuedModels(rescheduleListener);
        var executor = threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME);

        if (runImmediately) {
            executor.execute(loadQueuedModels);
        } else {
            scheduledFuture = threadPool.schedule(loadQueuedModels, MODEL_LOADING_CHECK_INTERVAL, executor);
        }
    }

    void stop() {
        stopped = true;
        ThreadPool.Cancellable cancellable = this.scheduledFuture;
        if (cancellable != null) {
            cancellable.cancel();
        }
    }

    void loadQueuedModels(ActionListener<Boolean> rescheduleImmediately) {
        if (stopped) {
            rescheduleImmediately.onResponse(false);
            return;
        }
        if (latestState != null) {
            List<String> unassignedIndices = AbstractJobPersistentTasksExecutor.verifyIndicesPrimaryShardsAreActive(
                latestState,
                expressionResolver,
                // we allow missing as that means the index doesn't exist at all and our loading will fail for the models and we need
                // to notify as necessary
                true,
                InferenceIndexConstants.INDEX_PATTERN,
                InferenceIndexConstants.nativeDefinitionStore()
            );
            if (unassignedIndices.size() > 0) {
                logger.trace("not loading models as indices {} primary shards are unassigned", unassignedIndices);
                rescheduleImmediately.onResponse(false);
                return;
            }
        }

        var loadingTask = loadingModels.poll();
        if (loadingTask == null) {
            rescheduleImmediately.onResponse(false);
            return;
        }

        loadModel(loadingTask, ActionListener.wrap(retry -> {
            if (retry != null && retry) {
                loadingModels.offer(loadingTask);
                // don't reschedule immediately if the next task is the one we just queued, instead wait a bit to retry
                rescheduleImmediately.onResponse(loadingModels.peek() != loadingTask);
            } else {
                rescheduleImmediately.onResponse(loadingModels.isEmpty() == false);
            }
        }, e -> rescheduleImmediately.onResponse(loadingModels.isEmpty() == false)));
    }

    void loadModel(TrainedModelDeploymentTask loadingTask, ActionListener<Boolean> retryListener) {
        if (loadingTask.isStopped()) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "[{}] attempted to load stopped task with reason [{}]",
                    loadingTask.getDeploymentId(),
                    loadingTask.stoppedReason().orElse("_unknown_")
                );
            }
            retryListener.onResponse(false);
            return;
        }
        SubscribableListener.<TrainedModelDeploymentTask>newForked(l -> deploymentManager.startDeployment(loadingTask, l))
            .andThen(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME), threadPool.getThreadContext(), this::handleLoadSuccess)
            .addListener(retryListener.delegateResponse((retryL, ex) -> {
                var deploymentId = loadingTask.getDeploymentId();
                logger.warn(() -> "[" + deploymentId + "] Start deployment failed", ex);
                if (ExceptionsHelper.unwrapCause(ex) instanceof ResourceNotFoundException) {
                    var modelId = loadingTask.getParams().getModelId();
                    logger.debug(() -> "[" + deploymentId + "] Start deployment failed as model [" + modelId + "] was not found", ex);
                    handleLoadFailure(loadingTask, ExceptionsHelper.missingTrainedModel(modelId, ex), retryL);
                } else if (ExceptionsHelper.unwrapCause(ex) instanceof SearchPhaseExecutionException) {
                    /*
                     * This case will not catch the ElasticsearchException generated from the ChunkedTrainedModelRestorer in a scenario
                     * where the maximum number of retries for a SearchPhaseExecutionException or CBE occur. This is intentional. If the
                     * retry logic fails after retrying we should return the error and not retry here. The generated
                     * ElasticsearchException will contain the SearchPhaseExecutionException or CBE but cannot be unwrapped.
                     */
                    logger.debug(() -> "[" + deploymentId + "] Start deployment failed, will retry", ex);
                    // A search phase execution failure should be retried, push task back to the queue

                    // This will cause the entire model to be reloaded (all the chunks)
                    retryL.onResponse(true);
                } else {
                    handleLoadFailure(loadingTask, ex, retryL);
                }
            }), threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME), threadPool.getThreadContext());
    }

    public void gracefullyStopDeploymentAndNotify(
        TrainedModelDeploymentTask task,
        String reason,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug(() -> format("[%s] Gracefully stopping deployment due to reason %s", task.getDeploymentId(), reason));

        stopAndNotifyHelper(task, reason, listener, deploymentManager::stopAfterCompletingPendingWork);
    }

    public void stopDeploymentAndNotify(TrainedModelDeploymentTask task, String reason, ActionListener<AcknowledgedResponse> listener) {
        logger.debug(() -> format("[%s] Forcefully stopping deployment due to reason %s", task.getDeploymentId(), reason));

        stopAndNotifyHelper(task, reason, listener, deploymentManager::stopDeployment);
    }

    private void stopAndNotifyHelper(
        TrainedModelDeploymentTask task,
        String reason,
        ActionListener<AcknowledgedResponse> listener,
        Consumer<TrainedModelDeploymentTask> stopDeploymentFunc
    ) {
        // Removing the entry from the map to avoid the possibility of a node shutdown triggering a concurrent graceful stopping of the
        // process while we are attempting to forcefully stop the native process
        // The graceful stopping will only occur if there is an entry in the map
        deploymentIdToTask.remove(task.getDeploymentId());
        ActionListener<Void> notifyDeploymentOfStopped = updateRoutingStateToStoppedListener(task.getDeploymentId(), reason, listener);

        updateStoredState(
            task.getDeploymentId(),
            RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.STOPPING, reason)),
            ActionListener.wrap(success -> stopDeploymentHelper(task, reason, stopDeploymentFunc, notifyDeploymentOfStopped), e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    logger.debug(
                        () -> format("[%s] failed to set routing state to stopping as assignment already removed", task.getDeploymentId()),
                        e
                    );
                } else {
                    // this is an unexpected error
                    // TODO this means requests may still be routed here, should we not stop deployment?
                    logger.warn(() -> "[" + task.getDeploymentId() + "] failed to set routing state to stopping due to error", e);
                }
                stopDeploymentHelper(task, reason, stopDeploymentFunc, notifyDeploymentOfStopped);
            })
        );
    }

    public void infer(
        TrainedModelDeploymentTask task,
        InferenceConfig config,
        NlpInferenceInput input,
        boolean skipQueue,
        TimeValue timeout,
        TrainedModelPrefixStrings.PrefixType prefixType,
        CancellableTask parentActionTask,
        boolean chunkResponse,
        ActionListener<InferenceResults> listener
    ) {
        deploymentManager.infer(task, config, input, skipQueue, timeout, prefixType, parentActionTask, chunkResponse, listener);
    }

    public Optional<ModelStats> modelStats(TrainedModelDeploymentTask task) {
        return deploymentManager.getStats(task);
    }

    public void clearCache(TrainedModelDeploymentTask task, ActionListener<AcknowledgedResponse> listener) {
        deploymentManager.clearCache(task, CONTROL_MESSAGE_TIMEOUT, listener);
    }

    private TaskAwareRequest taskAwareRequest(StartTrainedModelDeploymentAction.TaskParams params) {
        final TrainedModelAssignmentNodeService trainedModelAssignmentNodeService = this;
        return new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {
                throw new UnsupportedOperationException("parent task id for model deployment tasks shouldn't change");
            }

            @Override
            public void setRequestId(long requestId) {
                throw new UnsupportedOperationException("does not have request ID");
            }

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new TrainedModelDeploymentTask(
                    id,
                    type,
                    action,
                    parentTaskId,
                    headers,
                    params,
                    trainedModelAssignmentNodeService,
                    licenseState,
                    ML_PYTORCH_MODEL_INFERENCE_FEATURE
                );
            }
        };
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        latestState = event.state();
        if (event.metadataChanged() == false) {
            return;
        }

        final boolean isResetMode = MlMetadata.getMlMetadata(event.state()).isResetMode();
        TrainedModelAssignmentMetadata modelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(event.state());
        final String currentNode = event.state().nodes().getLocalNodeId();
        final boolean isNewAllocationSupported = event.state()
            .getMinTransportVersion()
            .onOrAfter(TrainedModelAssignmentClusterService.DISTRIBUTED_MODEL_ALLOCATION_TRANSPORT_VERSION);
        final Set<String> shuttingDownNodes = Collections.unmodifiableSet(event.state().metadata().nodeShutdowns().getAllNodeIds());

        if (isResetMode == false && isNewAllocationSupported) {
            updateNumberOfAllocations(modelAssignmentMetadata);
        }

        for (TrainedModelAssignment trainedModelAssignment : modelAssignmentMetadata.allAssignments().values()) {
            RoutingInfo routingInfo = trainedModelAssignment.getNodeRoutingTable().get(currentNode);
            if (routingInfo != null) {
                // Add new models to start loading if the assignment is not stopping
                if (isNewAllocationSupported && trainedModelAssignment.getAssignmentState() != AssignmentState.STOPPING) {
                    if (shouldAssignmentBeRestarted(routingInfo, trainedModelAssignment.getDeploymentId())) {
                        prepareAssignmentForRestart(trainedModelAssignment);
                    }

                    if (shouldLoadModel(routingInfo, trainedModelAssignment.getDeploymentId(), isResetMode)) {
                        StartTrainedModelDeploymentAction.TaskParams params = createStartTrainedModelDeploymentTaskParams(
                            trainedModelAssignment,
                            routingInfo.getCurrentAllocations()
                        );
                        // Loading the model is done by a separate task, so needs a new trace context
                        try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                            prepareModelToLoad(params);
                        }
                    }
                }

                /*
                 * Check if this is a shutting down node and if we can gracefully shut down the native process after draining its queues
                 */
                if (shouldGracefullyShutdownDeployment(trainedModelAssignment, shuttingDownNodes, currentNode)) {
                    gracefullyStopDeployment(trainedModelAssignment.getDeploymentId(), currentNode);
                }
            } else {
                stopUnreferencedDeployment(trainedModelAssignment.getDeploymentId(), currentNode);
            }
        }

        List<TrainedModelDeploymentTask> toCancel = new ArrayList<>();
        for (String deploymentIds : Sets.difference(deploymentIdToTask.keySet(), modelAssignmentMetadata.allAssignments().keySet())) {
            toCancel.add(deploymentIdToTask.remove(deploymentIds));
        }
        // should all be stopped in the same executor thread?
        for (TrainedModelDeploymentTask t : toCancel) {
            stopDeploymentAsync(
                t,
                ASSIGNMENT_NO_LONGER_EXISTS,
                ActionListener.wrap(
                    r -> logger.trace(() -> "[" + t.getDeploymentId() + "] stopped deployment"),
                    e -> logger.warn(() -> "[" + t.getDeploymentId() + "] failed to fully stop deployment", e)
                )
            );
        }
    }

    private boolean shouldAssignmentBeRestarted(RoutingInfo routingInfo, String deploymentId) {
        return routingInfo.getState() == RoutingState.STARTING
            && deploymentIdToTask.containsKey(deploymentId)
            && deploymentIdToTask.get(deploymentId).isFailed();
    }

    private void prepareAssignmentForRestart(TrainedModelAssignment trainedModelAssignment) {
        // This is a failed assignment and we are restarting it. For this we need to remove the task first.
        taskManager.unregister(deploymentIdToTask.get(trainedModelAssignment.getDeploymentId()));
        deploymentIdToTask.remove(trainedModelAssignment.getDeploymentId());
    }

    private boolean shouldLoadModel(RoutingInfo routingInfo, String deploymentId, boolean isResetMode) {
        return routingInfo.getState().isAnyOf(RoutingState.STARTING, RoutingState.STARTED) // periodic retries of `failed`
            // should
            // be handled in a separate process
            // This means we don't already have a task and should attempt creating one and starting the model loading
            // If we don't have a task but are STARTED, this means the cluster state had a started assignment,
            // the node crashed and then started again
            && deploymentIdToTask.containsKey(deploymentId) == false
            // If we are in reset mode, don't start loading a new model on this node.
            && isResetMode == false;
    }

    private static StartTrainedModelDeploymentAction.TaskParams createStartTrainedModelDeploymentTaskParams(
        TrainedModelAssignment trainedModelAssignment,
        int currentAllocations
    ) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            trainedModelAssignment.getTaskParams().getModelId(),
            trainedModelAssignment.getDeploymentId(),
            trainedModelAssignment.getTaskParams().getModelBytes(),
            currentAllocations,
            trainedModelAssignment.getTaskParams().getThreadsPerAllocation(),
            trainedModelAssignment.getTaskParams().getQueueCapacity(),
            trainedModelAssignment.getTaskParams().getCacheSize().orElse(null),
            trainedModelAssignment.getTaskParams().getPriority(),
            trainedModelAssignment.getTaskParams().getPerDeploymentMemoryBytes(),
            trainedModelAssignment.getTaskParams().getPerAllocationMemoryBytes()
        );
    }

    private boolean shouldGracefullyShutdownDeployment(
        TrainedModelAssignment trainedModelAssignment,
        Set<String> shuttingDownNodes,
        String currentNode
    ) {
        RoutingInfo routingInfo = trainedModelAssignment.getNodeRoutingTable().get(currentNode);

        if (routingInfo == null) {
            return true;
        }

        boolean isCurrentNodeShuttingDown = shuttingDownNodes.contains(currentNode);
        boolean isRouteStopping = routingInfo.getState() == RoutingState.STOPPING;
        boolean hasDeploymentTask = deploymentIdToTask.containsKey(trainedModelAssignment.getDeploymentId());
        boolean hasStartedRoutes = trainedModelAssignment.hasStartedRoutes();
        boolean assignmentIsRoutedToOneOrFewerNodes = trainedModelAssignment.getNodeRoutingTable().size() <= 1;

        // To avoid spamming the logs we'll only print these if we meet the base criteria
        if (isCurrentNodeShuttingDown && isRouteStopping && hasDeploymentTask) {
            logger.debug(
                () -> format(
                    "[%s] Checking if deployment can be gracefully shutdown on node %s, "
                        + "has other started routes: %s, "
                        + "single or no routed nodes: %s",
                    trainedModelAssignment.getDeploymentId(),
                    currentNode,
                    hasStartedRoutes,
                    assignmentIsRoutedToOneOrFewerNodes
                )
            );
        }

        // the current node is shutting down
        return isCurrentNodeShuttingDown
            // the route is marked as ready to shut down during a rebalance
            && isRouteStopping
            // the deployment wasn't already being stopped by a stop deployment API call
            && hasDeploymentTask
            // the assignment has another allocation that can serve any additional requests or the shutting down node is the only node that
            // serves this model (maybe the other available nodes are already full or no other ML nodes exist) in which case we can't wait
            // for another node to become available so allow a graceful shutdown
            && (hasStartedRoutes || assignmentIsRoutedToOneOrFewerNodes);
    }

    private void gracefullyStopDeployment(String deploymentId, String currentNode) {
        logger.debug(() -> format("[%s] Gracefully stopping deployment for shutting down node %s", deploymentId, currentNode));

        TrainedModelDeploymentTask task = deploymentIdToTask.remove(deploymentId);
        if (task == null) {
            logger.debug(
                () -> format(
                    "[%s] Unable to gracefully stop deployment for shutting down node %s because task does not exit",
                    deploymentId,
                    currentNode
                )
            );
            return;
        }

        ActionListener<AcknowledgedResponse> routingStateListener = ActionListener.wrap(
            r -> logger.debug(
                () -> format("[%s] Gracefully stopped deployment for shutting down node %s", task.getDeploymentId(), currentNode)
            ),
            e -> logger.error(
                () -> format("[%s] Failed to gracefully stop deployment for shutting down node %s", task.getDeploymentId(), currentNode),
                e
            )
        );

        ActionListener<Void> notifyDeploymentOfStopped = updateRoutingStateToStoppedListener(
            task.getDeploymentId(),
            NODE_IS_SHUTTING_DOWN,
            routingStateListener
        );

        stopDeploymentAfterCompletingPendingWorkAsync(task, notifyDeploymentOfStopped);
    }

    private ActionListener<Void> updateRoutingStateToStoppedListener(
        String deploymentId,
        String reason,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final RoutingInfoUpdate updateToStopped = RoutingInfoUpdate.updateStateAndReason(
            new RoutingStateAndReason(RoutingState.STOPPED, reason)
        );

        return ActionListener.wrap(_void -> {
            logger.debug(() -> format("[%s] Updating routing state to stopped", deploymentId));
            updateStoredState(deploymentId, updateToStopped, listener);
        }, e -> {
            // if we failed to stop the process, something strange is going on, but we should set the routing state to stopped
            logger.warn(() -> format("[%s] Failed to stop deployment due to error", deploymentId), e);
            updateStoredState(deploymentId, updateToStopped, listener);
        });
    }

    private void stopUnreferencedDeployment(String deploymentId, String currentNode) {
        // This model is not routed to the current node at all
        TrainedModelDeploymentTask task = deploymentIdToTask.remove(deploymentId);
        if (task == null) {
            return;
        }

        logger.debug(() -> format("[%s] Stopping unreferenced deployment for node %s", deploymentId, currentNode));
        stopDeploymentAsync(
            task,
            NODE_NO_LONGER_REFERENCED,
            ActionListener.wrap(
                r -> logger.trace(() -> "[" + task.getDeploymentId() + "] stopped deployment"),
                e -> logger.warn(() -> "[" + task.getDeploymentId() + "] failed to fully stop deployment", e)
            )
        );
    }

    private void stopDeploymentAsync(TrainedModelDeploymentTask task, String reason, ActionListener<Void> listener) {
        stopDeploymentHelper(task, reason, deploymentManager::stopDeployment, listener);
    }

    private void stopDeploymentHelper(
        TrainedModelDeploymentTask task,
        String reason,
        Consumer<TrainedModelDeploymentTask> stopDeploymentFunc,
        ActionListener<Void> listener
    ) {
        if (stopped) {
            return;
        }
        task.markAsStopped(reason);

        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                stopDeploymentFunc.accept(task);
                taskManager.unregister(task);
                deploymentIdToTask.remove(task.getDeploymentId());
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void stopDeploymentAfterCompletingPendingWorkAsync(TrainedModelDeploymentTask task, ActionListener<Void> listener) {
        stopDeploymentHelper(task, NODE_IS_SHUTTING_DOWN, deploymentManager::stopAfterCompletingPendingWork, listener);
    }

    private void updateNumberOfAllocations(TrainedModelAssignmentMetadata assignments) {
        List<TrainedModelAssignment> assignmentsToUpdate = assignments.allAssignments()
            .values()
            .stream()
            .filter(a -> hasStartingAssignments(a) == false)
            .filter(a -> a.isRoutedToNode(nodeId))
            .filter(a -> {
                RoutingInfo routingInfo = a.getNodeRoutingTable().get(nodeId);
                return routingInfo.getState() == RoutingState.STARTED
                    && routingInfo.getCurrentAllocations() != routingInfo.getTargetAllocations();
            })
            .toList();

        for (TrainedModelAssignment assignment : assignmentsToUpdate) {
            TrainedModelDeploymentTask task = deploymentIdToTask.get(assignment.getDeploymentId());
            if (task == null) {
                logger.debug(() -> format("[%s] task was removed whilst updating number of allocations", assignment.getDeploymentId()));
                continue;
            }
            RoutingInfo routingInfo = assignment.getNodeRoutingTable().get(nodeId);
            deploymentManager.updateNumAllocations(
                task,
                assignment.getNodeRoutingTable().get(nodeId).getTargetAllocations(),
                CONTROL_MESSAGE_TIMEOUT,
                ActionListener.wrap(threadSettings -> {
                    logger.debug(
                        "[{}] Updated number of allocations to [{}]",
                        assignment.getDeploymentId(),
                        threadSettings.numAllocations()
                    );
                    task.updateNumberOfAllocations(threadSettings.numAllocations());
                    updateStoredState(
                        assignment.getDeploymentId(),
                        RoutingInfoUpdate.updateNumberOfAllocations(threadSettings.numAllocations()),
                        ActionListener.noop()
                    );
                },
                    e -> logger.error(
                        format(
                            "[%s] Could not update number of allocations to [%s]",
                            assignment.getDeploymentId(),
                            routingInfo.getTargetAllocations()
                        ),
                        e
                    )
                )
            );
        }
    }

    private static boolean hasStartingAssignments(TrainedModelAssignment assignment) {
        return assignment.getNodeRoutingTable()
            .values()
            .stream()
            .anyMatch(routingInfo -> routingInfo.getState().isAnyOf(RoutingState.STARTING));
    }

    // For testing purposes
    TrainedModelDeploymentTask getTask(String deploymentId) {
        return deploymentIdToTask.get(deploymentId);
    }

    void prepareModelToLoad(StartTrainedModelDeploymentAction.TaskParams taskParams) {
        logger.debug(
            () -> format(
                "[%s] preparing to load model [%s] with task params: %s",
                taskParams.getDeploymentId(),
                taskParams.getModelId(),
                taskParams
            )
        );
        TrainedModelDeploymentTask task = (TrainedModelDeploymentTask) taskManager.register(
            TRAINED_MODEL_ASSIGNMENT_TASK_TYPE,
            TRAINED_MODEL_ASSIGNMENT_TASK_ACTION,
            taskAwareRequest(taskParams),
            false
        );
        // threadsafe check to verify we are not loading/loaded the model
        if (deploymentIdToTask.putIfAbsent(taskParams.getDeploymentId(), task) == null) {
            loadingModels.offer(task);
        } else {
            // If there is already a task for the deployment, unregister the new task
            taskManager.unregister(task);
        }
    }

    private void handleLoadSuccess(ActionListener<Boolean> retryListener, TrainedModelDeploymentTask task) {
        logger.debug(
            () -> "["
                + task.getParams().getDeploymentId()
                + "] model ["
                + task.getParams().getModelId()
                + "] successfully loaded and ready for inference. Notifying master node"
        );
        if (task.isStopped()) {
            logger.debug(
                () -> format(
                    "[%s] model [%s] loaded successfully, but stopped before routing table was updated; reason [%s]",
                    task.getDeploymentId(),
                    task.getParams().getModelId(),
                    task.stoppedReason().orElse("_unknown_")
                )
            );
            retryListener.onResponse(false);
            return;
        }

        updateStoredState(
            task.getDeploymentId(),
            RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.STARTED, "")),
            ActionListener.runAfter(ActionListener.wrap(r -> {
                logger.debug(() -> "[" + task.getDeploymentId() + "] model loaded and accepting routes");
            }, e -> {
                // This means that either the assignment has been deleted, or this node's particular route has been removed
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    logger.debug(
                        () -> format(
                            "[%s] model [%s] loaded but failed to start accepting routes as assignment to this node was removed",
                            task.getDeploymentId(),
                            task.getParams().getModelId()
                        ),
                        e
                    );
                } else {
                    // this is an unexpected error
                    logger.warn(
                        () -> "["
                            + task.getDeploymentId()
                            + "] model ["
                            + task.getParams().getModelId()
                            + "] loaded but failed to start accepting routes",
                        e
                    );
                }
            }), () -> retryListener.onResponse(false))
        );
    }

    private void updateStoredState(String deploymentId, RoutingInfoUpdate update, ActionListener<AcknowledgedResponse> listener) {
        if (stopped) {
            return;
        }
        trainedModelAssignmentService.updateModelAssignmentState(
            new UpdateTrainedModelAssignmentRoutingInfoAction.Request(nodeId, deploymentId, update),
            ActionListener.wrap(success -> {
                logger.debug(() -> format("[%s] deployment routing info was updated with [%s] and master notified", deploymentId, update));
                listener.onResponse(AcknowledgedResponse.TRUE);
            }, error -> {
                logger.warn(() -> format("[%s] failed to update deployment routing info with [%s]", deploymentId, update), error);
                listener.onFailure(error);
            })
        );
    }

    private void handleLoadFailure(TrainedModelDeploymentTask task, Exception ex, ActionListener<Boolean> retryListener) {
        if (ex instanceof ElasticsearchException esEx && esEx.status().getStatus() < 500) {
            logger.warn(() -> "[" + task.getDeploymentId() + "] model [" + task.getParams().getModelId() + "] failed to load", ex);
        } else {
            logger.error(() -> "[" + task.getDeploymentId() + "] model [" + task.getParams().getModelId() + "] failed to load", ex);
        }
        if (task.isStopped()) {
            logger.debug(
                () -> format(
                    "[%s] model [" + task.getParams().getModelId() + "] failed to load, but is now stopped; reason [%s]",
                    task.getDeploymentId(),
                    task.getParams().getModelId(),
                    task.stoppedReason().orElse("_unknown_")
                )
            );
        }
        // TODO: Do we want to stop the task? This would cause it to be reloaded by state updates on INITIALIZING
        // We should stop the local task so that future task actions won't get routed to the older one.
        Runnable stopTask = () -> stopDeploymentAsync(
            task,
            "model failed to load; reason [" + ex.getMessage() + "]",
            ActionListener.running(() -> retryListener.onResponse(false))
        );
        updateStoredState(
            task.getDeploymentId(),
            RoutingInfoUpdate.updateStateAndReason(
                new RoutingStateAndReason(RoutingState.FAILED, ExceptionsHelper.unwrapCause(ex).getMessage())
            ),
            ActionListener.running(stopTask)
        );
    }

    public void failAssignment(TrainedModelDeploymentTask task, String reason) {
        updateStoredState(
            task.getDeploymentId(),
            RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.FAILED, reason)),
            ActionListener.wrap(
                r -> logger.debug(
                    () -> format(
                        "[%s] Successfully updating assignment state to [%s] with reason [%s]",
                        task.getDeploymentId(),
                        RoutingState.FAILED,
                        reason
                    )
                ),
                e -> logger.error(
                    () -> format(
                        "[%s] Error while updating assignment state to [%s] with reason [%s]",
                        task.getDeploymentId(),
                        RoutingState.FAILED,
                        reason
                    ),
                    e
                )
            )
        );
    }
}
