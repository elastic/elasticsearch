/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.health;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class TransportClusterHealthAction extends TransportMasterNodeReadAction<ClusterHealthRequest, ClusterHealthResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterHealthAction.class);

    private final AllocationService allocationService;

    @Inject
    public TransportClusterHealthAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService
    ) {
        super(
            ClusterHealthAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterHealthRequest::new,
            indexNameExpressionResolver,
            ClusterHealthResponse::new,
            ThreadPool.Names.MANAGEMENT // fork to management since the health computation can become expensive for large cluster states
        );
        this.allocationService = allocationService;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterHealthRequest request, ClusterState state) {
        // we want users to be able to call this even when there are global blocks, just to check the health (are there blocks?)
        return null;
    }

    @Override
    protected final void masterOperation(ClusterHealthRequest request, ClusterState state, ActionListener<ClusterHealthResponse> listener)
        throws Exception {
        logger.warn("attempt to execute a cluster health operation without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void masterOperation(
        final Task task,
        final ClusterHealthRequest request,
        final ClusterState unusedState,
        final ActionListener<ClusterHealthResponse> listener
    ) {
        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;

        final int waitCount = getWaitCount(request);

        if (request.waitForEvents() != null) {
            waitForEventsAndExecuteHealth(
                cancellableTask,
                request,
                listener,
                waitCount,
                threadPool.relativeTimeInMillis() + request.timeout().millis()
            );
        } else {
            executeHealth(
                cancellableTask,
                request,
                clusterService.state(),
                listener,
                waitCount,
                clusterState -> sendResponse(cancellableTask, request, clusterState, waitCount, TimeoutState.OK, listener)
            );
        }
    }

    private void waitForEventsAndExecuteHealth(
        final CancellableTask task,
        final ClusterHealthRequest request,
        final ActionListener<ClusterHealthResponse> listener,
        final int waitCount,
        final long endTimeRelativeMillis
    ) {
        if (task.notifyIfCancelled(listener)) {
            return;
        }

        assert request.waitForEvents() != null;
        if (request.local()) {
            clusterService.submitStateUpdateTask(
                "cluster_health (wait_for_events [" + request.waitForEvents() + "])",
                new LocalClusterUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                        return unchanged();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis());
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);
                        executeHealth(
                            task,
                            request,
                            clusterService.state(),
                            listener,
                            waitCount,
                            observedState -> waitForEventsAndExecuteHealth(task, request, listener, waitCount, endTimeRelativeMillis)
                        );
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                        listener.onFailure(e);
                    }
                }
            );
        } else {
            final TimeValue taskTimeout = TimeValue.timeValueMillis(Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis()));
            clusterService.submitStateUpdateTask(
                "cluster_health (wait_for_events [" + request.waitForEvents() + "])",
                new ClusterStateUpdateTask(request.waitForEvents(), taskTimeout) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis());
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);

                        // we must use the state from the applier service, because if the state-not-recovered block is in place then the
                        // applier service has a different view of the cluster state from the one supplied here
                        final ClusterState appliedState = clusterService.state();
                        assert newState.stateUUID().equals(appliedState.stateUUID())
                            : newState.stateUUID() + " vs " + appliedState.stateUUID();
                        executeHealth(
                            task,
                            request,
                            appliedState,
                            listener,
                            waitCount,
                            observedState -> waitForEventsAndExecuteHealth(task, request, listener, waitCount, endTimeRelativeMillis)
                        );
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.trace(
                            "stopped being master while waiting for events with priority [{}]. retrying.",
                            request.waitForEvents()
                        );
                        // TransportMasterNodeAction implements the retry logic, which is triggered by passing a NotMasterException
                        listener.onFailure(new NotMasterException("no longer master. source: [" + source + "]"));
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        if (e instanceof ProcessClusterEventTimeoutException) {
                            sendResponse(task, request, clusterService.state(), waitCount, TimeoutState.TIMED_OUT, listener);
                        } else {
                            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                            listener.onFailure(e);
                        }
                    }
                }
            );
        }
    }

    private void executeHealth(
        final CancellableTask task,
        final ClusterHealthRequest request,
        final ClusterState currentState,
        final ActionListener<ClusterHealthResponse> listener,
        final int waitCount,
        final Consumer<ClusterState> onNewClusterStateAfterDelay
    ) {
        if (task.notifyIfCancelled(listener)) {
            return;
        }

        if (request.timeout().millis() == 0) {
            sendResponse(task, request, currentState, waitCount, TimeoutState.ZERO_TIMEOUT, listener);
            return;
        }

        final Predicate<ClusterState> validationPredicate = newState -> validateRequest(request, newState, waitCount);
        if (validationPredicate.test(currentState)) {
            sendResponse(task, request, currentState, waitCount, TimeoutState.OK, listener);
        } else {
            final ClusterStateObserver observer = new ClusterStateObserver(
                currentState,
                clusterService,
                null,
                logger,
                threadPool.getThreadContext()
            );
            final ClusterStateObserver.Listener stateListener = new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState newState) {
                    onNewClusterStateAfterDelay.accept(newState);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    sendResponse(task, request, observer.setAndGetObservedState(), waitCount, TimeoutState.TIMED_OUT, listener);
                }
            };
            observer.waitForNextChange(stateListener, validationPredicate, request.timeout());
        }
    }

    private static int getWaitCount(ClusterHealthRequest request) {
        int waitCount = 0;
        if (request.waitForStatus() != null) {
            waitCount++;
        }
        if (request.waitForNoRelocatingShards()) {
            waitCount++;
        }
        if (request.waitForNoInitializingShards()) {
            waitCount++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            waitCount++;
        }
        if (request.waitForNodes().isEmpty() == false) {
            waitCount++;
        }
        if (CollectionUtils.isEmpty(request.indices()) == false) { // check that they actually exists in the meta data
            waitCount++;
        }
        return waitCount;
    }

    private boolean validateRequest(final ClusterHealthRequest request, ClusterState clusterState, final int waitCount) {
        ClusterHealthResponse response = clusterHealth(
            request,
            clusterState,
            clusterService.getMasterService().numberOfPendingTasks(),
            allocationService.getNumberOfInFlightFetches(),
            clusterService.getMasterService().getMaxTaskWaitTime()
        );
        return prepareResponse(request, response, clusterState, indexNameExpressionResolver) == waitCount;
    }

    private enum TimeoutState {
        OK,
        TIMED_OUT,
        ZERO_TIMEOUT
    }

    private void sendResponse(
        final CancellableTask task,
        final ClusterHealthRequest request,
        final ClusterState clusterState,
        final int waitFor,
        final TimeoutState timeoutState,
        final ActionListener<ClusterHealthResponse> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            task.ensureNotCancelled();
            ClusterHealthResponse response = clusterHealth(
                request,
                clusterState,
                clusterService.getMasterService().numberOfPendingTasks(),
                allocationService.getNumberOfInFlightFetches(),
                clusterService.getMasterService().getMaxTaskWaitTime()
            );
            int readyCounter = prepareResponse(request, response, clusterState, indexNameExpressionResolver);
            boolean valid = (readyCounter == waitFor);
            assert valid || (timeoutState != TimeoutState.OK);
            // If valid && timeoutState == TimeoutState.ZERO_TIMEOUT then we immediately found **and processed** a valid state, so we don't
            // consider this a timeout. However if timeoutState == TimeoutState.TIMED_OUT then we didn't process a valid state (perhaps we
            // failed on wait_for_events) so this does count as a timeout.
            response.setTimedOut(valid == false || timeoutState == TimeoutState.TIMED_OUT);
            return response;
        });
    }

    static int prepareResponse(
        final ClusterHealthRequest request,
        final ClusterHealthResponse response,
        final ClusterState clusterState,
        final IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        int waitForCounter = 0;
        if (request.waitForStatus() != null && response.getStatus().value() <= request.waitForStatus().value()) {
            waitForCounter++;
        }
        if (request.waitForNoRelocatingShards() && response.getRelocatingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForNoInitializingShards() && response.getInitializingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            ActiveShardCount waitForActiveShards = request.waitForActiveShards();
            assert waitForActiveShards.equals(ActiveShardCount.DEFAULT) == false
                : "waitForActiveShards must not be DEFAULT on the request object, instead it should be NONE";
            if (waitForActiveShards.equals(ActiveShardCount.ALL)) {
                if (response.getUnassignedShards() == 0 && response.getInitializingShards() == 0) {
                    // if we are waiting for all shards to be active, then the num of unassigned and num of initializing shards must be 0
                    waitForCounter++;
                }
            } else if (waitForActiveShards.enoughShardsActive(response.getActiveShards())) {
                // there are enough active shards to meet the requirements of the request
                waitForCounter++;
            }
        }
        if (CollectionUtils.isEmpty(request.indices()) == false) {
            try {
                indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), request);
                waitForCounter++;
            } catch (IndexNotFoundException e) {
                response.setStatus(ClusterHealthStatus.RED); // no indices, make sure its RED
                // missing indices, wait a bit more...
            }
        }
        if (request.waitForNodes().isEmpty() == false) {
            if (request.waitForNodes().startsWith(">=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("ge(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("le(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith(">")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("gt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("lt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else {
                int expected = Integer.parseInt(request.waitForNodes());
                if (response.getNumberOfNodes() == expected) {
                    waitForCounter++;
                }
            }
        }
        return waitForCounter;
    }

    private ClusterHealthResponse clusterHealth(
        ClusterHealthRequest request,
        ClusterState clusterState,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        TimeValue pendingTaskTimeInQueue
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace("Calculating health based on state version [{}]", clusterState.version());
        }

        String[] concreteIndices;
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        } catch (IndexNotFoundException e) {
            // one of the specified indices is not there - treat it as RED.
            ClusterHealthResponse response = new ClusterHealthResponse(
                clusterState.getClusterName().value(),
                Strings.EMPTY_ARRAY,
                clusterState,
                numberOfPendingTasks,
                numberOfInFlightFetch,
                UnassignedInfo.getNumberOfDelayedUnassigned(clusterState),
                pendingTaskTimeInQueue
            );
            response.setStatus(ClusterHealthStatus.RED);
            return response;
        }

        return new ClusterHealthResponse(
            clusterState.getClusterName().value(),
            concreteIndices,
            clusterState,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            UnassignedInfo.getNumberOfDelayedUnassigned(clusterState),
            pendingTaskTimeInQueue
        );
    }
}
